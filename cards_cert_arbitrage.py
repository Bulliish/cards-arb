
import os
import re
import time
import certifi
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple, Callable

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from urllib3.util.ssl_ import create_urllib3_context
from bs4 import BeautifulSoup
import pandas as pd

# ==============================
# Config
# ==============================

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}
THROTTLE = float(os.environ.get("ARB_THROTTLE", "1.25"))  # polite delay between requests
BASE = "https://www.cardshq.com"

# Categories to scan (can be narrowed in the app)
CARDSHQ_CATEGORY_URLS = {
    "Baseball":              f"{BASE}/collections/baseball-cards?page=1",
    "Basketball (Graded)":   f"{BASE}/collections/basketball-graded?page=1",
    "Football":              f"{BASE}/collections/football-cards?page=1",
    "Soccer":                f"{BASE}/collections/soccer-cards?page=1",
    "Pokemon":               f"{BASE}/collections/pokemon-cards?page=1",
}

# Optional proxy (ScraperAPI / ZenRows). You DO NOT need these for Path A.
SCRAPERAPI_KEY = os.environ.get("SCRAPERAPI_KEY")
ZENROWS_KEY    = os.environ.get("ZENROWS_KEY")

# PSA-specific handling (Path A): prefer certifi CA and allow a single unsafe retry
PSA_HOSTS = {"www.psacard.com", "psacard.com"}

# Parser: allow override via env; Streamlit will also pass this
PARSER = os.environ.get("BS_PARSER", "lxml")

# ==============================
# TLS-hardened Session
# ==============================

# Slightly opinionated cipher suite for better compatibility with older edges
CIPHERS = "ECDHE+AESGCM:ECDHE+CHACHA20:ECDHE+AES256:RSA+AESGCM:RSA+AES"

class TLSHttpAdapter(HTTPAdapter):
    \"\"\"Force modern TLS + certifi CA bundle to avoid handshake issues.\"\"\"
    def init_poolmanager(self, *args, **kwargs):
        ctx = create_urllib3_context(ciphers=CIPHERS)
        kwargs.setdefault("ssl_context", ctx)
        kwargs.setdefault("cert_reqs", "CERT_REQUIRED")
        kwargs.setdefault("ca_certs", certifi.where())
        return super().init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        ctx = create_urllib3_context(ciphers=CIPHERS)
        kwargs.setdefault("ssl_context", ctx)
        kwargs.setdefault("cert_reqs", "CERT_REQUIRED")
        kwargs.setdefault("ca_certs", certifi.where())
        return super().proxy_manager_for(*args, **kwargs)

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retries = Retry(total=2, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    adapter = TLSHttpAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s

SESSION = make_session()

# ==============================
# HTTP helpers
# ==============================

def _proxy_wrap(url: str) -> Optional[str]:
    from urllib.parse import quote
    if SCRAPERAPI_KEY:
        return f"https://api.scraperapi.com/?api_key={SCRAPERAPI_KEY}&keep_headers=true&url={quote(url, safe='')}"
    if ZENROWS_KEY:
        return f"https://api.zenrows.com/v1/?apikey={ZENROWS_KEY}&url={quote(url, safe='')}"
    return None

def _get(url: str, *, verify_tls: bool = True) -> requests.Response:
    # Use certifi CA when verify=True
    return SESSION.get(url, timeout=30, verify=(certifi.where() if verify_tls else False))

def _fetch(
    url: str,
    *,
    allow_proxy_fallback: bool = True,
    force_proxy: Optional[bool] = None,
    verify_tls: bool = True,
    logger: Optional[Callable[[str], None]] = None,
) -> requests.Response:
    \"\"\"Fetch a URL with PSA-scoped TLS hardening and optional proxy fallback.\"\"\"
    mode = "auto"
    if force_proxy is True: mode = "proxy"
    if force_proxy is False: mode = "direct"
    if logger: logger(f\"GET {url}  | mode={mode}  tls_verify={'ON' if verify_tls else 'OFF'}\")

    # Forced proxy path
    if force_proxy is True:
        prox = _proxy_wrap(url)
        if not prox:
            raise RuntimeError(\"Proxy is forced but no SCRAPERAPI_KEY or ZENROWS_KEY configured.\")
        r = _get(prox, verify_tls=verify_tls)
        if logger: logger(f\" → via proxy {('scraperapi' if SCRAPERAPI_KEY else 'zenrows')}, status={r.status_code}\")
        r.raise_for_status()
        return r

    # Direct-first path
    host = (requests.utils.urlparse(url).hostname or \"\").lower()
    try:
        # PSA hosts always use certifi CA even if verify_tls=True
        r = _get(url, verify_tls=verify_tls)
        if logger: logger(f\" → direct status={r.status_code}\")
        r.raise_for_status()
        return r
    except requests.exceptions.SSLError as e:
        if logger: logger(f\" !! SSL error on direct: {e.__class__.__name__}\")
        # PSA-only: one unsafe retry (public HTML only)
        if host in PSA_HOSTS and verify_tls:
            try:
                r = SESSION.get(url, timeout=30, verify=False)
                if logger: logger(\"    PSA-scoped unsafe retry succeeded\" if r.ok else \"    PSA-scoped unsafe retry failed\")
                r.raise_for_status()
                return r
            except Exception as e2:
                if logger: logger(f\"    PSA-scoped unsafe retry error: {type(e2).__name__}\")
                # fall through to proxy if allowed

        if allow_proxy_fallback and force_proxy is None:
            prox = _proxy_wrap(url)
            if prox:
                if logger: logger(\"    PSA cert fetch SSL error; trying proxy host\")
                r = _get(prox, verify_tls=verify_tls)
                if logger: logger(f\" → via proxy {('scraperapi' if SCRAPERAPI_KEY else 'zenrows')}, status={r.status_code}\")
                r.raise_for_status()
                return r
        raise

# ==============================
# Scraping helpers
# ==============================

def _soup(html: str, parser: Optional[str] = None) -> BeautifulSoup:
    return BeautifulSoup(html, parser or PARSER)

def _price_to_float(txt: str) -> Optional[float]:
    m = re.search(r\"([\\$]?\\s*([0-9]{1,3}(?:,[0-9]{3})*|[0-9]+)(?:\\.[0-9]{2})?)\", txt)
    if not m: return None
    s = m.group(1).replace(\"$\", \"\").replace(\",\", \"\").strip()
    try:
        return float(s)
    except ValueError:
        return None

def _extract_cert_from_title(title: str) -> Optional[str]:
    # Look for an 8-digit cert number
    m = re.search(r\"\\b(\\d{8})\\b\", title)
    return m.group(1) if m else None

def _extract_grade_from_title(title: str) -> Optional[str]:
    # Look for common PSA grade tokens (e.g., PSA 10, GEM MT 10)
    m = re.search(r\"\\b(PSA\\s*)?(GEM\\s*MT\\s*10|Mint\\s*9|\\d{1,2})\\b\", title, re.I)
    return m.group(0) if m else None

# ==============================
# CardsHQ
# ==============================

def _discover_product_urls_for_category(category_url: str, logger: Callable[[str], None]) -> List[str]:
    urls: List[str] = []
    page = 1
    while True:
        url = re.sub(r\"page=\\d+\", f\"page={page}\", category_url)
        logger(f\"GET {url}  | mode=auto  tls_verify=ON\")
        r = _fetch(url, force_proxy=False, verify_tls=True, logger=logger)
        logger(f\" → direct status={r.status_code}\")
        soup = _soup(r.text, parser=\"html.parser\")  # CardsHQ is simple; html.parser is fine
        links = [a.get(\"href\") for a in soup.select(\"a.card-product__title-link\") if a.get(\"href\")]
        if not links:
            # Try a more generic selector if theme changed
            links = [a.get(\"href\") for a in soup.select(\"a[href*='/products/']\") if a.get(\"href\")]
        links = [u if u.startswith(\"http\") else (BASE + u) for u in links]
        if links:
            logger(f\"   page {page}: found {len(links)} product links\")
            urls.extend(links)
            page += 1
            time.sleep(THROTTLE)
        else:
            break
    return urls

@dataclass
class Listing:
    category: str
    product_url: str
    title: str
    price: Optional[float]
    cert: Optional[str]
    grade: Optional[str]

def _scrape_cardshq_product(url: str, logger: Callable[[str], None]) -> Listing:
    r = _fetch(url, force_proxy=False, verify_tls=True, logger=logger)
    soup = _soup(r.text, parser=\"html.parser\")
    title = (soup.select_one(\"h1.product__title\") or soup.select_one(\"h1\")).get_text(strip=True)
    # price can be in data attributes or visible
    price_txt = \"\"
    price_el = soup.select_one(\"span.price-item--regular\") or soup.select_one(\"span.price-item\")
    if price_el:
        price_txt = price_el.get_text(\" \", strip=True)
    if not price_txt:
        meta = soup.find(\"meta\", attrs={\"itemprop\": \"price\"})
        if meta and meta.get(\"content\"):
            price_txt = meta[\"content\"]
    price = _price_to_float(price_txt) if price_txt else None
    cert = _extract_cert_from_title(title)
    grade = _extract_grade_from_title(title)
    return Listing(category=\"\", product_url=url, title=title, price=price, cert=cert, grade=grade)

# ==============================
# PSA cert page
# ==============================

def _psa_cert_url(cert: str) -> List[str]:
    return [
        f\"https://www.psacard.com/cert/{cert}/psa\",
        f\"https://psacard.com/cert/{cert}/psa\",
    ]

def _extract_psa_estimate_from_cert_soup(soup: BeautifulSoup) -> Optional[float]:
    # Look for a \"PSA Estimate\" label or a $ near that section
    # Common pattern: <div>PSA Estimate</div> <div>$169.87</div>
    text = soup.get_text(\" \", strip=True)
    # Prefer a tighter search around the label if present
    m = re.search(r\"PSA\\s*Estimate[^$]*\\$\\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\\.[0-9]{2})?)\", text, re.I)
    if not m:
        # fallback: first price on page (usually the estimate badge near the top)
        m = re.search(r\"\\$\\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\\.[0-9]{2})?)\", text)
    if not m:
        return None
    return float(m.group(1).replace(\",\", \"\"))

def fetch_psa_cert_estimate(cert: str, *, verify_tls: bool, logger: Callable[[str], None]) -> Optional[float]:
    for u in _psa_cert_url(cert):
        logger(f\"   PSA cert try: {u}\")
        try:
            r = _fetch(u, force_proxy=None, verify_tls=verify_tls, logger=logger)
            soup = _soup(r.text)
            est = _extract_psa_estimate_from_cert_soup(soup)
            if est is not None:
                return est
        except requests.exceptions.SSLError:
            logger(\"    PSA cert fetch SSL error; trying next host\")
            continue
        except Exception as e:
            logger(f\"    PSA cert fetch error: {type(e).__name__}: {e}\")
            continue
    return None

# ==============================
# Public API
# ==============================

def scan_category(
    category_name: str,
    limit_per_category: int,
    fee_rate: float,
    ship_out: float,
    force_proxy: Optional[bool],
    verify_tls: bool,
    parser_override: Optional[str],
    logger: Callable[[str], None],
) -> pd.DataFrame:
    if parser_override:
        global PARSER
        PARSER = parser_override

    logger(f\"[{category_name}] discovering products…\")
    product_urls = _discover_product_urls_for_category(CARDSHQ_CATEGORY_URLS[category_name], logger)
    logger(f\"[{CARDSHQ_CATEGORY_URLS[category_name]}] total discovered: {len(product_urls)}\")

    rows: List[Dict] = []
    count = 0
    for pu in product_urls:
        listing = _scrape_cardshq_product(pu, logger)
        listing.category = category_name
        logger(f\"   parsed product | cert={listing.cert or '—'} grade={listing.grade or '—'} price={listing.price if listing.price is not None else '—'}\")
        if listing.cert:
            est = fetch_psa_cert_estimate(listing.cert, verify_tls=verify_tls, logger=logger)
        else:
            est = None
        rows.append({
            \"category\": listing.category,
            \"title\": listing.title,
            \"product_url\": listing.product_url,
            \"price\": listing.price,
            \"psa_cert\": listing.cert,
            \"grade\": listing.grade,
            \"psa_estimate\": est,
            # simple calc: target sell price minus fees/shipping
            \"est_margin\": (est or 0) - (listing.price or 0) - ((est or 0) * fee_rate) - ship_out if (est and listing.price) else None,
        })
        count += 1
        if count >= limit_per_category:
            logger(f\"  reached limit_per_category={limit_per_category}\")
            break
        time.sleep(THROTTLE)

    return pd.DataFrame(rows)

def quick_psa_cert_test(cert: str, *, verify_tls: bool, logger: Callable[[str], None]) -> Dict[str, Optional[float]]:
    est = fetch_psa_cert_estimate(cert, verify_tls=verify_tls, logger=logger)
    return {\"cert\": cert, \"psa_estimate\": est}
