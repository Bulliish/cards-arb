import os
import re
import time
import certifi
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from urllib3.util.ssl_ import create_urllib3_context
from bs4 import BeautifulSoup
import pandas as pd

# ---------------- Config ----------------
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}
THROTTLE = 1.25  # seconds between requests — be polite
BASE = "https://www.cardshq.com"

# The only lists we scan (provided by user)
CARDSHQ_CATEGORY_URLS = {
    "Baseball":              f"{BASE}/collections/baseball-cards?page=1",
    "Basketball (Graded)":   f"{BASE}/collections/basketball-graded?page=1",
    "Football":              f"{BASE}/collections/football-cards?page=1",
    "Soccer":                f"{BASE}/collections/soccer-cards?page=1",
    "Pokemon":               f"{BASE}/collections/pokemon-cards?page=1",
}

# Optional proxy/fetcher fallback (set one of these in Streamlit Cloud Secrets)
# SCRAPERAPI_KEY:   https://www.scraperapi.com/
# ZENROWS_KEY:      https://www.zenrows.com/
SCRAPERAPI_KEY = os.environ.get("SCRAPERAPI_KEY")
ZENROWS_KEY = os.environ.get("ZENROWS_KEY")

# ---------------- Robust HTTPS session ----------------
# Some origins are picky about ciphers/TLS; we provide a custom adapter + sane retries.
CIPHERS = (
    "ECDHE+AESGCM:ECDHE+CHACHA20:ECDHE+AES256:RSA+AESGCM:RSA+AES"
)

class TLS12HttpAdapter(HTTPAdapter):
    """Force modern TLS + preferred ciphers to avoid handshake issues on some origins."""
    def init_poolmanager(self, *args, **kwargs):
        ctx = create_urllib3_context(ciphers=CIPHERS)
        kwargs["ssl_context"] = ctx
        kwargs["cert_reqs"] = "CERT_REQUIRED"
        kwargs["ca_certs"] = certifi.where()
        return super().init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        ctx = create_urllib3_context(ciphers=CIPHERS)
        kwargs["ssl_context"] = ctx
        kwargs["cert_reqs"] = "CERT_REQUIRED"
        kwargs["ca_certs"] = certifi.where()
        return super().proxy_manager_for(*args, **kwargs)

def build_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"])
    )
    adapter = TLS12HttpAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(HEADERS)
    return s

SESSION = build_session()

def _throttle():
    time.sleep(THROTTLE)

def _fetch(url: str, *, allow_proxy_fallback: bool = True) -> requests.Response:
    """
    Robust fetch:
      1) Try direct HTTPS with hardened session
      2) If SSLError/connection issues and proxy key exists, retry via proxy provider
    """
    try:
        r = SESSION.get(url, timeout=30)
        r.raise_for_status()
        return r
    except requests.exceptions.SSLError as e:
        if allow_proxy_fallback and (SCRAPERAPI_KEY or ZENROWS_KEY):
            # Proxy fallback
            if SCRAPERAPI_KEY:
                prox = f"https://api.scraperapi.com/?api_key={SCRAPERAPI_KEY}&url={requests.utils.quote(url, safe='')}"
                rp = SESSION.get(prox, timeout=40)
                rp.raise_for_status()
                return rp
            if ZENROWS_KEY:
                prox = f"https://api.zenrows.com/v1/?apikey={ZENROWS_KEY}&url={requests.utils.quote(url, safe='')}"
                rp = SESSION.get(prox, timeout=40)
                rp.raise_for_status()
                return rp
        # If no proxy configured or still failing, bubble up
        raise
    except requests.RequestException:
        # Let normal HTTP errors bubble; caller can handle if needed
        raise

# ---------------- Models ----------------
@dataclass
class StoreItem:
    source: str
    url: str
    card_name: str
    price: Optional[float]
    psa_grade_text: Optional[str]
    psa_grade_num: Optional[int]
    psa_cert: Optional[str]

@dataclass
class PsaComp:
    cert_url: str
    apr_url: Optional[str]
    most_recent_for_grade: Optional[float]
    median_recent_sales: Optional[float]
    last_n_prices: List[float]

# ---------------- Utils ----------------
def _clean_money(txt: str) -> Optional[float]:
    if not txt:
        return None
    m = re.search(r'[\$€£]\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?|[0-9]+(?:\.[0-9]{2})?)', txt)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except Exception:
        return None

def _grade_num_from_text(grade_text: Optional[str]) -> Optional[int]:
    if not grade_text:
        return None
    m = re.search(r'(\d{1,2})', grade_text)
    return int(m.group(1)) if m else None

# ---------------- Category crawler ----------------
def _discover_product_urls_for_category(category_url_first_page: str, max_pages: int = 200) -> List[str]:
    urls: List[str] = []
    base_no_page = re.sub(r"(\?|&)page=\d+", "", category_url_first_page)
    page = 1
    while page <= max_pages:
        sep = "&" if "?" in base_no_page else "?"
        url = f"{base_no_page}{sep}page={page}"
        r = _fetch(url)
        soup = BeautifulSoup(r.text, "lxml")
        anchors = soup.select('a[href*="/products/"]')
        page_urls = []
        for a in anchors:
            href = a.get("href") or ""
            if "/products/" in href:
                if href.startswith("/"):
                    href = BASE + href
                if href.startswith(BASE) and href not in page_urls and href not in urls:
                    page_urls.append(href)
        if not page_urls:
            break
        urls.extend(page_urls)
        page += 1
        _throttle()
    return urls

# ---------------- Product page parser ----------------
def _scrape_cardshq_product(url: str) -> Optional[StoreItem]:
    r = _fetch(url)
    soup = BeautifulSoup(r.text, "lxml")

    # Name / title
    h1 = soup.find(["h1", "h2"], string=True)
    name = h1.get_text(" ", strip=True) if h1 else url

    # Price
    price: Optional[float] = None
    price_candidates = [
        '.price__regular', '.price-item--regular', '.price', '.product__price',
        '[data-product-price]', '.price__container', '.price__current'
    ]
    for sel in price_candidates:
        el = soup.select_one(sel)
        if el:
            price = _clean_money(el.get_text(" ", strip=True))
            if price is not None:
                break
    if price is None:
        meta_price = soup.select_one('meta[itemprop="price"]')
        if meta_price and meta_price.get("content"):
            try:
                price = float(meta_price["content"])
            except Exception:
                pass

    # Page text
    body_txt = soup.get_text(" ", strip=True)
    body_up = body_txt.upper()

    # PSA Cert
    cert = None
    m_cert = re.search(r'CERTIFICATION\s*#\s*(\d{6,9})', body_up)
    if m_cert:
        cert = m_cert.group(1)
    else:
        m_alt = re.search(r'PSA[^#]{0,30}#\s*(\d{6,9})', body_up)
        if m_alt:
            cert = m_alt.group(1)

    # PSA Grade
    grade_text = None
    m_grade = re.search(r'GRADE\s*:\s*([A-Z\s\-]*\d{1,2})', body_up)
    if m_grade:
        grade_text = m_grade.group(1).title()
    else:
        m_g2 = re.search(r'PSA\s*(\d{1,2})', body_up)
        if m_g2:
            grade_text = f"PSA {m_g2.group(1)}"
    grade_num = _grade_num_from_text(grade_text)

    return StoreItem(
        source="cardshq.com",
        url=url,
        card_name=name,
        price=price,
        psa_grade_text=grade_text,
        psa_grade_num=grade_num,
        psa_cert=cert
    )

# ---------------- PSA APR fetch ----------------
def _psa_cert_and_apr_urls(cert: str) -> Tuple[str, Optional[str]]:
    cert_url = f"https://www.psacard.com/cert/{cert}/psa"
    r = _fetch(cert_url, allow_proxy_fallback=True)
    soup = BeautifulSoup(r.text, "lxml")
    apr_link = None
    for a in soup.select("a"):
        href = (a.get("href") or "").strip()
        label = (a.get_text(strip=True) or "").lower()
        if "auctionprices" in href.lower() or label == "sales history":
            apr_link = href
            break
    if apr_link and apr_link.startswith("/"):
        apr_link = "https://www.psacard.com" + apr_link
    return cert_url, apr_link

def _psa_apr_most_recent_for_grade(apr_url: str, grade_num: int) -> Optional[float]:
    r = _fetch(apr_url, allow_proxy_fallback=True)
    text = BeautifulSoup(r.text, "lxml").get_text(" ", strip=True)
    m = re.search(rf'PSA\s*{grade_num}\s*\$([0-9\.,]+)', text)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except Exception:
        return None

def _psa_apr_recent_prices(apr_url: str, take: int = 25) -> List[float]:
    r = _fetch(apr_url, allow_proxy_fallback=True)
    text = BeautifulSoup(r.text, "l
