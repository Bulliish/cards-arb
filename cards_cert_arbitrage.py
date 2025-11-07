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

# Optional proxy/fetcher fallback (set in Streamlit Cloud Secrets)
SCRAPERAPI_KEY = os.environ.get("SCRAPERAPI_KEY")
ZENROWS_KEY   = os.environ.get("ZENROWS_KEY")

# PSA hosts to try (some environments fail on one and work on the other)
PSA_HOSTS = ["https://www.psacard.com", "https://psacard.com"]

# ---------------- Robust HTTPS session ----------------
CIPHERS = "ECDHE+AESGCM:ECDHE+CHACHA20:ECDHE+AES256:RSA+AESGCM:RSA+AES"

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
    s.trust_env = False  # ignore system proxy oddities in hosted envs
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

def _proxy_wrap(url: str) -> Optional[str]:
    """Return a proxied URL if a provider key is configured."""
    if SCRAPERAPI_KEY:
        from requests.utils import quote
        return f"https://api.scraperapi.com/?api_key={SCRAPERAPI_KEY}&url={quote(url, safe='')}"
    if ZENROWS_KEY:
        from requests.utils import quote
        return f"https://api.zenrows.com/v1/?apikey={ZENROWS_KEY}&url={quote(url, safe='')}"
    return None

def _get(url: str, *, verify_tls: bool = True) -> requests.Response:
    """Single GET with explicit CA bundle or disabled verification."""
    return SESSION.get(
        url,
        timeout=30,
        verify=(certifi.where() if verify_tls else False),
    )

def _fetch(
    url: str,
    *,
    allow_proxy_fallback: bool = True,
    force_proxy: Optional[bool] = None,
    verify_tls: bool = True,
    logger: Optional[Callable[[str], None]] = None,
) -> requests.Response:
    """
    Robust fetch:
      - force_proxy=True  -> always use proxy (error if no key configured)
      - force_proxy=False -> always direct (no proxy)
      - force_proxy=None  -> try direct, on SSLError and if key exists -> proxy fallback
      - verify_tls=False  -> disable TLS verification (last resort; not recommended)
    """
    mode = "auto"
    if force_proxy is True: mode = "proxy"
    if force_proxy is False: mode = "direct"
    if logger: logger(f"GET {url}  | mode={mode}  tls_verify={'ON' if verify_tls else 'OFF'}")

    # Force proxy path
    if force_proxy is True:
        prox = _proxy_wrap(url)
        if not prox:
            raise RuntimeError("Proxy is forced but no SCRAPERAPI_KEY or ZENROWS_KEY configured.")
        r = _get(prox, verify_tls=verify_tls)
        if logger: logger(f" → via proxy {('scraperapi' if SCRAPERAPI_KEY else 'zenrows')}, status={r.status_code}")
        r.raise_for_status()
        return r

    # Direct path (with optional fallback)
    try:
        r = _get(url, verify_tls=verify_tls)
        if logger: logger(f" → direct status={r.status_code}")
        r.raise_for_status()
        return r
    except requests.exceptions.SSLError as e:
        if logger: logger(f" !! SSL error on direct: {e.__class__.__name__}")
        if allow_proxy_fallback and force_proxy is None:
            prox = _proxy_wrap(url)
            if prox:
                rp = _get(prox, verify_tls=verify_tls)
                if logger: logger(f" → retry via proxy {('scraperapi' if SCRAPERAPI_KEY else 'zenrows')}, status={rp.status_code}")
                rp.raise_for_status()
                return rp
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
    psa_estimate: Optional[float]              # NEW: from cert page
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
def _discover_product_urls_for_category(category_url_first_page: str, max_pages: int = 200, *, force_proxy: Optional[bool] = None, verify_tls: bool = True, logger: Optional[Callable[[str], None]] = None) -> List[str]:
    urls: List[str] = []
    base_no_page = re.sub(r"(\?|&)page=\d+", "", category_url_first_page)
    page = 1
    while page <= max_pages:
        sep = "&" if "?" in base_no_page else "?"
        url = f"{base_no_page}{sep}page={page}"
        r = _fetch(url, force_proxy=force_proxy, verify_tls=verify_tls, logger=logger)
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
        if logger: logger(f"   page {page}: found {len(page_urls)} product links")
        if not page_urls:
            break
        urls.extend(page_urls)
        page += 1
        _throttle()
    if logger: logger(f"[{category_url_first_page}] total discovered: {len(urls)}")
    return urls

# ---------------- Product page parser ----------------
def _scrape_cardshq_product(url: str, *, force_proxy: Optional[bool] = None, verify_tls: bool = True, logger: Optional[Callable[[str], None]] = None) -> Optional[StoreItem]:
    r = _fetch(url, force_proxy=force_proxy, verify_tls=verify_tls, logger=logger)
    soup = BeautifulSoup(r.text, "lxml")

    # Name / title
    h1 = soup.find(["h1", "h2"], string=True)
    name = h1.get_text(" ", strip=True) if h1 else url

    # Price: try multiple known patterns including the user's snippet
    price: Optional[float] = None
    price_candidates = [
        '.price__regular', '.price-item--regular', '.price', '.product__price',
        '[data-product-price]', '.price__container', '.price__current',
        'div.mr-auto.flex.w-auto.items-center.py-2.text-xl p',  # exact snippet path
    ]
    for sel in price_candidates:
        el = soup.select_one(sel)
        if el:
            price = _clean_money(el.get_text(" ", strip=True))
            if price is not None:
                break
    if price is None:
        # Fallback: meta or whole-page scan (handles "$80.00 USD" etc.)
        meta_price = soup.select_one('meta[itemprop="price"]')
        if meta_price and meta_price.get("content"):
            try:
                price = float(meta_price["content"])
            except Exception:
                pass
    if price is None:
        # Ultimate fallback: search first "$..." token on page
        price = _clean_money(soup.get_text(" ", strip=True))

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

    if logger:
        logger(f"   parsed product | cert={cert or '—'} grade={grade_text or '—'} price={price if price is not None else '—'}")

    return StoreItem(
        source="cardshq.com",
        url=url,
        card_name=name,
        price=price,
        psa_grade_text=grade_text,
        psa_grade_num=grade_num,
        psa_cert=cert
    )

# ---------------- PSA helpers ----------------
def _extract_psa_estimate_from_cert_soup(soup: BeautifulSoup, logger: Optional[Callable[[str], None]] = None) -> Optional[float]:
    """
    Find the 'PSA Estimate' dollar value on the cert page.
    Strategy:
      1) Look for any element whose text contains 'PSA Estimate'
      2) Search within its parent/container for the first $ amount
      3) Fallback: regex 'PSA Estimate ... $123.45' in whole page text
    """
    # Direct label search
    label_node = soup.find(string=re.compile(r'PSA\s*Estimate', re.I))
    if label_node:
        # search up to two ancestor levels for the amount
        for ancestor in [label_node.parent, getattr(label_node, "parent", None) and label_node.parent.parent]:
            if not ancestor:
                continue
            money = _clean_money(ancestor.get_text(" ", strip=True))
            if money is not None:
                if logger: logger(f"   PSA Estimate found in label container: ${money}")
                return money

    # Fallback regex across whole page
    txt = soup.get_text(" ", strip=True)
    m = re.search(r'PSA\s*Estimate[^$]{0,200}\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?|[0-9]+(?:\.[0-9]{2})?)', txt, re.I | re.S)
    if m:
        try:
            val = float(m.group(1).replace(",", ""))
            if logger: logger(f"   PSA Estimate (fallback regex): ${val}")
            return val
        except Exception:
            return None
    return None

# ---------------- PSA APR fetch ----------------
def _psa_cert_info(cert: str, *, force_proxy: Optional[bool] = None, verify_tls: bool = True, logger: Optional[Callable[[str], None]] = None) -> Tuple[str, Optional[str], Optional[float]]:
    """
    Returns (cert_url, apr_url, psa_estimate)
    """
    last_cert_url = None
    for host in PSA_HOSTS:
        cert_url = f"{host}/cert/{cert}/psa"
        last_cert_url = cert_url
        if logger: logger(f"   PSA cert try: {cert_url}")
        try:
            r = _fetch(cert_url, allow_proxy_fallback=True, force_proxy=force_proxy, verify_tls=verify_tls, logger=logger)
        except requests.exceptions.SSLError:
            if logger: logger("   PSA cert fetch SSL error; trying next host")
            continue
        soup = BeautifulSoup(r.text, "lxml")

        # NEW: grab PSA Estimate before we leave the page
        psa_estimate = _extract_psa_estimate_from_cert_soup(soup, logger=logger)

        # Find Sales History link
        apr_link = None
        for a in soup.select("a"):
            href = (a.get("href") or "").strip()
            label = (a.get_text(strip=True) or "").lower()
            if "auctionprices" in href.lower() or label == "sales history":
                apr_link = href
                break
        if apr_link and apr_link.startswith("/"):
            apr_link = f"{host}{apr_link}"
        if logger: logger(f"   PSA APR link: {apr_link or '—'}; PSA Estimate: {psa_estimate if psa_estimate is not None else '—'}")
        return cert_url, apr_link, psa_estimate
    # If all hosts failed:
    return last_cert_url or f"{PSA_HOSTS[0]}/cert/{cert}/psa", None, None

def _psa_apr_most_recent_for_grade(apr_url: str, grade_num: int, *, force_proxy: Optional[bool] = None, verify_tls: bool = True, logger: Optional[Callable[[str], None]] = None) -> Optional[float]:
    r = _fetch(apr_url, allow_proxy_fallback=True, force_proxy=force_proxy, verify_tls=verify_tls, logger=logger)
    text = BeautifulSoup(r.text, "lxml").get_text(" ", strip=True)
    m = re.search(rf'PSA\s*{grade_num}\s*\$([0-9\.,]+)', text)
    if not m:
        if logger: logger("   PSA APR: no 'Most Recent' match for grade")
        return None
    try:
        val = float(m.group(1).replace(",", ""))
        if logger: logger(f"   PSA APR: Most Recent for PSA {grade_num} = ${val}")
        return val
    except Exception:
        return None

def _psa_apr_recent_prices(apr_url: str, take: int = 25, *, force_proxy: Optional[bool] = None, verify_tls: bool = True, logger: Optional[Callable[[str], None]] = None) -> List[float]:
    r = _fetch(apr_url, allow_proxy_fallback=True, force_proxy=force_proxy, verify_tls=verify_tls, logger=logger)
    text = BeautifulSoup(r.text, "lxml").get_text(" ", strip=True)
    hits = re.findall(r'\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?|[0-9]+(?:\.[0-9]{2})?)', text)
    out: List[float] = []
    for h in hits[:take]:
        try:
            out.append(float(h.replace(",", "")))
        except Exception:
            continue
    if logger: logger(f"   PSA APR: collected {len(out)} recent price tokens")
    return out

def _fetch_psa_comp(cert: str, grade_num: Optional[int], *, force_proxy: Optional[bool] = None, verify_tls: bool = True, logger: Optional[Callable[[str], None]] = None) -> PsaComp:
    cert_url, apr_url, psa_estimate = _psa_cert_info(cert, force_proxy=force_proxy, verify_tls=verify_tls, logger=logger)
    most_recent_for_grade = None
    median_recent_sales = None
    last_n_prices: List[float] = []
    if apr_url:
        if grade_num is not None:
            most_recent_for_grade = _psa_apr_most_recent_for_grade(apr_url, grade_num, force_proxy=force_proxy, verify_tls=verify_tls, logger=logger)
            _throttle()
        last_n_prices = _psa_apr_recent_prices(apr_url, take=25, force_proxy=force_proxy, verify_tls=verify_tls, logger=logger)
        if last_n_prices:
            sorted_vals = sorted(last_n_prices)
            median_recent_sales = sorted_vals[len(sorted_vals)//2]
            if logger: logger(f"   PSA APR: median recent (all rows) = ${median_recent_sales}")
    return PsaComp(
        cert_url=cert_url,
        apr_url=apr_url,
        most_recent_for_grade=most_recent_for_grade,
        median_recent_sales=median_recent_sales,
        psa_estimate=psa_estimate,
        last_n_prices=last_n_prices
    )

# ---------------- Public orchestrators ----------------
def scan_selected_categories(
    categories: List[str],
    limit_per_category: Optional[int] = None,
    fee_rate: float = 0.13,
    ship_out: float = 5.0,
    *,
    force_proxy: Optional[bool] = None,
    verify_tls: bool = True,
    logger: Optional[Callable[[str], None]] = None
) -> pd.DataFrame:
    """
    force_proxy:
      - True  => always route requests via proxy
      - False => always direct
      - None  => auto (direct, SSLError -> proxy if configured)
    verify_tls:
      - True  => strict TLS verification using certifi (recommended)
      - False => disable TLS verification (last resort)
    """
    selected: Dict[str, str] = {}
    for label in categories:
        if label in CARDSHQ_CATEGORY_URLS:
            selected[label] = CARDSHQ_CATEGORY_URLS[label]

    rows: List[Dict] = []
    for label, first_page_url in selected.items():
        if logger: logger(f"[{label}] discovering products…")
        product_urls = _discover_product_urls_for_category(first_page_url, max_pages=200, force_proxy=force_proxy, verify_tls=verify_tls, logger=logger)

        found_items: List[StoreItem] = []
        for idx, pu in enumerate(product_urls, start=1):
            _throttle()
            item = _scrape_cardshq_product(pu, force_proxy=force_proxy, verify_tls=verify_tls, logger=logger)
            if not item:
                continue
            if item.psa_cert and item.psa_grade_num is not None:
                found_items.append(item)
                if limit_per_category and len(found_items) >= limit_per_category:
                    if logger: logger(f"  reached limit_per_category={limit_per_category}")
                    break
            if idx % 25 == 0 and logger:
                logger(f"  parsed {idx}/{len(product_urls)} product pages…")

        if logger: logger(f"[{label}] PSA-cert listings: {len(found_items)} — fetching PSA comps…")

        for it in found_items:
            _throttle()
            comp = _fetch_psa_comp(it.psa_cert, it.psa_grade_num, force_proxy=force_proxy, verify_tls=verify_tls, logger=logger)

            # Choose best comp value in priority order
            comp_value = None
            for v in (comp.most_recent_for_grade, comp.psa_estimate, comp.median_recent_sales):
                if v is not None:
                    comp_value = v
                    break

            expected_net = None
            roi_pct = None
            if comp_value is not None and it.price and it.price > 0:
                expected_net = comp_value * (1 - fee_rate) - ship_out
                roi_pct = (expected_net - it.price) / it.price * 100

            rows.append({
                "Category": label,
                "Store": it.source,
                "Card Name": it.card_name,
                "Store Price": it.price,
                "PSA Grade": it.psa_grade_text,
                "PSA Cert": it.psa_cert,
                "PSA Cert URL": comp.cert_url,
                "PSA APR URL": comp.apr_url,
                "PSA Estimate (cert page)": comp.psa_estimate,     # NEW
                "APR Most Recent (Grade)": comp.most_recent_for_grade,
                "APR Median Recent (All)": comp.median_recent_sales,
                "Expected Net (est)": round(expected_net, 2) if expected_net is not None else None,
                "ROI % (est)": round(roi_pct, 2) if roi_pct is not None else None,
                "Store URL": it.url
            })

    df = pd.DataFrame(rows)
    if not df.empty and "ROI % (est)" in df.columns:
        df = df.sort_values(by=["ROI % (est)"], ascending=False, na_position="last")
    if logger: logger(f"[done] total rows={len(df)}")
    return df

def test_psa_cert(
    cert: str,
    grade_num: Optional[int] = None,
    *,
    force_proxy: Optional[bool] = None,
    verify_tls: bool = True,
    logger: Optional[Callable[[str], None]] = None
) -> Dict[str, Optional[float]]:
    """
    Quick connectivity + pricing test for a single PSA cert.
    """
    if logger: logger(f"[test] PSA cert {cert}  grade={grade_num or '—'}")
    comp = _fetch_psa_comp(cert, grade_num, force_proxy=force_proxy, verify_tls=verify_tls, logger=logger)
    # Choose best value with new PSA Estimate in the mix
    value = None
    for v in (comp.most_recent_for_grade, comp.psa_estimate, comp.median_recent_sales):
        if v is not None:
            value = v
            break
    if logger: logger(f"[test] chosen value=${value if value is not None else '—'}")
    return {
        "PSA Cert URL": comp.cert_url,
        "PSA APR URL": comp.apr_url,
        "PSA Estimate (cert page)": comp.psa_estimate,
        "APR Most Recent (Grade)": comp.most_recent_for_grade,
        "APR Median Recent (All)": comp.median_recent_sales,
        "Chosen Value": value
    }
