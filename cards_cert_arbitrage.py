import os
import re
import time
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple, Callable

import certifi
import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from urllib3.util.ssl_ import create_urllib3_context

# ---- Optional Playwright (used for PSA cert + APR fallbacks) ----
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
    _PLAYWRIGHT_OK = True
except Exception:
    _PLAYWRIGHT_OK = False

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

# Categories we scan
CARDSHQ_CATEGORY_URLS: Dict[str, str] = {
    "Baseball":              f"{BASE}/collections/baseball-cards?page=1",
    "Basketball (Graded)":   f"{BASE}/collections/basketball-graded?page=1",
    "Football":              f"{BASE}/collections/football-cards?page=1",
    "Soccer":                f"{BASE}/collections/soccer-cards?page=1",
    "Pokemon":               f"{BASE}/collections/pokemon-cards?page=1",
}

# Optional proxy/fetcher fallback (set in environment)
SCRAPERAPI_KEY = os.environ.get("SCRAPERAPI_KEY")
ZENROWS_KEY   = os.environ.get("ZENROWS_KEY")

# PSA hosts to try
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
    s.trust_env = False
    retries = Retry(
        total=3,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
    )
    adapter = TLS12HttpAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(HEADERS)
    return s


SESSION = build_session()


def _throttle() -> None:
    time.sleep(THROTTLE)


def _proxy_wrap(url: str) -> Optional[str]:
    if SCRAPERAPI_KEY:
        from requests.utils import quote
        return f"https://api.scraperapi.com/?api_key={SCRAPERAPI_KEY}&url={quote(url, safe='')}"
    if ZENROWS_KEY:
        from requests.utils import quote
        return f"https://api.zenrows.com/v1/?apikey={ZENROWS_KEY}&url={quote(url, safe='')}"
    return None


def _get(url: str, *, verify_tls: bool = True) -> requests.Response:
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
    Wrapper around requests that optionally tunnels via ScraperAPI/ZenRows and
    handles SSL issues.
    """
    mode = "auto"
    if force_proxy is True:
        mode = "proxy"
    if force_proxy is False:
        mode = "direct"
    if logger:
        logger(f"GET {url}  | mode={mode}  tls_verify={'ON' if verify_tls else 'OFF'}")

    # Explicit proxy-only mode
    if force_proxy is True:
        prox = _proxy_wrap(url)
        if not prox:
            raise RuntimeError("Proxy is forced but SCRAPERAPI_KEY / ZENROWS_KEY not configured.")
        r = _get(prox, verify_tls=verify_tls)
        if logger:
            logger(
                f"  → via proxy "
                f"{'scraperapi' if SCRAPERAPI_KEY else 'zenrows'}"
                f", status={r.status_code}"
            )
        r.raise_for_status()
        return r

    # Direct mode (with optional automatic proxy fallback)
    try:
        r = _get(url, verify_tls=verify_tls)
        if logger:
            logger(f"  → direct status={r.status_code}")
        r.raise_for_status()
        return r
    except requests.exceptions.SSLError as e:
        if logger:
            logger(f"  !! SSL error on direct: {e.__class__.__name__}")
        if allow_proxy_fallback and force_proxy is None:
            prox = _proxy_wrap(url)
            if prox:
                rp = _get(prox, verify_tls=verify_tls)
                if logger:
                    logger(
                        f"  → retry via proxy "
                        f"{'scraperapi' if SCRAPERAPI_KEY else 'zenrows'}"
                        f", status={rp.status_code}"
                    )
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
    psa_estimate: Optional[float]
    last_n_prices: List[float]


# ---------------- Utils ----------------
def _clean_money(txt: str) -> Optional[float]:
    if not txt:
        return None
    m = re.search(
        r'[\$€£]\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?|[0-9]+(?:\.[0-9]{2})?)',
        txt,
    )
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except Exception:
        return None


def _grade_num_from_text(grade_text: Optional[str]) -> Optional[int]:
    if not grade_text:
        return None
    m = re.search(r"(\d{1,2}(?:\.\d)?)", grade_text)
    try:
        g = m.group(1) if m else None
        return int(float(g)) if g else None
    except Exception:
        return None


# ---------------- Category crawler ----------------
def _discover_product_urls_for_category(
    category_url_first_page: str,
    max_pages: int = 200,
    *,
    force_proxy: Optional[bool] = None,
    verify_tls: bool = True,
    logger: Optional[Callable[[str], None]] = None,
) -> List[str]:
    urls: List[str] = []
    base_no_page = re.sub(r"(\?|&)page=\d+", "", category_url_first_page)
    page = 1
    while page <= max_pages:
        sep = "&" if "?" in base_no_page else "?"
        url = f"{base_no_page}{sep}page={page}"
        r = _fetch(url, force_proxy=force_proxy, verify_tls=verify_tls, logger=logger)
        soup = BeautifulSoup(r.text, "lxml")
        anchors = soup.select('a[href*="/products/"]')
        page_urls: List[str] = []
        for a in anchors:
            href = a.get("href") or ""
            if "/products/" not in href:
                continue
            if href.startswith("/"):
                href = BASE + href
            if href.startswith(BASE) and href not in page_urls and href not in urls:
                page_urls.append(href)
        if logger:
            logger(f"   page {page}: found {len(page_urls)} product links")
        if not page_urls:
            break
        urls.extend(page_urls)
        page += 1
        _throttle()
    if logger:
        logger(f"[{category_url_first_page}] total discovered: {len(urls)}")
    return urls


# ---------------- Product page parser ----------------
def _scrape_cardshq_product(
    url: str,
    *,
    force_proxy: Optional[bool] = None,
    verify_tls: bool = True,
    logger: Optional[Callable[[str], None]] = None,
) -> Optional[StoreItem]:
    r = _fetch(url, force_proxy=force_proxy, verify_tls=verify_tls, logger=logger)
    soup = BeautifulSoup(r.text, "lxml")

    # Name / title
    h1 = soup.find(["h1", "h2"], string=True)
    name = h1.get_text(" ", strip=True) if h1 else url

    # Price
    price: Optional[float] = None
    price_candidates = [
        ".price__regular",
        ".price-item--regular",
        ".price",
        ".product__price",
        "[data-product-price]",
        ".price__container",
        ".price__current",
        "div.mr-auto.flex.w-auto.items-center.py-2.text-xl p",
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
    if price is None:
        price = _clean_money(soup.get_text(" ", strip=True))

    # Page text
    body_txt = soup.get_text(" ", strip=True)
    body_up = body_txt.upper()

    # PSA Cert
    cert: Optional[str] = None
    m_cert = re.search(r"CERTIFICATION\s*#\s*(\d{6,9})", body_up)
    if m_cert:
        cert = m_cert.group(1)
    else:
        m_alt = re.search(r"PSA[^#]{0,30}#\s*(\d{6,9})", body_up)
        if m_alt:
            cert = m_alt.group(1)

    # PSA Grade
    grade_text: Optional[str] = None
    m_grade = re.search(r"GRADE\s*:\s*([A-Z\s\-]*\d{1,2}(?:\.\d)?)", body_up)
    if m_grade:
        grade_text = m_grade.group(1).title()
    else:
        m_g2 = re.search(r"PSA\s*(\d{1,2}(?:\.\d)?)", body_up)
        if m_g2:
            grade_text = f"PSA {m_g2.group(1)}"
    grade_num = _grade_num_from_text(grade_text)

    if logger:
        logger(
            "   parsed product | cert="
            f"{cert or '—'} grade={grade_text or '—'} price={price if price is not None else '—'}"
        )

    return StoreItem(
        source="cardshq.com",
        url=url,
        card_name=name,
        price=price,
        psa_grade_text=grade_text,
        psa_grade_num=grade_num,
        psa_cert=cert,
    )


# ---------------- PSA helpers ----------------
def _extract_psa_estimate_from_cert_soup(
    soup: BeautifulSoup,
    logger: Optional[Callable[[str], None]] = None,
) -> Optional[float]:
    """
    Find the 'PSA Estimate' dollar value on the cert page.
    """
    label_node = soup.find(string=re.compile(r"PSA\s*Estimate", re.I))
    if label_node:
        containers = [
            getattr(label_node, "parent", None),
            getattr(getattr(label_node, "parent", None), "parent", None),
        ]
        for cont in containers:
            if not cont:
                continue
            for sib in cont.find_all(True, recursive=True):
                val = _clean_money(sib.get_text(" ", strip=True))
                if val is not None:
                    if logger:
                        logger(f"   PSA Estimate found in label container: ${val}")
                    return val
        val = _clean_money(label_node.parent.get_text(" ", strip=True))
        if val is not None:
            if logger:
                logger(f"   PSA Estimate via parent text: ${val}")
            return val

    txt = soup.get_text(" ", strip=True)
    m = re.search(
        r"PSA\s*Estimate[^$]{0,250}\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?|[0-9]+(?:\.[0-9]{2})?)",
        txt,
        re.I | re.S,
    )
    if m:
        try:
            val = float(m.group(1).replace(",", ""))
            if logger:
                logger(f"   PSA Estimate (fallback regex): ${val}")
            return val
        except Exception:
            return None
    return None


def _extract_apr_url_from_cert_soup(soup: BeautifulSoup, host: str) -> Optional[str]:
    """
    Find the Sales History / Auction Prices link on the cert page.
    """
    for a in soup.select("a"):
        txt = (a.get_text(strip=True) or "").lower()
        href = (a.get("href") or "").strip()
        if "sales history" in txt or "auction prices" in txt:
            if href.startswith("/"):
                return f"{host}{href}"
            if href.startswith("http"):
                return href
    a = soup.select_one('a[href*="/auctionprices"]')
    if a:
        href = (a.get("href") or "").strip()
        if href.startswith("/"):
            return f"{host}{href}"
        if href.startswith("http"):
            return href
    return None


# --------- Playwright-assisted helpers ---------
def _fetch_html_via_playwright(
    url: str,
    logger: Optional[Callable[[str], None]] = None,
) -> Optional[str]:
    """
    Fetch a page using Playwright headless Chromium and return the HTML.
    Used as a fallback when PSA cert pages return 403 to plain requests.
    """
    if not _PLAYWRIGHT_OK:
        if logger:
            logger("   [PW] Playwright not available; cannot fetch via browser.")
        return None

    if logger:
        logger(f"   [PW] Fetching via Playwright: {url}")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/123.0.0.0 Safari/537.36"
                )
            )
            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            html = page.content()
            browser.close()
            return html
    except Exception as e:
        if logger:
            logger(f"   [PW] Failed to fetch via Playwright: {e.__class__.__name__}: {e}")
        return None


def _apr_prices_by_grade_playwright(
    apr_url: str,
    logger: Optional[Callable[[str], None]] = None,
) -> Dict[str, Dict[str, Optional[float]]]:
    """
    Fetch the APR page with Playwright and extract the 'Auction Prices by Grade' table.
    Returns: grade_text -> {most_recent_price, average_price}
    """
    if not _PLAYWRIGHT_OK:
        if logger:
            logger("   [PW] Playwright not available; cannot use APR fallback.")
        return {}

    if logger:
        logger(f"   [PW] Launching headless Chromium for APR: {apr_url}")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/123.0.0.0 Safari/537.36"
                )
            )
            page = context.new_page()
            page.goto(apr_url, wait_until="domcontentloaded", timeout=60000)
            try:
                page.wait_for_selector(
                    "tbody.text-left.text-body1.text-primary", timeout=60000
                )
            except TimeoutError as PlaywrightTimeoutError:  # noqa: F841
                if logger:
                    logger(
                        "   [PW] Warning: APR table selector timeout, parsing whatever HTML is available."
                    )
            html = page.content()
            browser.close()
    except Exception as e:
        if logger:
            logger(
                f"   [PW] Failed to fetch APR page: {e.__class__.__name__}: {e}"
            )
        return {}

    soup = BeautifulSoup(html, "html.parser")
    tbody = soup.select_one("tbody.text-left.text-body1.text-primary")
    out: Dict[str, Dict[str, Optional[float]]] = {}
    if not tbody:
        return out

    def _parse_price(text: str) -> Optional[float]:
        text = (text or "").strip().replace("$", "").replace(",", "")
        if text in ("", "-", "—"):
            return None
        try:
            return float(text)
        except ValueError:
            return None

    for row in tbody.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 3:
            continue
        grade = cells[0].get_text(strip=True)
        most_recent_raw = cells[1].get_text(strip=True)
        average_raw = cells[2].get_text(strip=True)
        out[grade] = {
            "most_recent_price": _parse_price(most_recent_raw),
            "average_price": _parse_price(average_raw),
        }

    if logger:
        logger(f"   [PW] Parsed {len(out)} APR grade rows.")
    return out


# ---------------- PSA cert + APR ----------------
def _psa_cert_info(
    cert: str,
    *,
    force_proxy: Optional[bool] = None,
    verify_tls: bool = True,
    use_playwright_cert: bool = False,
    logger: Optional[Callable[[str], None]] = None,
) -> Tuple[str, Optional[str], Optional[float]]:
    """
    Returns (cert_url, apr_url, psa_estimate).

    Normal path:
      - Fetch PSA cert page with _fetch (requests).
    Fallback:
      - If we hit 403 Forbidden and use_playwright_cert is True, try a full
        browser fetch via Playwright to load the cert page HTML.
    """
    last_cert_url: Optional[str] = None

    for host in PSA_HOSTS:
        cert_url = f"{host}/cert/{cert}/psa"
        last_cert_url = cert_url
        if logger:
            logger(f"   PSA cert try: {cert_url}")

        html: Optional[str] = None

        # First, try the normal requests-based fetch
        try:
            r = _fetch(
                cert_url,
                allow_proxy_fallback=True,
                force_proxy=force_proxy,
                verify_tls=verify_tls,
                logger=logger,
            )
            html = r.text
        except requests.exceptions.SSLError as e:
            if logger:
                logger(
                    f"   PSA cert fetch SSL error on {cert_url}: {e}. Trying next host (if any)."
                )
            continue
        except requests.exceptions.HTTPError as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if logger:
                logger(f"   PSA cert HTTP error on {cert_url}: status={status} {e}")
            # If 403 and we're allowed to use Playwright, try browser fetch
            if status == 403 and use_playwright_cert:
                html = _fetch_html_via_playwright(cert_url, logger=logger)
                if not html:
                    # If Playwright also fails, try next host
                    continue
            else:
                # For other HTTP errors just try the next host
                continue

        if not html:
            # Nothing fetched from this host; try the next one
            continue

        # Parse the cert page HTML
        soup = BeautifulSoup(html, "html.parser")

        psa_estimate = _extract_psa_estimate_from_cert_soup(soup, logger=logger)
        apr_link = _extract_apr_url_from_cert_soup(soup, host)

        if logger:
            logger(
                f"   PSA APR link: {apr_link or '—'}; "
                f"PSA Estimate: {psa_estimate if psa_estimate is not None else '—'}"
            )

        # Return on first successful host
        return cert_url, apr_link, psa_estimate

    # If everything failed, fall back to a best-guess cert_url
    return last_cert_url or f"{PSA_HOSTS[0]}/cert/{cert}/psa", None, None


def _parse_most_recent_by_grade_from_apr_soup(
    soup: BeautifulSoup,
    grade_num: int,
    logger: Optional[Callable[[str], None]] = None,
) -> Optional[float]:
    """
    Parse the 'Auction Prices By Grade' table for a given PSA grade.
    """
    tables = soup.select("table")
    for tbl in tables:
        head_txt = (tbl.find("thead") or tbl).get_text(" ", strip=True).lower()
        if "grade" not in head_txt or "most recent price" not in head_txt:
            continue
        tbody = tbl.find("tbody")
        if not tbody:
            continue
        for tr in tbody.find_all("tr"):
            tds = tr.find_all(["td", "th"])
            if len(tds) < 2:
                continue
            grade_txt = tds[0].get_text(" ", strip=True)
            m = re.search(r"PSA\s*(\d{1,2}(?:\.\d)?)", grade_txt, re.I)
            if not m:
                continue
            this_grade = int(float(m.group(1)))
            if this_grade == int(float(grade_num)):
                price_txt = tds[1].get_text(" ", strip=True)
                val = _clean_money(price_txt)
                if val is not None:
                    if logger:
                        logger(f"   APR table: PSA {this_grade} Most Recent = ${val}")
                    return val

    txt = soup.get_text(" ", strip=True)
    m = re.search(rf"PSA\s*{int(float(grade_num))}\s*\$([0-9\.,]+)", txt)
    if m:
        try:
            val = float(m.group(1).replace(",", ""))
            if logger:
                logger(f"   APR regex fallback: Most Recent PSA {grade_num} = ${val}")
            return val
        except Exception:
            return None
    return None


def _fetch_psa_comp(
    cert: str,
    grade_num: Optional[int],
    *,
    force_proxy: Optional[bool] = None,
    verify_tls: bool = True,
    use_playwright_apr: bool = False,
    logger: Optional[Callable[[str], None]] = None,
) -> PsaComp:
    """
    Fetch PSA cert page + APR data for a given cert & grade.
    """
    cert_url, apr_url, psa_estimate = _psa_cert_info(
        cert,
        force_proxy=force_proxy,
        verify_tls=verify_tls,
        use_playwright_cert=use_playwright_apr,
        logger=logger,
    )

    most_recent_for_grade: Optional[float] = None
    median_recent_sales: Optional[float] = None
    last_n_prices: List[float] = []

    if apr_url:
        # First pass: try requests + BeautifulSoup
        r = _fetch(
            apr_url,
            allow_proxy_fallback=True,
            force_proxy=force_proxy,
            verify_tls=verify_tls,
            logger=logger,
        )
        soup = BeautifulSoup(r.text, "lxml")

        if grade_num is not None:
            most_recent_for_grade = _parse_most_recent_by_grade_from_apr_soup(
                soup,
                grade_num,
                logger=logger,
            )

        # If that failed and we're allowed to, try Playwright APR table
        if most_recent_for_grade is None and use_playwright_apr and grade_num is not None:
            if logger:
                logger("   APR table parse failed; attempting Playwright APR fallback…")
            table = _apr_prices_by_grade_playwright(apr_url, logger=logger)
            key_variants = [
                f"PSA {int(float(grade_num))}",
                f"PSA{int(float(grade_num))}",
                str(int(float(grade_num))),
            ]
            for k in key_variants:
                if k in table and table[k].get("most_recent_price") is not None:
                    most_recent_for_grade = table[k]["most_recent_price"]
                    if logger:
                        logger(
                            f"   [PW] Most Recent for grade found in APR: ${most_recent_for_grade}"
                        )
                    break
        _throttle()

        # Collect a bunch of $ amounts across the APR page (used for median fallback)
        text_prices = re.findall(
            r"\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?|[0-9]+(?:\.[0-9]{2})?)",
            soup.get_text(" ", strip=True),
        )
        for h in text_prices[:25]:
            try:
                last_n_prices.append(float(h.replace(",", "")))
            except Exception:
                continue
        if last_n_prices:
            sorted_vals = sorted(last_n_prices)
            median_recent_sales = sorted_vals[len(sorted_vals) // 2]
            if logger:
                logger(f"   PSA APR: median recent (all rows) = ${median_recent_sales}")

    return PsaComp(
        cert_url=cert_url,
        apr_url=apr_url,
        most_recent_for_grade=most_recent_for_grade,
        median_recent_sales=median_recent_sales,
        psa_estimate=psa_estimate,
        last_n_prices=last_n_prices,
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
    use_playwright_apr: bool = False,
    logger: Optional[Callable[[str], None]] = None,
) -> pd.DataFrame:
    """
    Main entrypoint for the Streamlit app.

    - Discovers products for each selected CardsHQ category.
    - Scrapes each product page.
    - Filters to PSA-like listings (cert or grade or PSA in title).
    - Fetches PSA cert + APR data when a cert number exists.
    - Computes expected net and ROI using the best available PSA value.
    """
    selected: Dict[str, str] = {}
    for label in categories:
        if label in CARDSHQ_CATEGORY_URLS:
            selected[label] = CARDSHQ_CATEGORY_URLS[label]

    rows: List[Dict] = []
    for label, first_page_url in selected.items():
        if logger:
            logger(f"[{label}] discovering products…")
        product_urls = _discover_product_urls_for_category(
            first_page_url,
            max_pages=200,
            force_proxy=force_proxy,
            verify_tls=verify_tls,
            logger=logger,
        )

        found_items: List[StoreItem] = []
        total_products = 0

        for idx, pu in enumerate(product_urls, start=1):
            _throttle()
            item = _scrape_cardshq_product(
                pu,
                force_proxy=force_proxy,
                verify_tls=verify_tls,
                logger=logger,
            )
            if not item:
                continue
            total_products += 1

            # Treat anything clearly PSA-graded as a candidate
            text_blob = f"{item.card_name} {item.psa_grade_text or ''}".upper()
            is_psa_like = (
                (item.psa_cert is not None)
                or (item.psa_grade_num is not None)
                or (" PSA " in f" {text_blob} ")
            )
            if is_psa_like:
                found_items.append(item)
                if limit_per_category and len(found_items) >= limit_per_category:
                    if logger:
                        logger(f"  reached limit_per_category={limit_per_category}")
                    break

            if idx % 25 == 0 and logger:
                logger(f"  parsed {idx}/{len(product_urls)} product pages…")

        if logger:
            logger(
                f"[{label}] total products parsed: {total_products}, "
                f"PSA-like listings: {len(found_items)} — fetching PSA comps…"
            )

        for it in found_items:
            _throttle()

            # If there is no PSA cert, we can't hit PSA; still record the store-side info.
            if not it.psa_cert:
                if logger:
                    logger(
                        "   listing appears PSA-graded but has no cert number on page; "
                        f"keeping Store row only: {it.url}"
                    )
                rows.append(
                    {
                        "Category": label,
                        "Store": it.source,
                        "Card Name": it.card_name,
                        "Store Price": it.price,
                        "PSA Grade": it.psa_grade_text,
                        "PSA Cert": None,
                        "PSA Cert URL": None,
                        "PSA APR URL": None,
                        "PSA Estimate (cert page)": None,
                        "APR Most Recent (Grade)": None,
                        "APR Median Recent (All)": None,
                        "Expected Net (est)": None,
                        "ROI % (est)": None,
                        "Store URL": it.url,
                    }
                )
                continue

            try:
                comp = _fetch_psa_comp(
                    it.psa_cert,
                    it.psa_grade_num,
                    force_proxy=force_proxy,
                    verify_tls=verify_tls,
                    use_playwright_apr=use_playwright_apr,
                    logger=logger,
                )
            except requests.exceptions.HTTPError as e:
                if logger:
                    status = getattr(
                        getattr(e, "response", None), "status_code", "unknown"
                    )
                    logger(
                        "   PSA HTTP error for cert "
                        f"{it.psa_cert or 'unknown'} (status={status}): {e}. "
                        "Skipping PSA data for this card."
                    )
                comp = None

            comp_value: Optional[float] = None
            psa_cert_url: Optional[str] = None
            psa_apr_url: Optional[str] = None
            psa_estimate: Optional[float] = None
            apr_most_recent: Optional[float] = None
            apr_median_all: Optional[float] = None

            if comp is not None:
                psa_cert_url = comp.cert_url
                psa_apr_url = comp.apr_url
                psa_estimate = comp.psa_estimate
                apr_most_recent = comp.most_recent_for_grade
                apr_median_all = comp.median_recent_sales

                for v in (apr_most_recent, psa_estimate, apr_median_all):
                    if v is not None:
                        comp_value = v
                        break

            expected_net: Optional[float] = None
            roi_pct: Optional[float] = None
            if comp_value is not None and it.price and it.price > 0:
                expected_net = comp_value * (1 - fee_rate) - ship_out
                roi_pct = (expected_net - it.price) / it.price * 100

            rows.append(
                {
                    "Category": label,
                    "Store": it.source,
                    "Card Name": it.card_name,
                    "Store Price": it.price,
                    "PSA Grade": it.psa_grade_text,
                    "PSA Cert": it.psa_cert,
                    "PSA Cert URL": psa_cert_url,
                    "PSA APR URL": psa_apr_url,
                    "PSA Estimate (cert page)": psa_estimate,
                    "APR Most Recent (Grade)": apr_most_recent,
                    "APR Median Recent (All)": apr_median_all,
                    "Expected Net (est)": round(expected_net, 2)
                    if expected_net is not None
                    else None,
                    "ROI % (est)": round(roi_pct, 2) if roi_pct is not None else None,
                    "Store URL": it.url,
                }
            )

    df = pd.DataFrame(rows)
    if not df.empty and "ROI % (est)" in df.columns:
        df = df.sort_values(
            by=["ROI % (est)"],
            ascending=False,
            na_position="last",
        )
    if logger:
        logger(f"[done] total rows={len(df)}")
    return df


def test_psa_cert(
    cert: str,
    grade_num: Optional[int] = None,
    *,
    force_proxy: Optional[bool] = None,
    verify_tls: bool = True,
    use_playwright_apr: bool = False,
    logger: Optional[Callable[[str], None]] = None,
) -> Dict[str, Optional[float]]:
    """
    Convenience helper for the "Quick PSA Cert Test" in the Streamlit app.
    """
    if logger:
        logger(f"[test] PSA cert {cert}  grade={grade_num or '—'}")
    comp = _fetch_psa_comp(
        cert,
        grade_num,
        force_proxy=force_proxy,
        verify_tls=verify_tls,
        use_playwright_apr=use_playwright_apr,
        logger=logger,
    )
    value: Optional[float] = None
    for v in (
        comp.most_recent_for_grade,
        comp.psa_estimate,
        comp.median_recent_sales,
    ):
        if v is not None:
            value = v
            break
    if logger:
        logger(f"[test] chosen value=${value if value is not None else '—'}")
    return {
        "PSA Cert URL": comp.cert_url,
        "PSA APR URL": comp.apr_url,
        "PSA Estimate (cert page)": comp.psa_estimate,
        "APR Most Recent (Grade)": comp.most_recent_for_grade,
        "APR Median Recent (All)": comp.median_recent_sales,
        "Chosen Value": value,
    }
