# cards_cert_arbitrage.py
from __future__ import annotations

import os
import re
import time
import math
import urllib.parse
import warnings
from dataclasses import dataclass
from typing import Callable, Iterable, Optional, Tuple, List, Dict

import requests
import certifi
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3.exceptions import InsecureRequestWarning
from bs4 import BeautifulSoup

# -----------------------------
# Config
# -----------------------------
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/122.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

BS_PARSER = os.environ.get("BS_PARSER", "html.parser")  # 'lxml' ok if installed

CARD_SHOP_CATEGORIES: Dict[str, str] = {
    "Baseball": "https://www.cardshq.com/collections/baseball-cards?page=1",
    "Basketball (Graded)": "https://www.cardshq.com/collections/basketball-graded?page=1",
    "Football": "https://www.cardshq.com/collections/football-cards?page=1",
    "Soccer": "https://www.cardshq.com/collections/soccer-cards?page=1",
    "Pokemon": "https://www.cardshq.com/collections/pokemon-cards?page=1",
}

PSA_BASE = "https://www.psacard.com"

MONEY_RE = re.compile(r"\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?)")
CERT_RE = re.compile(r"/cert/(\d+)/psa")
CERT_NUM_RE = re.compile(r"\b(\d{6,9})\b")
PSA_GRADE_WORD_RE = re.compile(r"\b(Pr|Fr|Gd|VG|EX|EX\-MT|NM|NM\-MT|Mint|Gem Mt|GEM MT|GEM-MT|MINT|GEM)\s*([0-9](?:\.[0-9])?)?\b", re.I)
PSA_GRADE_NUM_RE = re.compile(r"\bPSA\s*([0-9](?:\.[0-9])?)\b", re.I)

# -----------------------------
# HTTP Session
# -----------------------------
def _build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    s.verify = certifi.where()  # always validate with certifi
    retry = Retry(
        total=3,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

SESSION = _build_session()

def _get(url: str, *, verify_tls: bool) -> requests.Response:
    """
    Verified calls go through the shared SESSION (certifi + adapters).
    Insecure calls use a one-off raw requests.get(verify=False), suppressing warnings.
    This avoids the 'CERT_NONE with check_hostname=True' crash.
    """
    if verify_tls:
        r = SESSION.get(url, timeout=30, verify=certifi.where())
        r.raise_for_status()
        return r
    else:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", InsecureRequestWarning)
            r = requests.get(url, timeout=30, verify=False, headers=HEADERS)
            r.raise_for_status()
            return r

def _fetch(
    url: str,
    *,
    force_proxy: bool | None = None,
    verify_tls: bool = True,
    allow_psa_insecure_retry: bool = True,
    allow_proxy_fallback: bool | None = None,   # legacy (ignored)
    logger: Callable[[str], None] | None = None,
) -> requests.Response:
    if logger:
        mode = "auto" if force_proxy is None else ("proxy" if force_proxy else "direct")
        logger(f"GET {url}  | mode={mode}  tls_verify={'ON' if verify_tls else 'OFF'}")
    try:
        return _get(url, verify_tls=verify_tls)
    except requests.exceptions.SSLError:
        # PSA-only last-resort retry without TLS verification
        if allow_psa_insecure_retry and ("psacard.com" in url):
            if logger:
                logger("  !! SSL error — PSA insecure retry (verify=OFF, raw requests.get)")
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", InsecureRequestWarning)
                r = requests.get(url, timeout=30, verify=False, headers=HEADERS)
                r.raise_for_status()
                return r
        raise

# -----------------------------
# Models / helpers
# -----------------------------
@dataclass
class Listing:
    category: str
    product_url: str
    title: str
    price: float | None
    psa_grade_text: str | None
    psa_grade_num: float | None
    psa_cert: str | None

@dataclass
class PsaComp:
    cert_url: str | None
    apr_url: str | None
    psa_estimate: float | None
    apr_most_recent: float | None
    apr_average: float | None

def _money_to_float(text: str | None) -> float | None:
    if not text:
        return None
    m = MONEY_RE.search(text)
    if not m:
        return None
    return float(m.group(1).replace(",", ""))

def _extract_psa_grade(text: str) -> Tuple[str | None, float | None]:
    """
    Try to extract a textual PSA grade ('Gem Mt 10') and its numeric (10.0) from arbitrary text.
    """
    # Numeric like "PSA 10"
    m = PSA_GRADE_NUM_RE.search(text)
    if m:
        try:
            num = float(m.group(1))
            return f"PSA {m.group(1)}", num
        except Exception:
            pass

    # Wordy "Gem Mt 10", "Mint 9", etc.
    m = PSA_GRADE_WORD_RE.search(text)
    if m:
        maybe_num = None
        if m.group(2):
            try:
                maybe_num = float(m.group(2))
            except Exception:
                maybe_num = None
        # reconstruct readable
        word = m.group(1)
        if maybe_num is not None:
            return f"{word} {m.group(2)}", maybe_num
        return word, None

    return None, None

# -----------------------------
# CardsHQ parsing
# -----------------------------
def _discover_product_urls_for_category(
    first_page_url: str,
    *,
    max_pages: int = 200,
    force_proxy: bool | None = None,
    verify_tls: bool = True,
    logger: Callable[[str], None] | None = None,
) -> List[str]:
    """
    Walk paginated CardsHQ collection pages and collect product URLs until a page has 0 links
    or we reach max_pages.
    """
    base = first_page_url.split("?")[0]
    page = 1
    all_urls: List[str] = []

    if logger:
        logger(f"[{first_page_url}] discovering products…")

    while page <= max_pages:
        url = f"{base}?page={page}"
        r = _fetch(url, force_proxy=force_proxy, verify_tls=verify_tls, logger=logger)
        soup = BeautifulSoup(r.text, BS_PARSER)

        # Products appear as <a href="/products/...">
        links = []
        for a in soup.select("a[href]"):
            href = a["href"]
            if "/products/" in href:
                if href.startswith("/"):
                    links.append("https://www.cardshq.com" + href)
                elif href.startswith("http"):
                    links.append(href)

        # Dedup this page
        links = list(dict.fromkeys(links))
        if logger:
            logger(f"  page {page}: found {len(links)} product links")
        if not links:
            break

        all_urls.extend(links)
        page += 1

    if logger:
        logger(f"[{first_page_url}] total discovered: {len(all_urls)}")
    return all_urls

def _parse_cardshq_product(
    html: str,
    product_url: str,
    category: str,
) -> Listing:
    soup = BeautifulSoup(html, BS_PARSER)

    # Title
    title = soup.title.get_text(" ", strip=True) if soup.title else product_url
    # Price: <div class="mr-auto flex w-auto items-center py-2 text-xl"><p>$80.00<span class="ml-1 inline">USD</span></p></div>
    price = None
    # Try the exact element you sent
    price_el = soup.select_one("div.mr-auto.flex.w-auto.items-center.py-2.text-xl")
    if price_el:
        price = _money_to_float(price_el.get_text(" ", strip=True))
    if price is None:
        # Fallback: first $ on page
        price = _money_to_float(soup.get_text(" ", strip=True))

    # PSA grade + cert: typically in product title slug or page text
    page_text = soup.get_text(" ", strip=True)

    # cert inside title/url
    cert = None
    # Often the product handle ends with the cert
    m = CERT_NUM_RE.search(product_url)
    if m:
        cert = m.group(1)
    else:
        m2 = CERT_NUM_RE.search(page_text)
        if m2:
            cert = m2.group(1)

    grade_txt, grade_num = _extract_psa_grade(title + " " + page_text)

    return Listing(
        category=category,
        product_url=product_url,
        title=title,
        price=price,
        psa_grade_text=grade_txt,
        psa_grade_num=grade_num,
        psa_cert=cert,
    )

# -----------------------------
# PSA parsing
# -----------------------------
def _parse_psa_estimate_from_cert_soup(soup: BeautifulSoup) -> float | None:
    """
    Find 'PSA Estimate' container and read the nearest $ value.
    """
    # Look for the label first
    est_label_node = None
    for node in soup.find_all(string=True):
        if isinstance(node, str) and "PSA Estimate" in node:
            est_label_node = node
            break

    # Search up a few levels for a money token
    if est_label_node:
        container = est_label_node.parent
        for _ in range(4):
            if not container:
                break
            val = _money_to_float(container.get_text(" ", strip=True))
            if val is not None:
                return val
            container = container.parent

    # Fallback: first money near the first occurrence
    full = soup.get_text(" ", strip=True)
    if "PSA Estimate" in full:
        idx = full.index("PSA Estimate")
        tail = full[idx: idx + 500]
        val = _money_to_float(tail)
        if val is not None:
            return val
    return None

def _extract_apr_url_from_cert_soup(soup: BeautifulSoup) -> str | None:
    # Find the Sales History link; any <a> that contains '/auctionprices' or text 'Sales History'
    for a in soup.select("a[href]"):
        href = a["href"]
        text = (a.get_text(strip=True) or "").lower()
        if "/auctionprices" in href or "sales history" in text:
            if href.startswith("/"):
                return PSA_BASE + href
            if href.startswith("http"):
                return href
    return None

def _parse_apr_table_for_grade(html: str, grade_num: float | None) -> Tuple[float | None, float | None]:
    """
    Parse 'Auction Prices By Grade' table to get (Most Recent, Average) for the grade.
    If grade_num is None, try 10/9/8/7 in that order then any row.
    """
    soup = BeautifulSoup(html, BS_PARSER)
    table_text = soup.get_text(" ", strip=True)

    def as_float_str(s: str) -> float | None:
        try:
            return float(s.replace(",", ""))
        except Exception:
            return None

    grades = [grade_num] if grade_num is not None else [10, 9, 8, 7]
    # pattern like 'PSA 10 $105.05 $40.46'
    for g in grades:
        pat = re.compile(rf"PSA\s*{int(g) if g and g.is_integer() else g}\s*{MONEY_RE.pattern}\s*{MONEY_RE.pattern}")
        m = pat.search(table_text)
        if m:
            most_recent = as_float_str(m.group(1))
            avg_price = as_float_str(m.group(2))
            return most_recent, avg_price

    # Fallback: first PSA row in table-like text
    any_row = re.search(rf"PSA\s*(\d+(?:\.\d)?)\s*{MONEY_RE.pattern}\s*{MONEY_RE.pattern}", table_text)
    if any_row:
        most_recent = as_float_str(any_row.group(2))
        avg_price = as_float_str(any_row.group(3))
        return most_recent, avg_price

    return None, None

def _psa_cert_info(
    cert: str,
    *,
    force_proxy: bool | None,
    verify_tls: bool,
    logger: Callable[[str], None] | None,
) -> Tuple[str | None, str | None, float | None]:
    """
    Return (cert_url, apr_url, psa_estimate).
    """
    # Try both hosts in order; each via verified fetch with PSA-only insecure fallback inside _fetch.
    for host in ("https://www.psacard.com", "https://psacard.com"):
        cert_url = f"{host}/cert/{cert}/psa"
        if logger:
            logger(f"   PSA cert try: {cert_url}")
        r = _fetch(cert_url, force_proxy=force_proxy, verify_tls=verify_tls, allow_psa_insecure_retry=True, logger=logger)
        soup = BeautifulSoup(r.text, BS_PARSER)
        est = _parse_psa_estimate_from_cert_soup(soup)
        apr_url = _extract_apr_url_from_cert_soup(soup)
        if est is not None or apr_url:
            return cert_url, apr_url, est
    return None, None, None

def _fetch_psa_comp(
    cert: str,
    grade_num: float | None,
    *,
    force_proxy: bool | None,
    verify_tls: bool,
    logger: Callable[[str], None] | None,
) -> PsaComp:
    cert_url, apr_url, est = _psa_cert_info(cert, force_proxy=force_proxy, verify_tls=verify_tls, logger=logger)

    most_recent = avg_price = None
    if apr_url:
        r2 = _fetch(apr_url, force_proxy=force_proxy, verify_tls=verify_tls, allow_psa_insecure_retry=True, logger=logger)
        most_recent, avg_price = _parse_apr_table_for_grade(r2.text, grade_num)

    return PsaComp(
        cert_url=cert_url,
        apr_url=apr_url,
        psa_estimate=est,
        apr_most_recent=most_recent,
        apr_average=avg_price,
    )

# -----------------------------
# Scan API used by streamlit_app
# -----------------------------
def scan_selected_categories(
    *,
    categories: Iterable[str],
    limit_per_category: int = 10,
    selling_fee_rate: float = 0.13,
    outbound_shipping: float = 4.0,
    force_proxy: bool | None = None,
    verify_tls: bool = True,
    logger: Callable[[str], None] | None = None,
) -> List[Dict[str, object]]:
    """
    Crawl CardsHQ categories, parse PSA listings, fetch PSA comps, and compute margins.
    Returns list of dict rows suitable for pandas.
    """
    rows: List[Dict[str, object]] = []

    if logger:
        net_mode = "Auto" if force_proxy is None else ("Proxy" if force_proxy else "Direct")
        logger(f"START scan | mode={net_mode} | TLS={'ON' if verify_tls else 'OFF'} | parser={BS_PARSER}")

    for cat in categories:
        first_page = CARD_SHOP_CATEGORIES[cat]
        if logger:
            logger(f"[{cat}] discovering products…")
        product_urls = _discover_product_urls_for_category(
            first_page,
            max_pages=200,
            force_proxy=force_proxy,
            verify_tls=verify_tls,
            logger=logger,
        )

        if logger:
            logger(f"[{first_page}] total discovered: {len(product_urls)}")

        # Walk products until we get limit_per_category PSA-certified items
        found = 0
        for pu in product_urls:
            if found >= limit_per_category:
                if logger:
                    logger("  reached limit_per_category=%d" % limit_per_category)
                break

            r = _fetch(pu, force_proxy=force_proxy, verify_tls=verify_tls, logger=logger)
            listing = _parse_cardshq_product(r.text, pu, cat)

            # Always record a row; but only comps for PSA cert listings
            if listing.psa_cert and listing.psa_grade_num is not None:
                found += 1
                if logger:
                    logger(f"  parsed product | cert={listing.psa_cert} grade={listing.psa_grade_text or '-'} price={listing.price or '-'}")
                comp = _fetch_psa_comp(
                    listing.psa_cert,
                    listing.psa_grade_num,
                    force_proxy=force_proxy,
                    verify_tls=verify_tls,
                    logger=logger,
                )
            else:
                if logger and listing.psa_cert:
                    logger(f"  parsed product | cert={listing.psa_cert} grade=— price={listing.price or '-'}")
                elif logger:
                    logger(f"  parsed product | cert=— grade=— price={listing.price or '-'}")
                comp = PsaComp(None, None, None, None, None)

            # choose comp value heuristic
            comp_value = comp.apr_most_recent or comp.psa_estimate or comp.apr_average

            # compute margin if possible
            store_price = listing.price or None
            comp_net = None
            est_profit = None
            margin_pct = None
            if comp_value is not None:
                comp_net = comp_value * (1.0 - selling_fee_rate) - outbound_shipping
                if store_price is not None:
                    est_profit = comp_net - store_price
                    if store_price > 0:
                        margin_pct = est_profit / store_price

            rows.append({
                "Category": listing.category,
                "Title": listing.title,
                "Product URL": listing.product_url,
                "Store Price": store_price,
                "PSA Grade (text)": listing.psa_grade_text,
                "PSA Grade (num)": listing.psa_grade_num,
                "PSA Cert": listing.psa_cert,
                "PSA Cert URL": comp.cert_url,
                "PSA APR URL": comp.apr_url,
                "PSA Estimate": comp.psa_estimate,
                "APR Most Recent (Grade)": comp.apr_most_recent,
                "APR Average (Grade)": comp.apr_average,
                "Chosen Comp": comp_value,
                "Comp Net (after fees+ship)": comp_net,
                "Est. Profit": est_profit,
                "Margin %": margin_pct,
            })

    if logger:
        logger("[done] total rows=%d" % len(rows))
        logger("Scan finished.")
    return rows

# -----------------------------
# Optional single-cert debugger
# -----------------------------
def test_psa_cert(
    cert: str,
    *,
    grade_num: float | None = None,
    force_proxy: bool | None = None,
    verify_tls: bool = True,
    logger: Callable[[str], None] | None = None,
) -> Dict[str, object]:
    if logger:
        logger(f"[PSA Debug] testing cert {cert} (grade={grade_num or 'auto'})")
    comp = _fetch_psa_comp(cert, grade_num, force_proxy=force_proxy, verify_tls=verify_tls, logger=logger)
    chosen = comp.apr_most_recent or comp.psa_estimate or comp.apr_average
    return {
        "PSA Cert URL": comp.cert_url,
        "PSA APR URL": comp.apr_url,
        "PSA Estimate": comp.psa_estimate,
        "APR Most Recent (Grade)": comp.apr_most_recent,
        "APR Average (Grade)": comp.apr_average,
        "Chosen Comp": chosen,
    }
