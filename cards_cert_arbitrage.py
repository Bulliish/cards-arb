import re
import time
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple

import requests
from bs4 import BeautifulSoup
import pandas as pd

# ---------------- Config ----------------
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
}
THROTTLE = 1.25  # seconds between requests — be nice
BASE = "https://www.cardshq.com"

# The only lists we scan (provided by user)
CARDSHQ_CATEGORY_URLS = {
    "Baseball":   f"{BASE}/collections/baseball-cards?page=1",
    "Basketball (Graded)": f"{BASE}/collections/basketball-graded?page=1",
    "Football":   f"{BASE}/collections/football-cards?page=1",
    "Soccer":     f"{BASE}/collections/soccer-cards?page=1",
    "Pokemon":    f"{BASE}/collections/pokemon-cards?page=1",
}

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
def _throttle():
    time.sleep(THROTTLE)

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
    """
    Crawl the given CardsHQ collection, paginating until no products are found.
    Returns a list of product URLs (absolute).
    """
    urls: List[str] = []
    # Normalize to base path (replace ?page=1 with ?page=N as we increment)
    base_no_page = re.sub(r"(\?|&)page=\d+", "", category_url_first_page)
    page = 1
    while page <= max_pages:
        # Keep original param order; append page as needed
        sep = "&" if "?" in base_no_page else "?"
        url = f"{base_no_page}{sep}page={page}"
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200:
            break
        soup = BeautifulSoup(r.text, "lxml")
        # Product anchors containing /products/
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
            # No products => end
            break
        urls.extend(page_urls)
        page += 1
        _throttle()
    return urls

# ---------------- Product page parser ----------------
def _scrape_cardshq_product(url: str) -> Optional[StoreItem]:
    """
    Extract name, price, PSA grade (text + number), PSA cert from a CardsHQ product page.
    """
    r = requests.get(url, headers=HEADERS, timeout=30)
    if r.status_code != 200:
        return None
    soup = BeautifulSoup(r.text, "lxml")

    # Name / title
    h1 = soup.find(["h1", "h2"], string=True)
    name = h1.get_text(" ", strip=True) if h1 else url

    # Price: try common selectors then meta
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

    # Whole page text (upper) for regex
    body_txt = soup.get_text(" ", strip=True)
    body_up = body_txt.upper()

    # PSA Cert: primary pattern "Certification #123456789"
    cert = None
    m_cert = re.search(r'CERTIFICATION\s*#\s*(\d{6,9})', body_up)
    if m_cert:
        cert = m_cert.group(1)
    else:
        # Fallback like "... PSA ... #123456789"
        m_alt = re.search(r'PSA[^#]{0,30}#\s*(\d{6,9})', body_up)
        if m_alt:
            cert = m_alt.group(1)

    # PSA Grade
    grade_text = None
    # Pattern like: "Grade: GEM MT 10" or "Grade: MINT 9"
    m_grade = re.search(r'GRADE\s*:\s*([A-Z\s\-]*\d{1,2})', body_up)
    if m_grade:
        grade_text = m_grade.group(1).title()
    else:
        # Titles/descriptions often have "PSA 10" or "PSA 9"
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
    """
    Return (cert_url, apr_url) if found.
    """
    cert_url = f"https://www.psacard.com/cert/{cert}/psa"
    r = requests.get(cert_url, headers=HEADERS, timeout=30)
    if r.status_code != 200:
        return cert_url, None
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
    """
    Parse the APR page for the "PSA {grade} $X" 'Most Recent Price' value.
    """
    r = requests.get(apr_url, headers=HEADERS, timeout=30)
    if r.status_code != 200:
        return None
    text = BeautifulSoup(r.text, "lxml").get_text(" ", strip=True)
    m = re.search(rf'PSA\s*{grade_num}\s*\$([0-9\.,]+)', text)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except Exception:
        return None

def _psa_apr_recent_prices(apr_url: str, take: int = 25) -> List[float]:
    """
    Lightweight scan of the APR page to collect recent price numbers ($XX.XX).
    """
    r = requests.get(apr_url, headers=HEADERS, timeout=30)
    if r.status_code != 200:
        return []
    text = BeautifulSoup(r.text, "lxml").get_text(" ", strip=True)
    hits = re.findall(r'\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?|[0-9]+(?:\.[0-9]{2})?)', text)
    out: List[float] = []
    for h in hits[:take]:
        try:
            out.append(float(h.replace(",", "")))
        except Exception:
            continue
    return out

def _fetch_psa_comp(cert: str, grade_num: Optional[int]) -> PsaComp:
    cert_url, apr_url = _psa_cert_and_apr_urls(cert)
    most_recent_for_grade = None
    median_recent_sales = None
    last_n_prices: List[float] = []
    if apr_url:
        if grade_num is not None:
            most_recent_for_grade = _psa_apr_most_recent_for_grade(apr_url, grade_num)
            _throttle()
        last_n_prices = _psa_apr_recent_prices(apr_url, take=25)
        if last_n_prices:
            sorted_vals = sorted(last_n_prices)
            median_recent_sales = sorted_vals[len(sorted_vals)//2]
    return PsaComp(
        cert_url=cert_url,
        apr_url=apr_url,
        most_recent_for_grade=most_recent_for_grade,
        median_recent_sales=median_recent_sales,
        last_n_prices=last_n_prices
    )

# ---------------- Public orchestrator ----------------
def scan_selected_categories(
    categories: List[str],
    limit_per_category: Optional[int] = None,
    fee_rate: float = 0.13,
    ship_out: float = 5.0
) -> pd.DataFrame:
    """
    Crawl the selected CardsHQ categories, visit each product page, extract:
     - store name, url, card name, store price, PSA grade, PSA cert
     - PSA cert & APR URLs, Most Recent Price for the grade, and a median of recent prices
     - compute a simple expected net and ROI% using fee_rate and ship_out

    limit_per_category: if provided, stop after N PSA-cert listings per category (post-parse).
    """
    # Resolve which category URLs to use
    selected: Dict[str, str] = {}
    for label in categories:
        if label in CARDSHQ_CATEGORY_URLS:
            selected[label] = CARDSHQ_CATEGORY_URLS[label]

    rows: List[Dict] = []
    for label, first_page_url in selected.items():
        # 1) Discover product URLs by paginating this category until exhaustion
        product_urls = _discover_product_urls_for_category(first_page_url, max_pages=200)
        # 2) Parse each product page for cert+grade
        found_items: List[StoreItem] = []
        for pu in product_urls:
            _throttle()
            item = _scrape_cardshq_product(pu)
            if not item:
                continue
            # Only keep PSA-graded with a cert number
            if item.psa_cert and item.psa_grade_num is not None:
                found_items.append(item)
                if limit_per_category and len(found_items) >= limit_per_category:
                    break

        # 3) For each valid item, fetch PSA APR and compute ROI
        for it in found_items:
            _throttle()
            comp = _fetch_psa_comp(it.psa_cert, it.psa_grade_num)
            comp_value = comp.most_recent_for_grade or comp.median_recent_sales

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
                "APR Most Recent (Grade)": comp.most_recent_for_grade,
                "APR Median Recent (All)": comp.median_recent_sales,
                "Expected Net (est)": round(expected_net, 2) if expected_net is not None else None,
                "ROI % (est)": round(roi_pct, 2) if roi_pct is not None else None,
                "Store URL": it.url
            })

    df = pd.DataFrame(rows)
    if not df.empty and "ROI % (est)" in df.columns:
        df = df.sort_values(by=["ROI % (est)"], ascending=False, na_position="last")
    return df
