import re, time
from dataclasses import dataclass
from typing import Optional, List, Dict
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import pandas as pd

HEADERS = {"User-Agent":"Mozilla/5.0"}
THROTTLE = 1.0  # be nice to sites

@dataclass
class StoreItem:
    source: str
    url: str
    card_name: str
    price: float
    psa_grade_text: Optional[str]
    psa_grade_num: Optional[int]
    psa_cert: Optional[str]

def _clean_money(txt: str) -> Optional[float]:
    m = re.search(r'[\$€£]\s?([0-9\.,]+)', txt or "")
    return float(m.group(1).replace(",", "")) if m else None

def _grade_num_from_text(grade_text: Optional[str]) -> Optional[int]:
    if not grade_text: return None
    m = re.search(r'(\d{1,2})', grade_text)
    return int(m.group(1)) if m else None

# ---------- Shopify sitemap discoverer ----------
def _get_shopify_product_urls(domain: str, max_sitemaps: int = 3, max_urls: int = 400) -> List[str]:
    rootmap = f"https://{domain}/sitemap.xml"
    r = requests.get(rootmap, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    smaps = [loc.get_text(strip=True) for loc in soup.select("sitemap loc") if "products" in loc.get_text("")]
    urls = []
    for sm in smaps[:max_sitemaps]:
        time.sleep(THROTTLE)
        rr = requests.get(sm, headers=HEADERS, timeout=30)
        if rr.status_code != 200: continue
        ss = BeautifulSoup(rr.text, "lxml")
        for loc in ss.select("url loc"):
            u = loc.get_text(strip=True)
            if "/products/" in u:
                urls.append(u)
                if len(urls) >= max_urls: return urls
    return urls

# ---------- CardsHQ product parser (PSA cert + grade are in body text) ----------
def _scrape_cardshq_product(url: str) -> Optional[StoreItem]:
    r = requests.get(url, headers=HEADERS, timeout=30)
    if r.status_code != 200: return None
    soup = BeautifulSoup(r.text, "lxml")

    # Title
    h1 = soup.find(["h1","h2"], string=True)
    name = h1.get_text(" ", strip=True) if h1 else url

    # Price
    price = None
    price_el = soup.select_one('[class*="price"], [data-product-price], .price__regular, .price-item, .price')
    if price_el:
        price = _clean_money(price_el.get_text(" ", strip=True))
    if price is None:
        meta_price = soup.select_one('meta[itemprop="price"]')
        if meta_price and meta_price.get("content"):
            try: price = float(meta_price["content"])
            except: pass

    body_txt = soup.get_text(" ", strip=True).upper()

    cert = None
    m_cert = re.search(r'CERTIFICATION\s*#\s*(\d{6,9})', body_txt)
    if m_cert: cert = m_cert.group(1)

    grade_text = None
    m_grade = re.search(r'GRADE:\s*([A-Z\s\-]*\d{1,2})', body_txt)
    if m_grade:
        grade_text = m_grade.group(1).title()
    if not grade_text:
        m_just_num = re.search(r'PSA\s*(\d{1,2})', body_txt)
        if m_just_num: grade_text = f"PSA {m_just_num.group(1)}"

    grade_num = _grade_num_from_text(grade_text)

    return StoreItem(
        source="cardshq.com",
        url=url,
        card_name=name,
        price=price if price is not None else -1.0,
        psa_grade_text=grade_text,
        psa_grade_num=grade_num,
        psa_cert=cert
    )

# ---------- PSA: cert -> APR page + price by grade ----------
def _psa_cert_pages(cert: str):
    cert_url = f"https://www.psacard.com/cert/{cert}/psa"
    r = requests.get(cert_url, headers=HEADERS, timeout=30)
    if r.status_code != 200:
        return cert_url, None, None
    soup = BeautifulSoup(r.text, "lxml")
    apr_link = None
    for a in soup.select("a"):
        href = a.get("href") or ""
        if "auctionprices" in href.lower():
            apr_link = href
            break
        if a.get_text(strip=True).lower() == "sales history":
            apr_link = href
            break
    if apr_link and apr_link.startswith("/"):
        apr_link = "https://www.psacard.com" + apr_link
    return cert_url, apr_link, soup

def _psa_apr_most_recent_for_grade(apr_url: str, grade_num: int) -> Optional[float]:
    r = requests.get(apr_url, headers=HEADERS, timeout=30)
    if r.status_code != 200: return None
    text = BeautifulSoup(r.text, "lxml").get_text(" ", strip=True)
    m = re.search(rf'PSA\s*{grade_num}\s*\$([0-9\.,]+)', text)
    return float(m.group(1).replace(",", "")) if m else None

# ---------- Public function used by the Streamlit app ----------
def find_deals_cardshq(limit: int = 40, fee_rate: float = 0.13, ship_out: float = 5.0) -> pd.DataFrame:
    domain = "www.cardshq.com"
    urls = _get_shopify_product_urls(domain, max_sitemaps=3, max_urls=limit*3)

    items: List[StoreItem] = []
    for u in urls:
        time.sleep(THROTTLE)
        it = _scrape_cardshq_product(u)
        if it and it.psa_cert and it.psa_grade_num:
            items.append(it)
        if len(items) >= limit: break

    rows = []
    for it in items:
        time.sleep(THROTTLE)
        cert_url, apr_url, _ = _psa_cert_pages(it.psa_cert)
        most_recent = None
        if apr_url and it.psa_grade_num:
            most_recent = _psa_apr_most_recent_for_grade(apr_url, it.psa_grade_num)

        expected_net = None
        roi_pct = None
        if most_recent and it.price and it.price > 0:
            expected_net = most_recent * (1 - fee_rate) - ship_out
            roi_pct = (expected_net - it.price) / it.price * 100

        rows.append({
            "Store": it.source,
            "Card Name": it.card_name,
            "Store Price": it.price,
            "PSA Grade": it.psa_grade_text,
            "PSA Cert": it.psa_cert,
            "PSA Cert URL": cert_url,
            "PSA APR URL": apr_url,
            "APR Most Recent (Grade)": most_recent,
            "Expected Net (est)": round(expected_net, 2) if expected_net is not None else None,
            "ROI % (est)": round(roi_pct, 2) if roi_pct is not None else None,
            "Store URL": it.url
        })
    df = pd.DataFrame(rows)
    # Sort by ROI desc if present
    if "ROI % (est)" in df.columns and df["ROI % (est)"].notna().any():
        df = df.sort_values(by="ROI % (est)", ascending=False, na_position="last")
    return df
