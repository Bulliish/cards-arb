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
) -> requests.Response:
    """
    Robust fetch:
      - force_proxy=True  -> always use proxy (error if no key configured)
      - force_proxy=False -> always direct (no proxy)
      - force_proxy=None  -> try direct, on SSLError and if key exists -> proxy fallback
      - verify_tls=False  -> disable TLS verification (last resort; not recommended)
    """
    # Force proxy path
    if force_proxy is True:
        prox = _proxy_wrap(url)
        if not prox:
            raise RuntimeError("Proxy is forced but no SCRAPERAPI_KEY or ZENROWS_KEY configured.")
        r = _get(prox, verify_tls=verify_tls)  # proxy still uses TLS outward
        r.raise_for_status()
        return r

    # Direct path (with optional fallback)
    try:
        r = _get(url, verify_tls=verify_tls)
        r.raise_for_status()
        return r
    except requests.exceptions.SSLError:
        if allow_proxy_fallback and force_proxy is None:
            prox = _proxy_wrap(url)
            if prox:
                rp = _get(prox, verify_tls=verify_tls)
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
    return int(m.gr
