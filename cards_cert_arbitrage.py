# add near your imports if not already present
import certifi
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlparse, quote

PSA_HOSTS = {"www.psacard.com", "psacard.com"}

def _proxy_wrap(url: str) -> Optional[str]:
    # keep your existing implementation if you already have one
    if SCRAPERAPI_KEY:
        return f"https://api.scraperapi.com/?api_key={SCRAPERAPI_KEY}&keep_headers=true&url={quote(url, safe='')}"
    if ZENROWS_KEY:
        return f"https://api.zenrows.com/v1/?apikey={ZENROWS_KEY}&url={quote(url, safe='')}"
    return None

def _fetch(
    url: str,
    *,
    method: str = "GET",
    session: Optional[requests.Session] = None,
    timeout: int = 30,
    verify_tls: bool = True,
    allow_proxy_fallback: bool = True,
    force_proxy: Optional[bool] = None,
    logger: Optional[Callable[[str], None]] = None,
    **_ignored,  # swallow any other custom kwargs so they don't leak into requests
) -> requests.Response:
    """
    Fetch a URL with PSA-scoped TLS hardening.
    - Uses certifi CA when verify_tls=True
    - If PSA host TLS handshake fails, retries once with verify=False (public HTML only)
    - Optionally falls back to proxy if keys are configured
    """
    s = session or requests.Session()
    # polite retries for transient issues
    retries = Retry(total=2, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))

    mode = "auto"
    if force_proxy is True:
        mode = "proxy"
    elif force_proxy is False:
        mode = "direct"

    if logger:
        logger(f"GET {url}  | mode={mode}  tls_verify={'ON' if verify_tls else 'OFF'}")

    host = (urlparse(url).hostname or "").lower()
    verify_param = certifi.where() if verify_tls else False

    # If proxy is forced, go straight to proxy
    if force_proxy is True:
        prox = _proxy_wrap(url)
        if not prox:
            raise RuntimeError("Proxy is forced but no SCRAPERAPI_KEY or ZENROWS_KEY configured.")
        r = s.request(method, prox, timeout=timeout, verify=verify_param)
        if logger: logger(f" → via proxy {('scraperapi' if SCRAPERAPI_KEY else 'zenrows')} status={r.status_code}")
        r.raise_for_status()
        return r

    # Try direct first
    try:
        r = s.request(method, url, timeout=timeout, verify=verify_param)
        if logger: logger(f" → direct status={r.status_code}")
        r.raise_for_status()
        return r
    except requests.exceptions.SSLError as e:
        if logger: logger(f" !! SSL error on direct: {e.__class__.__name__}")
        # PSA-only: one unsafe retry with verify=False
        if host in PSA_HOSTS and verify_tls:
            try:
                r = s.request(method, url, timeout=timeout, verify=False)
                if logger: logger("    PSA-scoped unsafe retry " + ("succeeded" if r.ok else "failed"))
                r.raise_for_status()
                return r
            except Exception as e2:
                if logger: logger(f"    PSA-scoped unsafe retry error: {type(e2).__name__}")
        # Optional proxy fallback if available
        if allow_proxy_fallback and force_proxy is None:
            prox = _proxy_wrap(url)
            if prox:
                if logger: logger("    TLS error; trying proxy host")
                r = s.request(method, prox, timeout=timeout, verify=verify_param)
                if logger: logger(f" → via proxy {('scraperapi' if SCRAPERAPI_KEY else 'zenrows')} status={r.status_code}")
                r.raise_for_status()
                return r
        raise
