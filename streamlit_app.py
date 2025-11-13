
import os
import sys
import time
import platform
import importlib
import traceback
import streamlit as st

st.set_page_config(page_title="PSA Cert Arbitrage Finder — CardsHQ", layout="wide")

# -------------------- UI shell renders first --------------------
st.title("🧾 PSA Cert Arbitrage Finder — CardsHQ Categories")
st.caption("If this page ever shows white/blank, toggle Safe Mode below and check the Diagnostics section for import errors.")

# Simple in-app logger
if "log_lines" not in st.session_state:
    st.session_state.log_lines = []

def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    st.session_state.log_lines.append(f"[{ts}] {msg}")

def clear_log():
    st.session_state.log_lines = []

def render_log():
    log_text = "\n".join(st.session_state.log_lines[-800:]) or "(log is empty)"
    st.code(log_text, language="text")

with st.expander("Frontend Self-Test", expanded=True):
    st.write("✅ Streamlit renderer loaded. If you can read this, the front-end is fine.")
    st.write("Use **Diagnostics** below to surface backend import errors if any.")

with st.expander("Diagnostics"):
    cols = st.columns(4)
    cols[0].metric("Python", sys.version.split()[0])
    cols[1].metric("Platform", platform.platform().split("-")[0])
    cols[2].metric("Proc", platform.processor() or "n/a")
    cols[3].metric("PID", str(os.getpid()))
    st.write("`sys.path` head:", sys.path[:3])

# Safe Mode + Network options
with st.expander("Settings", expanded=True):
    colA, colB = st.columns([2, 1])
    with colA:
        safe_mode = st.toggle(
            "Safe Mode (fallback HTML parser, defer heavy imports)",
            value=True,
            help="Use built-in HTML parser instead of lxml. Helps avoid blank page if lxml isn't installed."
        )
        st.session_state["SAFE_MODE"] = safe_mode
    with colB:
        show_stack = st.checkbox("Show full tracebacks on error", value=True)

# -------------------- Lazy import of scraper module --------------------
# Import AFTER shell so errors show up as widgets, not blank page.
scraper = None
import_error = None
trace = ""

try:
    # We import inside the try so any ImportError shows as a Streamlit error box.
    # If you flipped parser to 'html.parser' in Safe Mode, the module will read the env flag.
    if safe_mode:
        os.environ["BS_PARSER"] = "html.parser"  # module reads this to pick parser
    else:
        os.environ.pop("BS_PARSER", None)

    scraper = importlib.import_module("cards_cert_arbitrage")
except Exception as e:
    import_error = e
    trace = traceback.format_exc()

if import_error:
    st.error(f"Import error while loading `cards_cert_arbitrage`: {type(import_error).__name__}: {import_error}")
    if show_stack:
        st.exception(import_error)
    st.stop()

# -------------------- App UI (uses imported module) --------------------
from cards_cert_arbitrage import (
    scan_selected_categories,
    CARDSHQ_CATEGORY_URLS,
    test_psa_cert,
)

st.markdown(
    "Scans your chosen **CardsHQ** categories, opens each product, extracts "
    "**Card Name, Price, PSA Grade, PSA Cert**, then fetches PSA **Sales History** and the **PSA Estimate** "
    "from the cert page, and estimates ROI after fees & outbound shipping."
)

with st.expander("Network settings", expanded=True):
    net_mode = st.radio(
        "Network mode (CardsHQ + PSA):",
        options=["Auto (default)", "Direct only", "Force proxy"],
        index=0,
        horizontal=True
    )
    if net_mode == "Auto (default)":
        force_proxy = None
    elif net_mode == "Direct only":
        force_proxy = False
    else:
        force_proxy = True

    insecure_tls = st.checkbox(
        "Unsafe: disable TLS verification for PSA & CardsHQ",
        value=False,
        help="Only use if you hit SSL certificate errors. Not recommended for long-term use."
    )

with st.expander("PSA parsing options", expanded=True):
    use_playwright_apr = st.toggle(
        "Use Playwright fallback for 'Auction Prices by Grade'",
        value=True,
        help="If the APR table doesn't parse via requests/BeautifulSoup, try a headless Chromium fetch to extract the table."
    )

with st.expander("Scan settings", expanded=True):
    left, right = st.columns([2, 1])
    with left:
        chosen = st.multiselect(
            "Categories to scan",
            options=list(CARDSHQ_CATEGORY_URLS.keys()),
            default=list(CARDSHQ_CATEGORY_URLS.keys())
        )
        limit = st.number_input(
            "Max PSA-cert listings per category (0 = no cap)",
            min_value=0, max_value=1000, value=0, step=25
        )
    with right:
        fee_rate = st.number_input(
            "Selling fee rate (e.g., eBay 13% = 0.13)",
            min_value=0.0, max_value=0.30, value=0.13, step=0.01, format="%.2f"
        )
        ship_out = st.number_input(
            "Outbound shipping/fulfillment cost ($)",
            min_value=0.0, max_value=50.0, value=5.0, step=0.5
        )

# Controls
c_run, c_clear = st.columns([1, 1])
run = c_run.button("Run category scan")
c_clear.button("🧹 Clear Log", on_click=clear_log)

st.caption("⚠️ Please respect each website’s Terms and robots.txt. For personal research.")

@st.cache_data(show_spinner=False, ttl=60*20)
def _run(categories, limit, fee_rate, ship_out, force_proxy, insecure_tls, use_playwright_apr):
    lim = None if limit == 0 else int(limit)
    return scan_selected_categories(
        categories=categories,
        limit_per_category=lim,
        fee_rate=float(fee_rate),
        ship_out=float(ship_out),
        force_proxy=force_proxy,
        verify_tls=not insecure_tls,
        use_playwright_apr=use_playwright_apr,
        logger=log
    )

if run:
    if not chosen:
        st.warning("Pick at least one category to scan.")
    else:
        clear_log()
        log(f"START scan | mode={'Auto' if force_proxy is None else 'Direct' if force_proxy is False else 'Proxy'} | TLS={'ON' if not insecure_tls else 'OFF'} | playwright={'ON' if use_playwright_apr else 'OFF'}")
        with st.spinner("Scanning categories, reading PSA Estimate, and fetching APR…"):
            try:
                df = _run(chosen, limit, fee_rate, ship_out, force_proxy, insecure_tls, use_playwright_apr)
            except Exception as e:
                log(f"ERROR: {e.__class__.__name__}: {e}")
                if show_stack:
                    st.exception(e)
                else:
                    st.error(str(e))
                st.stop()
        log("Scan finished.")

        st.subheader("Results")
        if df.empty:
            st.error("No PSA-cert listings found in the scanned categories.")
        else:
            total_rows = int(df.shape[0])
            pos_count = int(df["ROI % (est)"].fillna(-999).gt(0).sum())
            c1, c2, c3 = st.columns(3)
            c1.metric("Results", f"{total_rows:,}")
            c2.metric("Positive ROI (est)", f"{pos_count:,}")
            c3.metric("Categories scanned", f"{len(chosen)}")

            st.dataframe(df, use_container_width=True, hide_index=True)

            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "Download CSV",
                data=csv,
                file_name="psa_arbitrage_results.csv",
                mime="text/csv"
            )

# -------- Log Pane --------
st.divider()
st.subheader("📜 Log Pane")
render_log()

# -------- Quick PSA Cert Test --------
st.divider()
st.header("🔎 Quick PSA Cert Test")

with st.form("psa_test_form", clear_on_submit=False):
    colA, colB = st.columns([2, 1])
    cert = colA.text_input("PSA Certification Number", placeholder="e.g., 92911899")
    grade_opt = colB.text_input("Grade (optional)", placeholder="e.g., 10")
    submitted = st.form_submit_button("Test cert")

if submitted:
    clear_log()
    if not cert or not cert.isdigit():
        st.warning("Enter a numeric PSA certification number.")
    else:
        try:
            grade_num = int(float(grade_opt)) if grade_opt.strip() else None
        except ValueError:
            st.warning("Grade must be a number (e.g., 9 or 10). Ignoring grade filter.")
            grade_num = None

        log(f"TEST cert {cert} | mode={'Auto' if force_proxy is None else 'Direct' if force_proxy is False else 'Proxy'} | TLS={'ON' if not insecure_tls else 'OFF'} | grade={grade_num or '—'} | playwright={'ON' if use_playwright_apr else 'OFF'}")
        with st.spinner("Fetching PSA Estimate & Sales History…"):
            try:
                data = test_psa_cert(
                    cert.strip(),
                    grade_num=grade_num,
                    force_proxy=force_proxy,
                    verify_tls=not insecure_tls,
                    use_playwright_apr=use_playwright_apr,
                    logger=log
                )
                st.write(f"**Cert:** {cert}")
                if data.get("PSA Cert URL"):
                    st.write(f"[PSA Cert Page]({data['PSA Cert URL']})")
                if data.get("PSA APR URL"):
                    st.write(f"[PSA Sales History]({data['PSA APR URL']})")

                c1, c2, c3, c4 = st.columns(4)
                c1.metric("PSA Estimate", f"${data['PSA Estimate (cert page)']:,}" if data["PSA Estimate (cert page)"] else "—")
                c2.metric("Most Recent (grade)", f"${data['APR Most Recent (Grade)']:,}" if data["APR Most Recent (Grade)"] else "—")
                c3.metric("Median Recent (all)", f"${data['APR Median Recent (All)']:,}" if data["APR Median Recent (All)"] else "—")
                c4.metric("Chosen Value", f"${data['Chosen Value']:,}" if data["Chosen Value"] else "—")
            except Exception as e:
                log(f"ERROR: {e.__class__.__name__}: {e}")
                if show_stack:
                    st.exception(e)
                else:
                    st.error("PSA request failed.")
