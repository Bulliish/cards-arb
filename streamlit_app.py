import os
import streamlit as st
from cards_cert_arbitrage import scan_selected_categories, CARDSHQ_CATEGORY_URLS

st.set_page_config(page_title="PSA Cert Arbitrage Finder — CardsHQ", layout="wide")

st.title("🧾 PSA Cert Arbitrage Finder — CardsHQ Categories")

st.markdown(
    "This scans your chosen **CardsHQ** categories, opens each product, extracts "
    "**Card Name, Price, PSA Grade, PSA Cert**, then fetches PSA **Sales History** for that cert/grade "
    "and estimates ROI after fees & outbound shipping."
)

st.info(
    "If you see SSL/handshake errors to PSA on Streamlit Cloud, set a proxy in **Secrets**:\n\n"
    "- `SCRAPERAPI_KEY: your_key_here`  (ScraperAPI)\n"
    "- or `ZENROWS_KEY: your_key_here` (ZenRows)\n\n"
    "The app will automatically retry PSA requests through the proxy when needed."
)

with st.expander("Settings", expanded=True):
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

run = st.button("Run scan")
st.caption("⚠️ Respect each website’s Terms and robots.txt. This is for personal research.")

@st.cache_data(show_spinner=False, ttl=60*20)
def _run(categories, limit, fee_rate, ship_out):
    lim = None if limit == 0 else int(limit)
    return scan_selected_categories(
        categories=categories,
        limit_per_category=lim,
        fee_rate=float(fee_rate),
        ship_out=float(ship_out)
    )

if run:
    if not chosen:
        st.warning("Pick at least one category to scan.")
    else:
        with st.spinner("Scanning categories and fetching PSA APR…"):
            try:
                df = _run(chosen, limit, fee_rate, ship_out)
            except Exception as e:
                st.exception(e)
                st.stop()

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

st.divider()
st.markdown(
    """
**How proxy fallback works**
- The app first tries **direct HTTPS** with a hardened TLS adapter and retries.
- If it hits an **SSLError** to PSA and you’ve set `SCRAPERAPI_KEY` or `ZENROWS_KEY` in Streamlit **Secrets**, it automatically retries through that provider.
- No proxy keys? It will stay on direct mode and surface the SSL error (good for debugging).
"""
)
