import streamlit as st
from cards_cert_arbitrage import scan_selected_categories, CARDSHQ_CATEGORY_URLS

st.set_page_config(page_title="PSA Cert Arbitrage Finder", layout="wide")

st.title("🧾 PSA Cert Arbitrage Finder — CardsHQ Categories")
st.markdown(
    "Scans the **exact categories** you provided on CardsHQ, opens each product page, "
    "extracts **Card Name, Price, PSA Grade, PSA Cert**, looks up the **PSA Sales History** "
    "price for that grade, and estimates ROI."
)

st.info(
    "Heads up: Scraping is throttled to be polite (default ~1.25s/request). "
    "Large scans can take a bit depending on inventory size."
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
            "Selling fee rate (eBay/marketplace) — e.g. 0.13 = 13%",
            min_value=0.0, max_value=0.30, value=0.13, step=0.01, format="%.2f"
        )
        ship_out = st.number_input(
            "Your outbound shipping/fulfillment cost ($)",
            min_value=0.0, max_value=50.0, value=5.0, step=0.5
        )

run = st.button("Run scan")
st.caption("⚠️ Please respect each website’s Terms and robots.txt. This tool is for your personal research.")

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
            df = _run(chosen, limit, fee_rate, ship_out)

        if df.empty:
            st.error("No PSA-cert listings found in the scanned categories.")
        else:
            # KPIs
            total_rows = int(df.shape[0])
            pos_count = int(df["ROI % (est)"].fillna(-999).gt(0).sum())
            c1, c2, c3 = st.columns(3)
            c1.metric("Results", f"{total_rows:,}")
            c2.metric("Positive ROI (est)", f"{pos_count:,}")
            c3.metric("Categories scanned", f"{len(chosen)}")

            # Display table
            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True
            )

            # Download
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
**Included categories (fixed):**
- Baseball — `/collections/baseball-cards`  
- Basketball (Graded) — `/collections/basketball-graded`  
- Football — `/collections/football-cards`  
- Soccer — `/collections/soccer-cards`  
- Pokemon — `/collections/pokemon-cards`  

**Notes**
- We paginate each category until no more product links are present.
- Product pages are parsed for **Certification #** and **Grade** (pattern matches handle common variations).
- PSA **Most Recent Price** for the scraped grade is used when available; otherwise we show a median of recent sale prices captured from the page.
- ROI = `(APR * (1 - fee_rate) - ship_out - ask) / ask`.
"""
)
