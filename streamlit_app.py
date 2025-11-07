import streamlit as st
import pandas as pd
from cards_cert_arbitrage import find_deals_cardshq

st.set_page_config(page_title="PSA Cert Arbitrage Finder", layout="wide")

st.title("🧾 PSA Cert Arbitrage Finder")
st.markdown(
    "Find potentially underpriced PSA-graded cards by scraping shop listings that publish PSA **cert** numbers, "
    "then comparing to PSA **Auction Prices Realized** (Most Recent by Grade)."
)

with st.expander("Settings", expanded=True):
    col1, col2, col3 = st.columns(3)
    limit = col1.number_input("Max listings to evaluate", 10, 400, 60, 10)
    fee_rate = col2.number_input("Selling fee rate (e.g., eBay 13% = 0.13)", 0.00, 0.30, 0.13, 0.01, format="%.2f")
    ship_out = col3.number_input("Your shipping/fulfillment cost ($)", 0.0, 30.0, 5.0, 0.5)

run = st.button("Run scan (CardsHQ)")
st.caption("⚠️ Please respect site ToS and robots.txt. Throttling is applied.")

@st.cache_data(show_spinner=False, ttl=60*30)
def _run(limit, fee_rate, ship_out):
    return find_deals_cardshq(limit=int(limit), fee_rate=float(fee_rate), ship_out=float(ship_out))

if run:
    with st.spinner("Scanning CardsHQ and fetching PSA APR…"):
        df = _run(limit, fee_rate, ship_out)

    if df.empty:
        st.warning("No PSA-cert listings found in the scanned range.")
    else:
        # Show KPIs
        found = int(df.shape[0])
        winners = int(df["ROI % (est)"].fillna(-999).gt(0).sum())
        colA, colB = st.columns(2)
        colA.metric("Listings evaluated", found)
        colB.metric("Positive ROI% (est)", winners)

        # Table
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True
        )

        # Download
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("Download CSV", data=csv, file_name="psa_arbitrage_results.csv", mime="text/csv")

st.divider()
st.markdown(
    """
**Notes**
- Current adapter targets **cardshq.com** (Shopify) because it prints **PSA Certification #** and Grade in-page.
- Add more adapters (e.g., Burbank) by writing a parser that extracts **name, price, PSA grade, PSA cert** per product.
- PSA APR uses the **Most Recent Price** for the exact grade when available.
- ROI = (APR * (1 - fee) - ship_out - ask) / ask. Tweak fee/ship above.
    """
)
