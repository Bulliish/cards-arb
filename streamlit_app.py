import streamlit as st
from cards_cert_arbitrage import (
    scan_selected_categories,
    CARDSHQ_CATEGORY_URLS,
    test_psa_cert,
)

st.set_page_config(page_title="PSA Cert Arbitrage Finder — CardsHQ", layout="wide")

st.title("🧾 PSA Cert Arbitrage Finder — CardsHQ Categories")
st.markdown(
    "Scans your chosen **CardsHQ** categories, opens each product, extracts "
    "**Card Name, Price, PSA Grade, PSA Cert**, then fetches PSA **Sales History** for that cert/grade "
    "and estimates ROI after fees & outbound shipping."
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

run = st.button("Run category scan")
st.caption("⚠️ Please respect each website’s Terms and robots.txt. For personal research.")

@st.cache_data(show_spinner=False, ttl=60*20)
def _run(categories, limit, fee_rate, ship_out, force_proxy, insecure_tls):
    lim = None if limit == 0 else int(limit)
    return scan_selected_categories(
        categories=categories,
        limit_per_category=lim,
        fee_rate=float(fee_rate),
        ship_out=float(ship_out),
        force_proxy=force_proxy,
        verify_tls=not insecure_tls
    )

if run:
    if not chosen:
        st.warning("Pick at least one category to scan.")
    else:
        with st.spinner("Scanning categories and fetching PSA APR…"):
            try:
                df = _run(chosen, limit, fee_rate, ship_out, force_proxy, insecure_tls)
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
st.header("🔎 Quick PSA Cert Test")

with st.form("psa_test_form", clear_on_submit=False):
    colA, colB, colC = st.columns([2, 1, 1])
    cert = colA.text_input("PSA Certification Number", placeholder="e.g., 92911899")
    grade_opt = colB.text_input("Grade (optional)", placeholder="e.g., 10")
    submitted = st.form_submit_button("Test cert")

if submitted:
    if not cert or not cert.isdigit():
        st.warning("Enter a numeric PSA certification number.")
    else:
        try:
            grade_num = int(grade_opt) if grade_opt.strip() else None
        except ValueError:
            st.warning("Grade must be a number (e.g., 9 or 10). Ignoring grade filter.")
            grade_num = None

        with st.spinner("Fetching PSA Sales History…"):
            try:
                data = test_psa_cert(
                    cert.strip(),
                    grade_num=grade_num,
                    force_proxy=force_proxy,
                    verify_tls=not insecure_tls
                )
                st.write(f"**Cert:** {cert}")
                if data.get("PSA Cert URL"):
                    st.write(f"[PSA Cert Page]({data['PSA Cert URL']})")
                if data.get("PSA APR URL"):
                    st.write(f"[PSA Sales History]({data['PSA APR URL']})")

                c1, c2, c3 = st.columns(3)
                c1.metric("Most Recent (grade)", f"${data['APR Most Recent (Grade)']:,}" if data["APR Most Recent (Grade)"] else "—")
                c2.metric("Median Recent (all)", f"${data['APR Median Recent (All)']:,}" if data["APR Median Recent (All)"] else "—")
                c3.metric("Chosen Value", f"${data['Chosen Value']:,}" if data["Chosen Value"] else "—")

                st.caption(
                    f"Mode: **{'Force proxy' if force_proxy is True else 'Direct only' if force_proxy is False else 'Auto'}**, "
                    f"TLS verify: **{'OFF (unsafe)' if insecure_tls else 'ON'}**"
                )
            except Exception as e:
                st.error("PSA request failed.")
                st.exception(e)

st.divider()
st.markdown(
    """
**Tips if you still see SSL errors**
- Try **Force proxy** mode and set a key in Streamlit Secrets (`SCRAPERAPI_KEY` or `ZENROWS_KEY`).
- Toggle **Unsafe: disable TLS verification** (last resort; turn it off once you confirm connectivity).
- Some hosted environments intermittently fail on `www.psacard.com` — this app auto-retries `psacard.com`.
"""
)
