# streamlit_app.py
import os
import math
import pandas as pd
import streamlit as st

from cards_cert_arbitrage import (
    CARD_SHOP_CATEGORIES,
    scan_selected_categories,
    test_psa_cert,
)

st.set_page_config(page_title="Cards Arbitrage (PSA Cert Scan)", layout="wide")

# -----------------------------
# Sidebar controls
# -----------------------------
st.sidebar.header("Scan Settings")
categories = st.sidebar.multiselect(
    "Categories to scan",
    list(CARD_SHOP_CATEGORIES.keys()),
    default=list(CARD_SHOP_CATEGORIES.keys()),
)
limit = st.sidebar.number_input("Limit per category (PSA-cert listings)", 1, 100, 1, 1)
fee_rate = st.sidebar.slider("Selling fee rate (marketplace %)", 0.00, 0.25, 0.13, 0.01)
ship_out = st.sidebar.number_input("Outbound shipping (per sale)", 0.0, 50.0, 4.0, 0.5)

st.sidebar.markdown("---")
st.sidebar.subheader("Network")
force_proxy = st.sidebar.selectbox(
    "Network mode",
    options=["Auto", "Direct", "Proxy (placeholder)"],
    index=0,
    help="We don't actually route a proxy in this demo, but the flag is wired throughout.",
)
force_proxy_val = None if force_proxy == "Auto" else (False if force_proxy == "Direct" else True)

insecure_tls = st.sidebar.checkbox(
    "Unsafe: disable TLS verification (global)",
    value=False,
    help="For debugging odd SSL chains. Safer fix is built-in PSA-only fallback. Leave OFF if possible.",
)

st.sidebar.markdown("---")
tab = st.sidebar.radio("Mode", ["Scanner", "PSA Debug"])

# -----------------------------
# Log pane
# -----------------------------
log_placeholder = st.empty()
log_lines = []

def log(msg: str):
    log_lines.append(msg)
    # Keep last ~400 lines
    if len(log_lines) > 400:
        del log_lines[: len(log_lines) - 400]
    with log_placeholder.container():
        st.code("\n".join(log_lines), language="text")

# -----------------------------
# Scanner tab
# -----------------------------
if tab == "Scanner":
    st.title("Card Store → PSA Cert Arbitrage Scanner")
    st.caption("Scans CardsHQ category pages, extracts PSA cert listings, fetches PSA comps from psacard.com, and computes quick margins.")

    run = st.button("Run scan", type="primary")
    if run:
        with st.spinner("Scanning…"):
            rows = scan_selected_categories(
                categories=categories,
                limit_per_category=int(limit),
                selling_fee_rate=float(fee_rate),
                outbound_shipping=float(ship_out),
                force_proxy=force_proxy_val,
                verify_tls=not insecure_tls,
                logger=log,
            )
        if not rows:
            st.warning("No rows found.")
        else:
            df = pd.DataFrame(rows)
            # nicer formatting
            money_cols = ["Store Price", "PSA Estimate", "APR Most Recent (Grade)", "APR Average (Grade)", "Chosen Comp", "Comp Net (after fees+ship)", "Est. Profit"]
            pct_cols = ["Margin %"]
            for c in money_cols:
                if c in df.columns:
                    df[c] = df[c].map(lambda x: None if pd.isna(x) else x)
            for c in pct_cols:
                if c in df.columns:
                    df[c] = df[c].map(lambda x: None if pd.isna(x) else x)

            st.subheader("Results")
            st.dataframe(df, use_container_width=True)

            # Quick filters: profitable only
            with st.expander("Show only profitable rows"):
                if "Est. Profit" in df.columns:
                    prof = df[df["Est. Profit"].fillna(-1) > 0].copy()
                    st.dataframe(prof, use_container_width=True)

# -----------------------------
# PSA Debug tab
# -----------------------------
if tab == "PSA Debug":
    st.title("PSA Cert Debugger")
    cert = st.text_input("PSA Certification Number", value="96174719")
    grade = st.text_input("Grade (optional, e.g., 10)", value="10")
    try:
        grade_num = float(grade) if grade.strip() else None
    except Exception:
        grade_num = None

    force_psa_insecure = st.checkbox("Force globally-insecure TLS for this debug call", value=False)
    go = st.button("Test this cert", type="primary")

    if go and cert.strip():
        with st.spinner("Fetching PSA cert & APR…"):
            data = test_psa_cert(
                cert.strip(),
                grade_num=grade_num,
                force_proxy=force_proxy_val,
                verify_tls=not (insecure_tls or force_psa_insecure),
                logger=log,
            )
        st.subheader("Parsed")
        st.json(data, expanded=False)
