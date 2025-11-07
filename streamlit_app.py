
import os
import streamlit as st
import pandas as pd

import cards_cert_arbitrage as arb

st.set_page_config(page_title=\"Cards Cert Arbitrage\", layout=\"wide\")

st.title(\"Cards Cert Arbitrage — PSA Path A (TLS-hardened)\" )
st.caption(\"Scans CardsHQ, extracts PSA certs, fetches PSA Estimate with PSA-scoped TLS fallback.\")

with st.sidebar:
    st.header(\"Settings\")
    category = st.selectbox(\"Category\", list(arb.CARDSHQ_CATEGORY_URLS.keys()), index=0)
    limit = st.number_input(\"Limit per category\", min_value=1, max_value=200, value=1, step=1)
    fee_rate = st.number_input(\"Marketplace fee rate (e.g., 0.13 for 13%)\", min_value=0.0, max_value=0.25, value=0.13, step=0.01)
    ship_out = st.number_input(\"Estimated outbound shipping ($)\", min_value=0.0, max_value=25.0, value=4.50, step=0.5)
    parser = st.selectbox(\"HTML parser\", [\"lxml\", \"html.parser\"], index=1)
    tls_verify = st.toggle(\"Verify TLS (recommended)\", value=True, help=\"Path A hardens TLS for PSA and will do a single unsafe retry only for PSA if needed.\")
    st.caption(\"Proxy keys (optional): set SCRAPERAPI_KEY or ZENROWS_KEY in env for fallback. Path A does not require them.\")

    st.divider()
    st.header(\"Quick PSA Cert Test\")
    cert_input = st.text_input(\"PSA cert number\", value=\"96174719\")
    run_quick = st.button(\"Run Quick Test\", type=\"primary\")

log_lines = []

def log(msg: str):
    log_lines.append(msg)
    st.session_state.setdefault(\"_log\", [])
    st.session_state[\"_log\"].append(msg)

with st.expander(\"📜 Log Pane\", expanded=True):
    log_box = st.empty()
    if \"_log\" not in st.session_state:
        st.session_state[\"_log\"] = []

col1, col2 = st.columns([2,1])

with col1:
    if st.button(\"Start Scan\", type=\"primary\"):
        log(f\"START scan | mode=Auto | TLS={'ON' if tls_verify else 'OFF'} | parser={parser}\")
        df = arb.scan_category(
            category_name=category,
            limit_per_category=int(limit),
            fee_rate=float(fee_rate),
            ship_out=float(ship_out),
            force_proxy=None,
            verify_tls=bool(tls_verify),
            parser_override=parser,
            logger=log,
        )
        st.dataframe(df, use_container_width=True)
        log(f\"[done] total rows={len(df)}\")
        log(\"Scan finished.\")

with col2:
    if run_quick and cert_input.strip():
        log(f\"Quick test on cert={cert_input.strip()} | TLS={'ON' if tls_verify else 'OFF'}\")
        res = arb.quick_psa_cert_test(cert_input.strip(), verify_tls=bool(tls_verify), logger=log)
        st.write(res)

with st.expander(\"📜 Log Pane (live)\", expanded=True):
    st.code(\"\\n\".join(st.session_state.get(\"_log\", [])) or \"(no logs yet)\")
