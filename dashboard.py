import streamlit as st
import pandas as pd
import plotly.graph_objects as go

# ==========================================================
# 1. IMPORT UPDATERS AND SCANNERS
# ==========================================================
from Updater import load_all_market_data        # capital U
from updater_hourly import load_all_hourly_data
from updater_daily import load_all_daily_data

import scanner
import scanner_hourly
import scanner_daily

# ==========================================================
# 2. STREAMLIT CONFIG
# ==========================================================
st.set_page_config(page_title="Trading Dashboard", layout="wide")
st.title("📊 Trading Dashboard")

# ==========================================================
# 3. REFRESH DATA
# ==========================================================
if st.sidebar.button("🔄 Refresh Data", key="refresh_button"):
    st.session_state["market_data"] = load_all_market_data()
    st.session_state["hourly_data"] = load_all_hourly_data()
    st.session_state["daily_data"] = load_all_daily_data()
    st.success("Data refreshed!")

# ==========================================================
# 4. RUN SCANNERS
# ==========================================================
def run_scanners():
    results = {}

    # Weekly scanner
    if "market_data" in st.session_state:
        df = st.session_state["market_data"]
        signals = scanner.scan_weekly(df)
        valid = signals[signals["Signal"] == "VALID"]

        if not valid.empty:
            valid = valid.rename(columns={"Latest Price": "Current Price",
                                          "Retr Low": "Retrace Low"})
            # Add stock name lookup (Ticker → Company Name)
            # Example mapping; replace with your own dictionary or data source
            ticker_to_name = {
                "AAPL": "Apple Inc.",
                "MSFT": "Microsoft Corporation",
                "GOOGL": "Alphabet Inc.",
                # add more tickers here...
            }
            valid["Stock Name"] = valid["Ticker"].map(ticker_to_name).fillna("Unknown")

            # Reorder columns
            valid = valid[["Ticker", "Stock Name", "Current Price",
                           "Swing High", "Swing Low", "Fib618", "Fib786", "Retrace Low"]]

        results["Weekly"] = valid

    # Hourly scanner
    if "hourly_data" in st.session_state:
        df = st.session_state["hourly_data"]
        signals = []
        for ticker, g in df.groupby("Ticker"):
            sig = scanner_hourly.detect_swing_and_retrace(g)
            if sig and sig["Signal"] == "VALID":
                sig["Ticker"] = ticker
                signals.append(sig)
        results["Hourly"] = pd.DataFrame(signals)

    # Daily scanner
    if "daily_data" in st.session_state:
        df = st.session_state["daily_data"]
        signals = []
        for ticker, g in df.groupby("Ticker"):
            sig = scanner_daily.detect_swing_and_retrace(g)
            if sig and sig["Signal"] == "VALID":
                sig["Ticker"] = ticker
                signals.append(sig)
        results["Daily"] = pd.DataFrame(signals)

    return results

if st.sidebar.button("📈 Run Scanners", key="scanner_button"):
    st.session_state["signals"] = run_scanners()
    st.success("Scanners executed!")

# ==========================================================
# 5. DISPLAY RESULTS
# ==========================================================
if "signals" in st.session_state:
    for category, df in st.session_state["signals"].items():
        st.subheader(f"{category} VALID Signals")
        if df.empty:
            st.info(f"No VALID signals found for {category}.")
            continue

        st.dataframe(df)

        # Allow user to click a ticker
        tickers = df["Ticker"].tolist()
        selected = st.selectbox(
            f"Select {category} ticker",
            tickers,
            key=f"{category}_select"
        )
        if selected:
            # Get data for selected ticker
            if category == "Weekly":
                g = st.session_state["market_data"].query("Ticker == @selected")
                x_axis = g["Date"]
            elif category == "Daily":
                g = st.session_state["daily_data"].query("Ticker == @selected")
                x_axis = g["Date"]
            else:  # Hourly
                g = st.session_state["hourly_data"].query("Ticker == @selected")
                x_axis = g["Datetime"]

            # Plot candlestick chart
            fig = go.Figure(data=[go.Candlestick(
                x=x_axis,
                open=g["Open"], high=g["High"],
                low=g["Low"], close=g["Close"]
            )])

            # Overlay Fib levels (from signal dict)
            sig = df[df["Ticker"] == selected].iloc[0]
            fig.add_hline(y=sig["Swing High"], line_color="green", annotation_text="Swing High")
            fig.add_hline(y=sig["Swing Low"], line_color="red", annotation_text="Swing Low")
            fig.add_hline(y=sig["Fib618"], line_color="blue", annotation_text="0.618")
            fig.add_hline(y=sig["Fib786"], line_color="purple", annotation_text="0.786")

            st.plotly_chart(fig, use_container_width=True)
