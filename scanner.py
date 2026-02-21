import pandas as pd
import numpy as np
from Updater import load_all_market_data   # note the capital U

# ==========================================================
# 1. RESAMPLE TO WEEKLY
# ==========================================================
def resample_weekly(df):
    df = df.set_index("Date")
    out = []
    for ticker, g in df.groupby("Ticker"):
        wk = g.resample("W-FRI").agg({
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Adj Close": "last",
            "Volume": "sum"
        }).dropna()
        wk["Ticker"] = ticker
        out.append(wk.reset_index())
    return pd.concat(out, ignore_index=True)

# ==========================================================
# 2. SWING DETECTION
# ==========================================================
def find_swing(group, lookback_weeks=80):
    window = group.tail(lookback_weeks)
    if len(window) < 10:
        return None
    highs = window["High"].values
    dates = window["Date"].values
    look = 3
    pivots = []
    for i in range(look, len(highs) - look):
        if highs[i] == max(highs[i - look : i + look + 1]):
            pivots.append(i)
    if not pivots:
        return None
    best_rel_idx = max(pivots, key=lambda idx: highs[idx])
    swing_high_price = float(highs[best_rel_idx])
    swing_high_date = pd.to_datetime(dates[best_rel_idx])
    prior_segment = window.iloc[: best_rel_idx + 1]
    low_idx = prior_segment["Low"].idxmin()
    swing_low_price = float(group.loc[low_idx, "Low"])
    swing_low_date = pd.to_datetime(group.loc[low_idx, "Date"])
    if swing_low_price >= swing_high_price:
        return None
    return {
        "Swing Low Date": swing_low_date,
        "Swing Low Price": swing_low_price,
        "Swing High Date": swing_high_date,
        "Swing High Price": swing_high_price,
    }

# ==========================================================
# 3. WEEKLY SCANNER
# ==========================================================
def scan_weekly(df, lookback_weeks=80):
    results = []
    weekly = resample_weekly(df)
    for ticker, g in weekly.groupby("Ticker"):
        g = g.sort_values("Date").copy()
        latest_price = g["Close"].iloc[-1]
        latest_date = g["Date"].iloc[-1]
        swing = find_swing(g, lookback_weeks)
        if swing is None:
            continue
        swing_range = swing["Swing High Price"] - swing["Swing Low Price"]
        fib618 = swing["Swing High Price"] - 0.618 * swing_range
        fib786 = swing["Swing High Price"] - 0.786 * swing_range
        correction = g[(g["Date"] > swing["Swing High Date"]) & (g["Date"] <= latest_date)]
        if correction.empty:
            continue
        retr_idx = correction["Low"].idxmin()
        retr_low_price = correction.loc[retr_idx, "Low"]
        retr_low_date = correction.loc[retr_idx, "Date"]

        # Condition 1: Fib zone
        retr_in_zone = (retr_low_price <= fib618) and (retr_low_price >= fib786)

        # Condition 2: retracement within last 8 weeks
        weeks_since_retr = (latest_date - retr_low_date).days / 7
        recent_hit = weeks_since_retr <= 8

        # Final signal (RSI divergence removed, charting removed)
        signal = "VALID" if retr_in_zone and recent_hit else "INVALID"

        results.append({
            "Ticker": ticker,
            "Latest Date": latest_date,
            "Latest Price": latest_price,
            "Swing Low": swing["Swing Low Price"],
            "Swing High": swing["Swing High Price"],
            "Fib618": fib618,
            "Fib786": fib786,
            "Retr Low": retr_low_price,
            "Retr Date": retr_low_date,
            "Weeks Since Retr": weeks_since_retr,
            "Recent Hit": recent_hit,
            "Signal": signal
        })
    return pd.DataFrame(results)

# ==========================================================
# 4. SELF-TEST (EXPORT ONLY)
# ==========================================================
if __name__ == "__main__":
    df = load_all_market_data()
    signals = scan_weekly(df)
    print("\nAll signals:")
    print(signals)
    valid = signals[signals["Signal"] == "VALID"]
    print(f"\nValid signals (Fib zone + recent hit): {len(valid)} found")
    print(valid)
    valid.to_excel("valid_signals.xlsx", index=False)
    print("Exported valid signals to valid_signals.xlsx")