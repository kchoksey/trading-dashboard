import pandas as pd
import numpy as np
from updater_daily import load_all_daily_data  # import daily data loader

def detect_swing_and_retrace(df, lookback_days=250):
    """
    Identify latest swing high, swing low, retrace into 0.618–0.786 zone,
    and check if current price is within +3% of retrace low.
    """
    df = df.sort_values("Date").reset_index(drop=True)

    lows = df["Low"].to_numpy(dtype=float)
    highs = df["High"].to_numpy(dtype=float)
    closes = df["Close"].to_numpy(dtype=float)

    if len(lows) < lookback_days:
        return None

    # Step 1: find recent swing low
    recent_low_idx = np.argmin(lows[-lookback_days:])
    recent_low_price = lows[-lookback_days:][recent_low_idx]
    recent_low_pos = len(lows) - lookback_days + recent_low_idx

    # Step 2: find swing high after that low
    window_highs = highs[recent_low_pos:]
    if len(window_highs) < 5:
        return None
    swing_high_idx = np.argmax(window_highs)
    swing_high_price = window_highs[swing_high_idx]
    swing_high_pos = recent_low_pos + swing_high_idx

    # Step 3: Fibonacci retracement levels
    fib618 = swing_high_price - 0.618 * (swing_high_price - recent_low_price)
    fib786 = swing_high_price - 0.786 * (swing_high_price - recent_low_price)

    # Step 4: retracement low after swing high
    retr_segment = lows[swing_high_pos:]
    if len(retr_segment) == 0:
        return None
    retr_low = retr_segment.min()
    retr_low_pos = swing_high_pos + np.argmin(retr_segment)

    retr_in_zone = fib786 <= retr_low <= fib618

    # Step 5: current price check
    current_price = closes[-1]
    within_3pct = current_price <= retr_low * 1.03

    if retr_in_zone and within_3pct:
        return {
            "Swing Low": recent_low_price,
            "Swing High": swing_high_price,
            "Fib618": fib618,
            "Fib786": fib786,
            "Retr Low": retr_low,
            "Current Price": current_price,
            "Signal": "VALID"
        }
    else:
        return {
            "Swing Low": recent_low_price,
            "Swing High": swing_high_price,
            "Fib618": fib618,
            "Fib786": fib786,
            "Retr Low": retr_low if retr_segment.size > 0 else None,
            "Current Price": current_price,
            "Signal": "INVALID"
        }

if __name__ == "__main__":
    # Load daily data from updater_daily.py
    df = load_all_daily_data()

    results = []
    for ticker, g in df.groupby("Ticker"):
        signal = detect_swing_and_retrace(g)
        if signal:
            signal["Ticker"] = ticker
            results.append(signal)

    out = pd.DataFrame(results)

    # ✅ Only keep VALID signals
    valid = out[out["Signal"] == "VALID"]

    print(valid)
    valid.to_excel("daily_signals.xlsx", index=False)
    print(f"Exported {len(valid)} VALID signals to daily_signals.xlsx")
