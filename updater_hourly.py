import pandas as pd
import requests
from io import StringIO
import yfinance as yf

# ==========================================================
# 1. UNIVERSE BUILDERS
# ==========================================================
def get_sp500_universe():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers, timeout=15)
    tables = pd.read_html(StringIO(r.text))

    df = None
    for t in tables:
        if "Symbol" in t.columns:
            df = t.copy()
            break
    if df is None:
        raise RuntimeError("Could not find S&P500 table on Wikipedia")

    df["Ticker"] = df["Symbol"].str.replace(".", "-", regex=False)
    df["Name"] = df["Security"]
    df["Sector"] = df["GICS Sector"]

    df = df.drop_duplicates(subset="Ticker").head(500)
    return df[["Ticker", "Name", "Sector"]]


def get_hsi_universe():
    url = "https://en.wikipedia.org/wiki/Hang_Seng_Index"
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers, timeout=15)
    tables = pd.read_html(StringIO(r.text))

    df = None
    for t in tables:
        cols = [str(c).lower() for c in t.columns]
        if any(x in cols for x in ["ticker", "constituent", "sub-index"]):
            df = t.copy()
            break
    if df is None:
        raise RuntimeError("Could not find HSI table on Wikipedia")

    df.columns = [str(c).lower() for c in df.columns]
    ticker_col = None
    for c in df.columns:
        if "sehk" in c or "ticker" in c or "code" in c:
            ticker_col = c
            break
    if ticker_col is None:
        raise RuntimeError("Could not find ticker column for HSI")

    df["Ticker"] = (
        df[ticker_col].astype(str).str.extract(r"(\d+)", expand=False)
        .astype(str).str.zfill(4) + ".HK"
    )
    if "name" in df.columns:
        name_col = "name"
    else:
        possible = [c for c in df.columns if c != ticker_col]
        name_col = possible[0]

    df["Name"] = df[name_col]
    df["Sector"] = df.get("sub-index", df.get("industry", None))

    df = df.head(50)
    return df[["Ticker", "Name", "Sector"]]


def get_eurostoxx50_universe():
    data = [
        ("ASML.AS", "ASML Holding", "Technology"),
        ("SAP.DE", "SAP SE", "Technology"),
        ("SIE.DE", "Siemens AG", "Industrial"),
        ("AIR.PA", "Airbus SE", "Industrial"),
        ("BNP.PA", "BNP Paribas", "Financials"),
        ("SAN.PA", "Sanofi", "Healthcare"),
        ("OR.PA", "L'Oréal", "Consumer Goods"),
        ("MC.PA", "LVMH Moët Hennessy Louis Vuitton", "Consumer Goods"),
        ("AI.PA", "Air Liquide", "Materials"),
        ("DG.PA", "Vinci", "Industrial"),
        ("ENEL.MI", "Enel SpA", "Utilities"),
        ("ISP.MI", "Intesa Sanpaolo", "Financials"),
        ("IBE.MC", "Iberdrola", "Utilities"),
        ("ITX.MC", "Inditex", "Consumer Goods"),
        ("BBVA.MC", "Banco Bilbao Vizcaya Argentaria", "Financials"),
        ("MBG.DE", "Mercedes-Benz Group", "Consumer Goods"),
        ("ALV.DE", "Allianz SE", "Financials"),
        ("BAS.DE", "BASF SE", "Materials"),
        ("BAYN.DE", "Bayer AG", "Healthcare"),
        ("DTE.DE", "Deutsche Telekom", "Telecom"),
        ("ADS.DE", "Adidas AG", "Consumer Goods"),
        ("MUV2.DE", "Munich Re", "Financials"),
        ("CRH.L", "CRH plc", "Materials"),
        ("UNA.AS", "Unilever PLC", "Consumer Goods"),
        ("SU.PA", "Schneider Electric", "Industrial"),
        ("ENGI.PA", "Engie", "Utilities"),
        ("GLE.PA", "Société Générale", "Financials"),
        ("ACA.PA", "Crédit Agricole", "Financials"),
        ("KER.PA", "Kering", "Consumer Goods"),
        ("STLAM.MI", "Stellantis", "Consumer Goods"),
        ("HO.PA", "Thales", "Industrial"),
        ("CS.PA", "AXA", "Financials"),
        ("ORA.PA", "Orange", "Telecom"),
        ("VIV.PA", "Vivendi", "Media"),
        ("EL.PA", "EssilorLuxottica", "Healthcare"),
        ("FER.MC", "Ferrovial", "Industrial"),
        ("MAP.MC", "Mapfre", "Financials"),
        ("SOLB.BR", "Solvay", "Materials"),
        ("UCG.MI", "UniCredit", "Financials"),
        ("IFX.DE", "Infineon Technologies", "Technology"),
        ("BMW.DE", "BMW AG", "Consumer Goods"),
        ("RMS.PA", "Hermès", "Consumer Goods"),
        ("MT.AS", "ArcelorMittal", "Materials"),
        ("VOW3.DE", "Volkswagen Group", "Consumer Goods"),
        ("PHIA.AS", "Philips", "Healthcare"),
        ("ABI.BR", "Anheuser-Busch InBev", "Consumer Goods"),
        ("ENI.MI", "ENI SpA", "Energy"),
        ("SGRE.MC", "Siemens Gamesa", "Industrial"),
        ("ADYEN.AS", "Adyen NV", "Technology"),
        ("AD.AS", "Ahold Delhaize", "Consumer Staples"),
    ]
    df = pd.DataFrame(data, columns=["Ticker", "Name", "Sector"])
    return df.head(50)

# ==========================================================
# 2. HOURLY DOWNLOADER (60 days, batched)
# ==========================================================
def download_hourly_prices(tickers, label, period="60d"):
    print(f"\nDownloading {label}: {len(tickers)} tickers")
    if not tickers:
        return []

    batch_size = 40
    frames = []
    failed = []

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        print(f"  Batch {i // batch_size + 1}: {len(batch)} tickers")
        try:
            data = yf.download(
                batch,
                period=period,
                interval="1h",
                group_by="ticker",
                auto_adjust=False,
                threads=True,
                progress=False,
            )
        except Exception as e:
            print(f"  ERROR downloading batch: {e}")
            failed.extend(batch)
            continue

        for t in batch:
            try:
                df_t = data[t].dropna().copy()
                df_t["Ticker"] = t
                df_t["Index"] = label
                frames.append(df_t.reset_index())
            except Exception as e:
                print(f"  Failed to parse {t}: {e}")
                failed.append(t)

    print(f"Completed {label}: {len(frames)} OK, {len(failed)} failed")
    return frames

# ==========================================================
# 3. MASTER FUNCTION
# ==========================================================
def load_all_hourly_data():
    print("Building universes...")

    sp500 = get_sp500_universe()
    hsi   = get_hsi_universe()
    euro  = get_eurostoxx50_universe()

    print("SP500:", len(sp500))
    print("HSI:  ", len(hsi))
    print("EuroStoxx50:", len(euro))

    sp = download_hourly_prices(sp500["Ticker"].tolist(), "SP500", period="60d")
    hs = download_hourly_prices(hsi["Ticker"].tolist(), "HSI",   period="60d")
    eu = download_hourly_prices(euro["Ticker"].tolist(), "EuroStoxx50", period="60d")

    if not (sp or hs or eu):
        raise RuntimeError("No hourly OHLC data downloaded from Yahoo")

    combined = pd.concat(sp + hs + eu, ignore_index=True)
    combined["Datetime"] = pd.to_datetime(combined["Datetime"])
    combined = combined.sort_values(["Ticker", "Datetime"]).reset_index(drop=True)

    print("\nFinal merged dataframe shape:", combined.shape)
    return combined

# ==========================================================
# 4. SELF-TEST
# ==========================================================
if __name__ == "__main__":
    df = load_all_hourly_data()
    print("\nPreview of merged hourly data:")
    print(df.head())

