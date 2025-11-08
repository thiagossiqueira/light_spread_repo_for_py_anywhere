import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import io
import base64
from datetime import datetime
from src.core.bootstrapping import LTN, NTNF, CurveBootstrap
from src.core.daycounts import DayCounts
from src.core.nss import NelsonSiegelSvensson

def load_sovereign_data():
    path = "datos_y_modelos/Domestic/domestic_sovereign_curve_brazil.xlsx"
    return pd.read_excel(path)

def generate_sovereign_surface_chart(df, ref_date: datetime):
    df = df.copy()
    df['MATURITY'] = pd.to_datetime(df['MATURITY'])

    ltn_df = df[df['papel'] == 'LTN'][['MATURITY', 'YAS_BOND_YLD']].dropna().sort_values('MATURITY')
    ntnf_df = df[df['papel'] == 'NTNF'][['MATURITY', 'YAS_BOND_YLD']].dropna().sort_values('MATURITY')

    ltn_expires = ltn_df['MATURITY'].dt.date.tolist()
    ntnf_expires = ntnf_df['MATURITY'].dt.date.tolist()

    ltn_yields = (ltn_df['YAS_BOND_YLD'].astype(float) / 100).tolist()
    ntnf_yields = (ntnf_df['YAS_BOND_YLD'].astype(float) / 100).tolist()

    ltn_prices, ltn_cash_flows = [], []
    for T, y in zip(ltn_expires, ltn_yields):
        bond = LTN(expiry=T, rate=y, ref_date=ref_date)
        ltn_prices.append(bond.price)
        ltn_cash_flows.append(pd.Series(index=[T], data=[bond.principal]))

    ntnf_prices, ntnf_cash_flows = [], []
    for T, y in zip(ntnf_expires, ntnf_yields):
        bond = NTNF(expiry=T, rate=y, ref_date=ref_date)
        ntnf_prices.append(bond.price)
        ntnf_cash_flows.append(bond.cash_flows)

    all_prices = ltn_prices + ntnf_prices
    all_cash_flows = ltn_cash_flows + ntnf_cash_flows

    cb = CurveBootstrap(prices=all_prices, cash_flows=all_cash_flows, ref_date=ref_date)
    nss = NelsonSiegelSvensson(prices=all_prices, cash_flows=all_cash_flows, ref_date=ref_date)

    x_dense = np.linspace(0.01, 12, 200)
    y_zero = [cb.rate_for_date(t) * 100 for t in x_dense]
    y_nss = [nss.rate_for_ytm(betas=nss.betas, ytm=t) * 100 for t in x_dense]

    plt.figure(figsize=(12, 7))
    plt.plot(x_dense, y_zero, label="Zero curve", lw=2)
    plt.plot(x_dense, y_nss, label="NSS", lw=2)
    plt.title(f"Soberana DI — {ref_date}", fontsize=16)
    plt.xlabel("Prazo (anos)")
    plt.ylabel("Yield (% a.a.)")
    plt.legend()
    plt.grid(True)

    buf = io.BytesIO()
    plt.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    image_base64 = base64.b64encode(buf.getvalue()).decode('utf-8')
    buf.close()
    plt.close()

    return image_base64
