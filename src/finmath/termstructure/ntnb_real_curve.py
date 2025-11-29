# src/finmath/termstructure/ntnb_real_curve.py

import numpy as np
import pandas as pd
from calendars.daycounts import DayCounts

from src.finmath.termstructure.curve_models import CurveBootstrap
from src.finmath.termstructure.combined_real_curve import CombinedRealCurve

# ANBIMA convention for curves
DAYCOUNT_BUS252 = DayCounts("bus/252", calendar="cdr_anbima")


def load_ntnb_metadata(govt_path: str) -> pd.DataFrame:
    """
    Lê metadados dos títulos públicos e filtra apenas NTN-B.
    Sheet: db_values_only, filtro: CALC_TYP_DES == 'BRAZIL I/L BOND'
    """
    df = pd.read_excel(govt_path, sheet_name="db_values_only")

    df = df[df["CALC_TYP_DES"] == "BRAZIL I/L BOND"].copy()

    df["MATURITY"] = pd.to_datetime(df["MATURITY"], errors="coerce")
    df = df.dropna(subset=["ID_ISIN", "MATURITY"])

    # Garantir colunas mínimas para cashflows
    for col in ["CPN", "CPN_FREQ", "CPN_TYP"]:
        if col not in df.columns:
            df[col] = np.nan

    df = df.set_index("ID_ISIN")
    return df


def load_ntnb_yields(ya_path: str, isin_list):
    """
    Lê as taxas das NTN-B do arquivo govt_ya.v1.xlsx (ya_values_only).
    Normaliza colunas para garantir match com ISIN do metadata.
    """
    df = pd.read_excel(ya_path, sheet_name="ya_values_only")

    # Normalize column names:
    new_cols = []
    for c in df.columns:
        c_str = str(c)
        # remove " Corp" (any casing), trailing spaces, double spaces, parentheses, unicode oddities
        c_str = c_str.replace("Corp", "")
        c_str = c_str.replace("corp", "")
        c_str = c_str.replace("CORP", "")
        c_str = c_str.replace("  ", " ")
        c_str = c_str.strip()
        new_cols.append(c_str)

    df.columns = new_cols

    # First column = date
    date_col = df.columns[0]
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col]).set_index(date_col)

    # Only keep columns that match metadata ISINs EXACTLY
    cols = [isin for isin in isin_list if isin in df.columns]

    # --- DEBUG ---
    print("\n[DEBUG] YA columns after normalization:", df.columns.tolist()[:20])
    print("[DEBUG] Matching columns:", cols)

    df = df[cols].apply(pd.to_numeric, errors="coerce")
    return df


def _build_cashflows_for_bond(
    obs_date: pd.Timestamp,
    maturity: pd.Timestamp,
    coupon_rate: float,
    freq: int,
):
    """
    Constrói cashflows simplificados para NTN-B:
    - cupom fixo 'coupon_rate' (em decimal) pago 'freq' vezes ao ano
    - principal = 1 no vencimento
    Ignora indexação do IPCA (trabalhamos em termos reais).
    """
    # datas de pagamento: frequência igual a freq, último pagamento na maturity
    # convenção simples: pagamentos regulares em ano civil / freq
    # (não é perfeito, mas segue a estrutura das NTNF no seu projeto)
    cf_dates = []
    current = maturity

    while current > obs_date:
        cf_dates.append(current)
        current = current - pd.DateOffset(months=int(12 / freq))

    cf_dates = sorted(cf_dates)
    if not cf_dates:
        return None

    coupon_cf = []
    for d in cf_dates:
        t = DAYCOUNT_BUS252.tf(obs_date.date(), d.date())
        # cupom real simples
        coupon_cf.append(coupon_rate / freq)

    # principal no último pagamento
    coupon_cf[-1] += 1.0

    return pd.Series(coupon_cf, index=[d.date() for d in cf_dates])


def build_real_curve_for_date(
    obs_date: pd.Timestamp,
    meta_df: pd.DataFrame,
    ya_df: pd.DataFrame,
    wla_yield_func_for_date,
):
    """
    Constrói curva soberana real via bootstrapping de cashflows NTN-B.
    - Convenção: bus/252 (ANBIMA)
    - Baseado em yields reais (govt_ya.v1.xlsx)
    - Combina com WLA via CombinedRealCurve(t_switch=5)
    """

    if obs_date not in ya_df.index:
        return None

    row = ya_df.loc[obs_date]

    cashflows = []
    rates = []

    for isin, y in row.items():
        if pd.isna(y):
            continue
        if isin not in meta_df.index:
            continue

        meta = meta_df.loc[isin]
        mat = meta["MATURITY"]
        if pd.isna(mat):
            continue

        # cupom (em decimal) e frequência
        cpn = float(meta.get("CPN", 0.0)) / 100.0  # assume % a.a.
        freq = int(meta.get("CPN_FREQ", 2) or 2)   # default semestral

        cf = _build_cashflows_for_bond(obs_date, mat, cpn, freq)
        if cf is None:
            continue

        rate = float(y) / 100.0  # yield em decimal

        cashflows.append(cf)
        rates.append(rate)

    if len(cashflows) < 2:
        # poucos bonds válidos pra bootstrapping
        return None

    # Bootstrapping da curva zero real
    bootstrap = CurveBootstrap(
        cash_flows=cashflows,
        rates=rates,
        day_count_convention="bus/252",
        calendar="cdr_anbima",
        ref_date=obs_date.date(),
    )

    zero_curve = bootstrap.zero_curve  # pd.Series indexado em anos

    # definimos função de yield para qualquer t:
    def ntnb_zero_yield(t: float) -> float:
        # usa a mesma flat_forward_interpolation interna do CurveBootstrap
        return bootstrap.rate_for_date(t)

    # WLA(t) para esta data (curva curta)
    def wla_func(t: float) -> float:
        return wla_yield_func_for_date(obs_date, t)

    return CombinedRealCurve(
        wla_func=wla_func,
        model_curve=ntnb_zero_yield,
        t_switch=5.0,
    )