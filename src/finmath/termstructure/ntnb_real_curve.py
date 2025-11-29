# src/finmath/termstructure/ntnb_real_curve.py

import numpy as np
import pandas as pd
from calendars.daycounts import DayCounts

from src.finmath.termstructure.curve_models import fit_nss_yield_curve
from src.finmath.termstructure.combined_real_curve import CombinedRealCurve

DAYCOUNT_ACT365 = DayCounts("act/365")


# ------------------------------------------------------
# 1. Carregar metadados (somente NTN-B)
# ------------------------------------------------------
def load_ntnb_metadata(govt_path: str) -> pd.DataFrame:
    """
    Lê metadados dos títulos públicos e filtra apenas NTN-B.
    Sheet correto: db_values_only.
    """

    df = pd.read_excel(govt_path, sheet_name="db_values_only")

    # Selecionar apenas NTNB (BRAZIL I/L BOND)
    df = df[df["CALC_TYP_DES"] == "BRAZIL I/L BOND"].copy()

    df["MATURITY"] = pd.to_datetime(df["MATURITY"], errors="coerce")
    df = df.dropna(subset=["ID_ISIN", "MATURITY"])

    df = df.set_index("ID_ISIN")

    return df


# ------------------------------------------------------
# 2. Carregar yields YA (por ISIN, sheet correto)
# ------------------------------------------------------
def load_ntnb_yields(ya_path: str, isin_list):
    """
    Lê as taxas das NTN-B do arquivo govt_ya.v1.xlsx.
    Sheet correto: ya_values_only
    """

    df = pd.read_excel(ya_path, sheet_name="ya_values_only")

    date_col = df.columns[0]
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col]).set_index(date_col)

    # manter apenas ISINs existentes no metadata
    cols = [isin for isin in isin_list if isin in df.columns]
    df = df[cols].apply(pd.to_numeric, errors="coerce")

    return df


# -----------------------------------------------------------
# 3. Construir curva real soberana para UMA data específica
# -----------------------------------------------------------
def build_real_curve_for_date(
    obs_date: pd.Timestamp,
    meta_df: pd.DataFrame,
    ya_df: pd.DataFrame,
    wla_yield_func_for_date,
):
    """
    Constrói curva soberana real (WLA 0-5 + NTNB 5+) para uma data específica.
    """

    if obs_date not in ya_df.index:
        return None

    row = ya_df.loc[obs_date]

    t_list = []
    y_list = []

    for isin, y in row.items():
        if pd.isna(y):
            continue
        if isin not in meta_df.index:
            continue

        mat = meta_df.loc[isin, "MATURITY"]
        if pd.isna(mat):
            continue

        # ACT/365 ano fracionário
        t_years = DAYCOUNT_ACT365.tf(
            obs_date.to_pydatetime().date(), mat.date()
        )
        if t_years <= 0:
            continue

        t_list.append(t_years)
        y_list.append(float(y) / 100.0)  # converter p/ decimal

    # precisa ter ao menos 4 pontos para ajustar NSS
    if len(t_list) < 4:
        return None

    t_arr = np.array(t_list)
    y_arr = np.array(y_list)

    # Ajuste NSS yield-based (simples e estável)
    model_curve = fit_nss_yield_curve(t_arr, y_arr)

    # Função WLA(t) específica para esta data
    def wla_func(t: float):
        return wla_yield_func_for_date(obs_date, t)

    # Curva real combinada final (matching em 5 anos)
    return CombinedRealCurve(
        wla_func=wla_func,
        model_curve=model_curve,
        t_switch=5.0,
    )