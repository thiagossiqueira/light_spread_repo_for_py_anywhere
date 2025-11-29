# src/finmath/termstructure/ntnb_real_curve.py

import numpy as np
import pandas as pd
from dataclasses import dataclass
from calendars.daycounts import DayCounts

from src.finmath.termstructure.curve_models import fit_nelson_siegel_svensson
from src.finmath.termstructure.combined_real_curve import CombinedRealCurve


DAYCOUNT_ACT365 = DayCounts("act/365")


# ------------------------------------------------------
# 1. Carregar metadados (para filtrar apenas NTN-B)
# ------------------------------------------------------
def load_ntnb_metadata(govt_path: str) -> pd.DataFrame:
    """
    Lê metadados dos títulos públicos e filtra apenas NTN-B:
    CALC_TYP_DES == 'BRAZIL I/L BOND'.
    """
    df = pd.read_excel(govt_path, sheet_name="ya_values_only")

    df = df[df["CALC_TYP_DES"] == "BRAZIL I/L BOND"].copy()
    df["MATURITY"] = pd.to_datetime(df["MATURITY"], errors="coerce")

    df = df.dropna(subset=["ID_ISIN", "MATURITY"])
    df = df.set_index("ID_ISIN")

    return df


# ------------------------------------------------------
# 2. Carregar yields YA (por ISIN)
# ------------------------------------------------------
def load_ntnb_yields(ya_path: str, isin_list):
    """
    Lê as taxas das NTN-B do arquivo govt_ya.v1.xlsx.
    await first column: date
    following columns: ISINs
    """
    df = pd.read_excel(ya_path, sheet_name="ya_values_only")

    date_col = df.columns[0]
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col]).set_index(date_col)

    # manter apenas ISINs existentes
    cols = [c for c in isin_list if c in df.columns]
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
    Constrói curva soberana real (WLA 0-5 + NTN-B 5+) para uma data específica.
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

        # year fraction ACT/365
        t_years = DAYCOUNT_ACT365.tf(obs_date.to_pydatetime().date(), mat.date())
        if t_years <= 0:
            continue

        t_list.append(t_years)
        y_list.append(float(y) / 100.0)  # assume % a.a.

    # precisa ter dados suficientes para ajustar NSS
    if len(t_list) < 4:
        return None

    t_arr = np.array(t_list)
    y_arr = np.array(y_list)

    # Ajuste NSS
    model_curve = fit_nelson_siegel_svensson(t_arr, y_arr)

    # função WLA(t) para ESTA data específica
    def wla_func(t: float):
        return wla_yield_func_for_date(obs_date, t)

    # curva combinada final
    return CombinedRealCurve(
        wla_func=wla_func,
        model_curve=model_curve,
        t_switch=5.0,
    )