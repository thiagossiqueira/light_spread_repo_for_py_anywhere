# src/core/curve_builder.py

import pandas as pd
from config import CONFIG

# ---- Importações das curvas ----
from src.finmath.termstructure.ntnb_real_curve import (
    load_ntnb_metadata,
    load_ntnb_yields,
    build_real_curve_for_date
)

from utils.interpolation import interpolate_yield_for_tenor


# ============================================================
# 1) Carregar suportes necessários para construir a curva real:
#       - meta_df  (características NTN-B)
#       - ya_df    (yields YA por ISIN)
# ============================================================

def load_real_curve_support():
    """
    Carrega e prepara os dois componentes essenciais para montar
    a curva soberana real diária (NTNB + NSS + WLA):

        - metadados (MATURITY por ISIN)
        - yields YA (time series por ISIN)

    Retorna: (meta_df, ya_df)
    """
    govt_path = CONFIG["GOVT_PATH"]
    ya_path = CONFIG["YA_PATH"]

    meta_df = load_ntnb_metadata(govt_path)
    ya_df = load_ntnb_yields(ya_path, meta_df.index.tolist())

    return meta_df, ya_df


# ============================================================
# 2) Função WLA diária usada pelo NTNB + NSS
#
# Esta função recebe:
#     - obs_date : pd.Timestamp
#     - tenor    : float (anos)
#
# E retorna o yield real WLA interpolado naquela data.
# ============================================================

def wla_yield_for_date(obs_date, tenor_years):
    """
    Retorna o yield WLA interpolado para uma data específica.
    Usa o mesmo mecanismo que você já possui (hist_ipca_curve_contracts_db.xlsx).

    CONFIG["WLA_TABLE"] deve conter sua tabela carregada da DB.
    """
    wla_table = CONFIG.get("WLA_TABLE", None)
    tenors_dict = CONFIG.get("WLA_TENORS", {})

    if wla_table is None:
        raise ValueError("CONFIG['WLA_TABLE'] não foi carregada.")

    return interpolate_yield_for_tenor(
        obs_date,
        wla_table,
        tenor_years,
        tenors_dict,
        obs_date
    )


# ============================================================
# 3) Função para montar curva real combinada POR DATA
#
# Esta função é usada pelo spread_calculator.
# ============================================================

def build_real_curve_for_obs_date(obs_date, meta_df, ya_df):
    """
    Wrapper pequeno para usar no compute_spreads.

    Retorna a curva soberana real (CombinedRealCurve)
    para a data obs_date, usando:
       - meta_df
       - ya_df
       - NSS
       - matching com WLA aos 5 anos
    """
    return build_real_curve_for_date(
        obs_date,
        meta_df,
        ya_df,
        wla_yield_for_date
    )
