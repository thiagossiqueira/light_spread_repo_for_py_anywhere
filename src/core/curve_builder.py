# src/core/curve_builder.py

import pandas as pd
from src.utils.file_io import load_govt_bond_data, load_yield_surface
from src.config import CONFIG
from src.finmath.termstructure.ntnb_real_curve import (
    load_ntnb_metadata,
    build_real_curve_for_date,
)
from src.finmath.termstructure.combined_real_curve import CombinedRealCurve
from calendars.daycounts import DayCounts

DAYCOUNT_BUS252 = DayCounts("bus/252", calendar="cdr_anbima")


# ============================================================
# 1. Carregar metadados + YA de NTNB, já alinhados
# ============================================================
def load_real_curve_support():
    """
    Carrega:
      - metadados das NTN-B (via load_ntnb_metadata, index = 'id')
      - matriz de yields YA para esses mesmos 'id' (GOVT_YA_PATH)

    Retorna:
      (ntnb_meta_df, ntnb_ya_df)
    """
    # Metadados NTNB a partir de domestic_sovereign_curve_brazil.xlsx
    ntnb_meta_df = load_ntnb_metadata(CONFIG["GOVT_PATH"])

    # Yields de governo (toda a matriz)
    ya_all = load_yield_surface(CONFIG["GOVT_YA_PATH"])
    # load_yield_surface:
    #   - lê sheet "ya_values_only"
    #   - primeira coluna -> OBS_DATE (index)
    #   - colunas restantes -> IDs (strings strip())

    # Alinhar usando 'id'
    meta_ids = ntnb_meta_df.index.astype(str).tolist()
    ya_cols = ya_all.columns.astype(str).tolist()

    overlap = [c for c in ya_cols if c in meta_ids]

    ntnb_ya_df = ya_all[overlap].copy()

    # DEBUG opcional
    print("\n[REAL CURVE SUPPORT] NTNB meta count:", len(ntnb_meta_df))
    print("[REAL CURVE SUPPORT] YA NTNB columns:", len(ntnb_ya_df.columns))
    print("[REAL CURVE SUPPORT] Overlap sample:", overlap[:10])

    return ntnb_meta_df, ntnb_ya_df


# ============================================================
# 2. Wrapper para WLA: yield real curta para uma data
# ============================================================
def wla_yield_for_date(obs_date: pd.Timestamp, t_years: float) -> float:
    """
    Função helper para obter WLA(t) em uma data.
    Aqui reaproveitamos a surface IPCA (CONFIG["WLA_CURVE_PATH"])
    e a interpolação já existente.
    """
    from src.utils.file_io import load_ipca_surface
    from src.utils.interpolation import interpolate_surface

    surface = load_ipca_surface(CONFIG["WLA_CURVE_PATH"])
    tenors = CONFIG["WLA_TENORS"]

    yc_table = interpolate_surface(surface, tenors)

    if obs_date not in yc_table.index:
        # fallback: usar última curva disponível
        obs_date_eff = yc_table.index.max()
    else:
        obs_date_eff = obs_date

    row = yc_table.loc[obs_date_eff]

    # converter índices de yc_table (labels) para anos via tenors dict
    tenor_map = {k: float(v) for k, v in tenors.items()}
    yc_series = row.copy()
    yc_series.index = [tenor_map.get(str(idx), float("nan")) for idx in yc_series.index]
    yc_series = yc_series.dropna()

    if yc_series.empty:
        return float("nan")

    # escolher o tenor mais próximo
    arr_idx = np.array(yc_series.index, dtype=float)
    idx = np.argmin(np.abs(arr_idx - float(t_years)))
    return float(yc_series.iloc[idx])


# ============================================================
# 3. Builder para uma CombinedRealCurve por data
# ============================================================
def build_real_curve_for_obs_date(
    obs_date: pd.Timestamp,
    ntnb_meta_df: pd.DataFrame,
    ntnb_ya_df: pd.DataFrame,
) -> CombinedRealCurve | None:
    """
    Construção de CombinedRealCurve (WLA + NTNB) para uma data específica,
    usando:
      - ntnb_meta_df (index = 'id')
      - ntnb_ya_df (matrix de yields por 'id')
      - wla_yield_for_date como perna curta.
    """
    return build_real_curve_for_date(
        obs_date=obs_date,
        meta_df=ntnb_meta_df,
        ya_df=ntnb_ya_df,
        wla_yield_func_for_date=wla_yield_for_date,
    )