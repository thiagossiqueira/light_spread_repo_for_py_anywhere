# src/finmath/termstructure/ntnb_real_curve.py

import numpy as np
import pandas as pd

from calendars.daycounts import DayCounts

from src.finmath.termstructure.curve_models import fit_nss_yield_curve
from src.finmath.termstructure.combined_real_curve import CombinedRealCurve

# ANBIMA convention for sovereign curves
DAYCOUNT_BUS252 = DayCounts("bus/252", calendar="cdr_anbima")


# ------------------------------------------------------
# 1. Carregar metadados (NTN-B) a partir do GOVT_PATH
# ------------------------------------------------------
def load_ntnb_metadata(govt_path: str) -> pd.DataFrame:
    """
    Lê metadados dos títulos soberanos e filtra apenas NTN-B
    (BRAZIL I/L BOND) a partir da planilha domestic_sovereign_curve_brazil.xlsx.

    Espera:
      - sheet_name="db_values_only"
      - colunas: ID_ISIN (ou id), CALC_TYP_DES, MATURITY, etc.
      - identificadores com sufixo " Corp" (ex: "BRSTNCNTB4U6 Corp")
    """
    df = pd.read_excel(govt_path, sheet_name="db_values_only")

    # Alguns arquivos usam "id" em vez de "ID_ISIN" como identificador único
    if "ID_ISIN" in df.columns:
        id_col = "ID_ISIN"
    else:
        id_col = "id"

    # Normalizar ID: string + strip
    df[id_col] = df[id_col].astype(str).str.strip()

    # Filtrar apenas NTN-B
    if "CALC_TYP_DES" in df.columns:
        df["CALC_TYP_DES"] = df["CALC_TYP_DES"].astype(str).str.upper().str.strip()
        df = df[df["CALC_TYP_DES"] == "BRAZIL I/L BOND"].copy()
    else:
        # fallback: se não houver CALC_TYP_DES, retorna vazio
        return df.iloc[0:0]

    # Converter MATURITY para datetime
    df["MATURITY"] = pd.to_datetime(df["MATURITY"], errors="coerce")
    df = df.dropna(subset=[id_col, "MATURITY"])

    # Garantir colunas mínimas para cupom, frequência etc.
    for col in ["CPN", "CPN_FREQ", "CPN_TYP"]:
        if col not in df.columns:
            df[col] = np.nan

    # Usar o identificador como índice (mesmo ID usado em govt_ya.v1.xlsx)
    df = df.set_index(id_col)

    return df


# ------------------------------------------------------
# 2. Carregar yields YA (NTN-B) do GOVT_YA_PATH
# ------------------------------------------------------
def load_ntnb_yields(ya_path: str, isin_index) -> pd.DataFrame:
    """
    Lê as taxas das NTN-B do arquivo govt_ya.v1.xlsx (sheet "ya_values_only").

    Espera:
      - Primeira coluna: data
      - Demais colunas: IDs iguais aos do metadata
        (ex: "BRSTNCNTB4U6 Corp").
    """
    df = pd.read_excel(ya_path, sheet_name="ya_values_only")

    # Normalizar nomes de colunas (manter exatamente o que vem do Excel,
    # apenas strip de espaços e conversão para string)
    df.columns = [str(c).strip() for c in df.columns]

    # Primeira coluna = data
    date_col = df.columns[0]
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col]).set_index(date_col)

    # Manter apenas colunas que existem no índice de metadados
    isin_list = list(isin_index)
    cols = [c for c in df.columns if c in isin_list]

    # Subconjunto + conversão para numérico
    df = df[cols].apply(pd.to_numeric, errors="coerce")

    return df


# ------------------------------------------------------
# 3. Construir curva real soberana para UMA data (NSS)
# ------------------------------------------------------
def build_real_curve_for_date(
    obs_date: pd.Timestamp,
    meta_df: pd.DataFrame,
    ya_df: pd.DataFrame,
    wla_yield_func_for_date,
) -> CombinedRealCurve | None:
    """
    Constrói a curva soberana REAL para uma data específica, usando:

      - Metadados de NTN-B (MATURITY por ID)
      - Yields YA (linha de govt_ya.v1.xlsx para obs_date)
      - wla_yield_func_for_date(obs_date, t): função que devolve WLA(t)
        para a MESMA data

    Abordagem:
      1. Para cada NTN-B com yield disponível em obs_date:
         - calcula tenor em anos (bus/252, ANBIMA)
         - converte yield (%) em decimal
      2. Ajusta uma curva NSS em termos de yield(t) sobre esses pontos (t, y)
      3. Cria uma CombinedRealCurve que:
         - usa WLA para tenores curtos (ex: até 5 anos)
         - usa NSS de NTN-B para tenores longos (ex: > 5 anos)

    Retorna CombinedRealCurve ou None se não houver dados suficientes.
    """
    if ya_df.empty or obs_date not in ya_df.index:
        return None

    row = ya_df.loc[obs_date]

    t_list: list[float] = []
    y_list: list[float] = []

    # Percorre todos os IDs do metadata (NTN-B) e coleta yields disponíveis
    for isin in meta_df.index:
        if isin not in row.index:
            continue

        y = row[isin]
        if pd.isna(y):
            continue

        mat = meta_df.loc[isin, "MATURITY"]
        if pd.isna(mat):
            continue

        # Tenor em anos, convenção bus/252 (ANBIMA)
        try:
            t_years = DAYCOUNT_BUS252.tf(obs_date.to_pydatetime().date(), mat.date())
        except Exception:
            continue

        if t_years <= 0:
            continue

        t_list.append(float(t_years))
        # Yield é % a.a. real => converter para decimal
        y_list.append(float(y) / 100.0)

    # Precisamos de pelo menos alguns pontos para ajustar NSS
    if len(t_list) < 4:
        return None

    t_arr = np.array(t_list, dtype=float)
    y_arr = np.array(y_list, dtype=float)

    # Ajuste NSS em termos de yield(t)
    nss_curve = fit_nss_yield_curve(t_arr, y_arr)

    # Função WLA(t) para ESTA data
    def wla_func(t: float) -> float:
        return wla_yield_func_for_date(obs_date, t)

    # Função de yield da NSS de NTN-B para ESTA data
    def ntnb_yield_func(t: float) -> float:
        return nss_curve.yield_at(t)

    # Curva combinada WLA (0–5y) + NTN-B (5y+)
    combined = CombinedRealCurve(
        wla_func=wla_func,
        model_curve=ntnb_yield_func,
        t_switch=5.0,
    )

    return combined