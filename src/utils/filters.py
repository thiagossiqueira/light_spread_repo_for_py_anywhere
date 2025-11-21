import pandas as pd
from src.config import CONFIG

def filter_corporate_universe(df: pd.DataFrame, inflation_linked: str = "N", log=None) -> pd.DataFrame:
    """
    Aplica os filtros padrão para selecionar o universo de bonds corporativos.
    Permite registrar os passos em um log opcional.
    """

    print_fn = (
        (lambda *args, **kwargs: print(*args, **kwargs))
        if log is None else
        (lambda *args, **kwargs: print(*args, **kwargs, file=log))
    )

    df = df.copy()
    print_fn(f"🔍 Inicial: {len(df)} linhas")

    # Filtros básicos
    df = df[~df['CLASSIFICATION_LEVEL_4_NAME'].str.startswith("Government", na=False)]
    print_fn(f"➡ Após remover 'Government': {len(df)}")

    df = df[~df['industry_sector'].isin(['Financial'])]
    print_fn(f"➡ Após remover 'Financial': {len(df)}")

    df = df[df['CPN_TYP'].isin(['FIXED'])]
    print_fn(f"➡ Após filtrar CPN_TYP='FIXED': {len(df)}")

    df = df[df['CRNCY'].isin(['BRL'])]
    print_fn(f"➡ Após filtrar CRNCY='BRL': {len(df)}")

    # Filtro por indexação à inflação
    df["INFLATION_LINKED_INDICATOR"] = (
        df["INFLATION_LINKED_INDICATOR"]
        .astype(str)
        .str.strip()
        .str.upper()
    )
    unique_vals = df["INFLATION_LINKED_INDICATOR"].unique()
    print_fn(f"🧪 Valores únicos normalizados em INFLATION_LINKED_INDICATOR: {unique_vals}")

    df = df[df["INFLATION_LINKED_INDICATOR"] == inflation_linked.strip().upper()]
    print_fn(f"➡ Após filtrar INFLATION_LINKED_INDICATOR={inflation_linked}: {len(df)}")

    # TOT_DEBT_TO_EBITDA válido
    df['TOT_DEBT_TO_EBITDA'] = pd.to_numeric(df['TOT_DEBT_TO_EBITDA'], errors='coerce')
    print_fn(f"➡ Após conversão de TOT_DEBT_TO_EBITDA (com NaN): {df['TOT_DEBT_TO_EBITDA'].isna().sum()} NaNs")

    df = df[df['TOT_DEBT_TO_EBITDA'].notna()]
    print_fn(f"➡ Após remover TOT_DEBT_TO_EBITDA nulos: {len(df)}")

    df["MATURITY"] = pd.to_datetime(df["MATURITY"], errors='coerce')

    return df


def filter_government_universe(df: pd.DataFrame, inflation_linked: str = "N", bond_type: str = None, log=None) -> pd.DataFrame:
    print_fn = (lambda *args, **kwargs: print(*args, **kwargs)) if log is None else (lambda *args, **kwargs: print(*args, **kwargs, file=log))

    df = df.copy()
    print_fn(f"🔍 Inicial: {len(df)} linhas")

    # Mantém apenas títulos BRL e FIXED
    df = df[df["CRNCY"] == "BRL"]
    df = df[df["CPN_TYP"] == "FIXED"]
    print_fn(f"➡ Após filtrar CPN_TYP='FIXED' e CRNCY='BRL': {len(df)}")

    # Normaliza indicador de inflação
    df["INFLATION_LINKED_INDICATOR"] = df["INFLATION_LINKED_INDICATOR"].astype(str).str.strip().str.upper()
    df = df[df["INFLATION_LINKED_INDICATOR"] == inflation_linked.strip().upper()]
    print_fn(f"➡ Após filtrar INFLATION_LINKED_INDICATOR={inflation_linked}: {len(df)}")

    # Se bond_type for especificado (LTN, NTNF, NTNB)
    if bond_type:
        df = df[df["SECURITY_TYP"].str.upper() == bond_type.upper()]
        print_fn(f"➡ Após filtrar SECURITY_TYP={bond_type}: {len(df)}")

    df["MATURITY"] = pd.to_datetime(df["MATURITY"], errors="coerce")
    return df


def anomaly_filtering_results(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica filtros para eliminar observações com yields zerados ou spreads anômalos.
    """
    df = df.copy()
    df = df[df["YAS_BOND_YLD"] != 0]
    df = df[(df["SPREAD"] >= -10) & (df["SPREAD"] <= 10)]
    return df


def apply_custom_filters(df: pd.DataFrame, inflation: str, exclude_gov: bool, exclude_fin: bool,
                         cpns: list) -> pd.DataFrame:
    df = df.copy()

    if exclude_gov:
        df = df[~df["CLASSIFICATION_LEVEL_4_NAME"].str.startswith("Government", na=False)]

    if exclude_fin:
        df = df[~df["industry_sector"].isin(["Financial"])]

    if cpns:
        df = df[df["CPN_TYP"].isin(cpns)]

    df["INFLATION_LINKED_INDICATOR"] = df["INFLATION_LINKED_INDICATOR"].astype(str).str.strip().str.upper()
    df = df[df["INFLATION_LINKED_INDICATOR"] == inflation.strip().upper()]

    return df


def load_raw_corp_data() -> pd.DataFrame:
    """
    Carrega a base de dados de bonds corporativos sem aplicar filtros.
    """
    df = pd.read_excel(CONFIG["CORP_PATH"], sheet_name="db_values_only")
    df["id"] = df["id"].astype(str).str.strip()
    return df


def load_raw_govt_data() -> pd.DataFrame:
    """
    Carrega a base de dados de bonds soberanos (governo) e cria a coluna SECURITY_TYP
    a partir de CALC_TYP_DES, mapeando tipos padrão (LTN, LFT, NTNF, NTNB).
    """
    path = CONFIG["GOVT_PATH"]

    try:
        df = pd.read_excel(path, sheet_name="db_values_only")
    except FileNotFoundError:
        raise FileNotFoundError(f"❌ Arquivo GOVT_PATH não encontrado: {path}")
    except Exception as e:
        raise RuntimeError(f"❌ Erro ao carregar GOVT_PATH ({path}): {e}")

    if "id" not in df.columns:
        raise KeyError("A coluna 'id' não foi encontrada na planilha GOVT_PATH.")
    df["id"] = df["id"].astype(str).str.strip()

    # ===========================
    # 🔧 Mapeamento CALC_TYP_DES → SECURITY_TYP
    # ===========================
    if "CALC_TYP_DES" in df.columns:
        df["CALC_TYP_DES"] = df["CALC_TYP_DES"].astype(str).str.upper().str.strip()

        mapping = {
            "BRAZIL: BBCS/LTNS": "LTN",
            "BRAZIL BBCS/LTNS": "LTN",
            "BRAZIL LFT ANN-OVR": "LFT",
            "BRAZIL FIXED CPN": "NTNF",
            "BRAZIL I/L BOND": "NTNB",
        }

        df["SECURITY_TYP"] = df["CALC_TYP_DES"].map(mapping)
    else:
        print("⚠️ Coluna CALC_TYP_DES não encontrada em GOVT_PATH — SECURITY_TYP não será gerado.")
        df["SECURITY_TYP"] = None

    return df




