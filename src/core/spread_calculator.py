# src/core/spread_calculator.py
import numpy as np
import pandas as pd
from utils.interpolation import interpolate_yield_for_tenor
from calendars.daycounts import DayCounts
from config import CONFIG  # ✅ used to map tenor labels like "12-year" -> 12.0

# Convenção ANBIMA: Business / 252 dias úteis
DAYCOUNT = DayCounts("bus/252", calendar="cdr_anbima")


# ================================================================
# Função padrão: usada para bonds com cupom (corporate, NTNF, NTNB)
# ================================================================
def compute_spreads(corp_base, yields_ts, yc_table, observation_periods, tenors_dict):
    """
    Calcula spreads entre os yields dos bonds e a curva DI/IPCA interpolada.
    Usa daycount bus/252 (ANBIMA). Retorna sempre (corp_bonds, skipped).
    """
    expanded_rows = []
    skipped = []

    for _, bond in corp_base.iterrows():
        bond_id = bond["id"]
        obs_start, obs_end = observation_periods.get(bond_id, (None, None))
        if obs_start is None:
            continue

        for obs_date, di_row in yc_table.iterrows():
            if not (obs_start <= obs_date <= obs_end):
                continue

            # Yield observado (YAS_BOND_YLD)
            try:
                yas_yld = yields_ts.at[obs_date, bond_id]
            except KeyError:
                skipped.append((bond_id, obs_date, "Missing column or date"))
                continue
            if pd.isna(yas_yld):
                skipped.append((bond_id, obs_date, "NaN yield"))
                continue

            # Tenor em anos (bus/252)
            tenor_yrs = DAYCOUNT.tf(obs_date, bond["MATURITY"])
            if tenor_yrs <= 0:
                continue

            # Interpolação da curva
            interpolated_di_yield = interpolate_yield_for_tenor(
                obs_date, yc_table, tenor_yrs, tenors_dict, obs_date
            )

            spread = interpolated_di_yield - yas_yld

            expanded_rows.append({
                "id": bond_id,
                "OBS_DATE": obs_date,
                "MATURITY": bond["MATURITY"],
                "YAS_BOND_YLD": yas_yld,
                "DI_YIELD": interpolated_di_yield,
                "SPREAD": spread,
                "CPN_TYP": bond.get("CPN_TYP", "Corp bond"),
                "CPN": bond.get("CPN", np.nan),
                "DAYS_TO_MATURITY": (bond["MATURITY"] - obs_date).days,
                "TENOR_YRS": tenor_yrs,
            })

    corp_bonds = pd.DataFrame(expanded_rows)
    if corp_bonds.empty:
        raise ValueError("No valid corporate bond spreads calculated.")

    # Bucketização de tenores
    names = list(tenors_dict.keys())
    vals = np.array(list(tenors_dict.values()))
    corp_bonds["TENOR_BUCKET"] = corp_bonds["TENOR_YRS"].apply(
        lambda y: names[np.argmin(np.abs(vals - y))]
    )

    # ✅ Retorno garantido
    return corp_bonds, skipped


# ================================================================
# Função específica para LTNs (zero-coupon)
# ================================================================
def compute_spreads_ltn(df_ltn: pd.DataFrame, yc_table: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula spreads simples para LTNs (zero-coupon), comparando o yield observado
    com a curva DI interpolada de mesmo tenor.
    Usa convenção de contagem de dias bus/252 (ANBIMA), igual aos corporativos.
    Inclui print de debug da curva DI interpolada (yc_interp).
    """
    df = df_ltn.copy()

    # Converter colunas de datas
    df["MATURITY"] = pd.to_datetime(df["MATURITY"], errors="coerce")
    df["OBS_DATE"] = pd.to_datetime(df["OBS_DATE"], errors="coerce")

    # Calcular tenor em anos pela convenção bus/252 (ANBIMA)
    df["TENOR_YRS"] = df.apply(
        lambda r: DAYCOUNT.tf(r["OBS_DATE"], r["MATURITY"])
        if pd.notna(r["OBS_DATE"]) and pd.notna(r["MATURITY"])
        else np.nan,
        axis=1
    )
    df = df[df["TENOR_YRS"] > 0]

    # Validar curva DI
    if yc_table is None or yc_table.empty:
        raise ValueError("yc_table vazia: não é possível calcular DI de referência para LTNs.")

    # Selecionar curva DI média ou linha única (transposta para usar tenores como índice)
    if yc_table.shape[0] > 1:
        yc_interp = yc_table.T.mean(axis=1)
    else:
        yc_interp = yc_table.T.iloc[:, 0]

    # 🧭 DEBUG: visualizar estrutura inicial da curva DI
    print("\n--- [DEBUG] Curva DI Interpolada (yc_interp) ---")
    print(f"Tipo: {type(yc_interp)}")
    print(f"Tamanho: {len(yc_interp)}")
    print("Index (tenores):", yc_interp.index.tolist()[:10])
    print("Valores (yields):", yc_interp.values[:10])
    print("------------------------------------------------\n")

    # Converter índice da curva DI em numérico usando CONFIG["TENORS"]
    tenor_map = CONFIG.get("TENORS", {})

    if yc_interp.index.dtype == object:
        mapped_index = yc_interp.index.map(tenor_map)
        yc_interp.index = mapped_index.astype(float)
    else:
        yc_interp.index = pd.to_numeric(yc_interp.index, errors="coerce")

    # Remover NaNs (unmapped labels)
    yc_interp = yc_interp[~pd.isna(yc_interp.index)]

    # 🧭 DEBUG: mostrar tenores mapeados
    print("\n--- [DEBUG] yc_interp index mapped with CONFIG['TENORS'] ---")
    print("Mapped index (tenors in years):", yc_interp.index.tolist()[:10])
    print("Yields:", yc_interp.values[:10])
    print("------------------------------------------------------------\n")

    # Verificar se há curva válida
    if len(yc_interp) == 0:
        raise ValueError("Curva DI interpolada está vazia após mapeamento de índices.")

    # Interpolar yield DI mais próximo para cada tenor observado
    df["DI_YIELD"] = df["TENOR_YRS"].apply(
        lambda t: yc_interp.iloc[(abs(yc_interp.index - t)).argmin()]
    )

    # Converter yields para float e calcular spread (em basis points)
    df["YAS_BOND_YLD"] = pd.to_numeric(df["YAS_BOND_YLD"], errors="coerce")
    df["DI_YIELD"] = pd.to_numeric(df["DI_YIELD"], errors="coerce")
    df["SPREAD"] = (df["DI_YIELD"] - df["YAS_BOND_YLD"])  # basis points

    # Criar bucket aproximado de maturidade
    names = [str(round(i, 2)) for i in yc_interp.index]
    vals = np.array(list(yc_interp.index))
    df["TENOR_BUCKET"] = df["TENOR_YRS"].apply(
        lambda y: names[np.argmin(np.abs(vals - y))]
    )

    # Colunas adicionais para consistência
    df["CPN_TYP"] = "ZERO"
    df["CPN"] = np.nan
    df["DAYS_TO_MATURITY"] = df.apply(
        lambda r: DAYCOUNT.days(r["OBS_DATE"], r["MATURITY"])
        if pd.notna(r["OBS_DATE"]) and pd.notna(r["MATURITY"])
        else np.nan,
        axis=1
    )

    return df