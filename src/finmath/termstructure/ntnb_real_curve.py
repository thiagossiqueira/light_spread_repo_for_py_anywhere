# src/finmath/termstructure/ntnb_real_curve.py

import numpy as np
import pandas as pd
from calendars.daycounts import DayCounts

from src.finmath.termstructure.curve_models import CurveBootstrap
from src.finmath.termstructure.combined_real_curve import CombinedRealCurve

# ANBIMA convention for curves
DAYCOUNT_BUS252 = DayCounts("bus/252", calendar="cdr_anbima")


# =====================================================================
# 1. METADATA: carregar NTNB usando a coluna "id" como chave
# =====================================================================
def load_ntnb_metadata(govt_path: str) -> pd.DataFrame:
    """
    Lê metadados dos títulos públicos e filtra apenas NTN-B.
    Sheet: db_values_only
    Filtro: CALC_TYP_DES == 'BRAZIL I/L BOND'
    Índice: coluna 'id' (ex.: 'BRSTNCNTB4U6 Corp')
    """
    df = pd.read_excel(govt_path, sheet_name="db_values_only")

    # Filtrar só NTN-B
    df = df[df["CALC_TYP_DES"] == "BRAZIL I/L BOND"].copy()

    # Normalizar 'id'
    df["id"] = df["id"].astype(str).str.strip()

    # Maturidade
    df["MATURITY"] = pd.to_datetime(df["MATURITY"], errors="coerce")
    df = df.dropna(subset=["id", "MATURITY"])

    # Garantir colunas de cupom
    for col in ["CPN", "CPN_FREQ", "CPN_TYP"]:
        if col not in df.columns:
            df[col] = np.nan

    # Índice = id (ticker com "Corp")
    df = df.set_index("id")

    return df


# =====================================================================
# 2. CASHFLOWS simplificados para NTN-B (em termos REAIS)
# =====================================================================
def _build_cashflows_for_bond(
    obs_date: pd.Timestamp,
    maturity: pd.Timestamp,
    coupon_rate: float,
    freq: int,
) -> pd.Series | None:
    """
    Constrói cashflows simplificados para NTN-B:
    - cupom fixo 'coupon_rate' (decimal ao ano) pago 'freq' vezes ao ano
    - principal = 1 no vencimento
    - ignora indexação ao IPCA (trabalho em termos REAIS)
    """
    if pd.isna(maturity) or maturity <= obs_date:
        return None

    # Datas de pagamento: freq vezes ao ano, último pagamento na maturity
    cf_dates = []
    current = maturity

    while current > obs_date:
        cf_dates.append(current)
        # simplificação: ano civil / freq
        current = current - pd.DateOffset(months=int(12 / max(freq, 1)))

    cf_dates = sorted(cf_dates)
    if not cf_dates:
        return None

    cashflows = []
    for d in cf_dates:
        # cupom simples (real)
        cashflows.append(coupon_rate / freq)

    # principal no último pagamento
    cashflows[-1] += 1.0

    return pd.Series(cashflows, index=[d.date() for d in cf_dates])


# =====================================================================
# 3. Curva real soberana para UMA data (bootstrapping NTNB + WLA)
# =====================================================================
def build_real_curve_for_date(
    obs_date: pd.Timestamp,
    meta_df: pd.DataFrame,
    ya_df: pd.DataFrame,
    wla_yield_func_for_date,
) -> CombinedRealCurve | None:
    """
    Constrói curva soberana real via bootstrapping de cashflows NTN-B.
    - Convenção: bus/252 (ANBIMA)
    - Usa yields REAIS (ya_df) por 'id' (ex.: 'BRSTNCNTB4U6 Corp')
    - Combina com WLA via CombinedRealCurve(t_switch=5.0)
    """

    # Garante que a data exista na matriz de yields NTN-B
    if obs_date not in ya_df.index:
        return None

    row = ya_df.loc[obs_date]

    cashflows = []
    rates = []

    for sec_id, y in row.items():
        if pd.isna(y):
            continue
        if sec_id not in meta_df.index:
            continue

        meta = meta_df.loc[sec_id]
        mat = meta["MATURITY"]
        if pd.isna(mat) or mat <= obs_date:
            continue

        # cupom em decimal ao ano
        cpn = float(meta.get("CPN", 0.0))
        cpn = cpn / 100.0 if cpn is not None else 0.0

        # frequência de cupom
        freq = meta.get("CPN_FREQ", 2)
        try:
            freq = int(freq) if not pd.isna(freq) else 2
        except Exception:
            freq = 2

        cf = _build_cashflows_for_bond(obs_date, mat, cpn, freq)
        if cf is None or cf.empty:
            continue

        rate = float(y) / 100.0  # yield em decimal

        cashflows.append(cf)
        rates.append(rate)

    # Poucos bonds válidos para bootstrapping → sem curva
    if len(cashflows) < 2:
        return None

    # Bootstrapping da curva zero real
    bootstrap = CurveBootstrap(
        cash_flows=cashflows,
        rates=rates,
        day_count_convention="bus/252",
        calendar="cdr_anbima",
        ref_date=obs_date.date(),
    )

    # Função de zero-yield soberano real NTNB
    def ntnb_zero_yield(t: float) -> float:
        return bootstrap.rate_for_date(t)

    # WLA(t) para esta data (curto prazo)
    def wla_func(t: float) -> float:
        return wla_yield_func_for_date(obs_date, t)

    # Curva combinada final: WLA (0–5y) + NTNB bootstrapped (5y+)
    return CombinedRealCurve(
        wla_func=wla_func,
        model_curve=ntnb_zero_yield,
        t_switch=5.0,
    )