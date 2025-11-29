# main.py

from src.utils.filters import (
    filter_corporate_universe,
    filter_government_universe,
    anomaly_filtering_results,
)
from src.utils.file_io import (
    load_corp_bond_data,
    load_govt_bond_data,
    load_yield_surface,
    load_di_surface,
    load_ipca_surface,
)
from src.utils.interpolation import interpolate_di_surface, interpolate_surface
from src.utils.plotting import (
    plot_surface_spread_with_bonds,
    plot_yield_curve_surface,
    show_summary_table,
    show_di_summary_table,
    show_ipca_summary_table,
    show_benchmark_table,
)

from src.core.windowing import build_observation_windows
from src.core.spread_calculator import compute_spreads, compute_spreads_ltn
from calendars.daycounts import DayCounts
from src.config import CONFIG

# NEW imports for REAL CURVE
from src.core.curve_builder import (
    load_real_curve_support,
    build_real_curve_for_obs_date,
    wla_yield_for_date,
)

# 🔧 FIX: REAL_CURVE_TENORS must be loaded from CONFIG
REAL_CURVE_TENORS = CONFIG["REAL_CURVE_TENORS"]

import pandas as pd
import os


if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    os.makedirs("templates", exist_ok=True)

    # ============================================================
    # LOAD NTN-B METADATA + YA VALUES FOR REAL CURVE IPCA
    # ============================================================
    ntnb_meta_df, ntnb_ya_df = load_real_curve_support()

    print("NTNB metadata count:", len(ntnb_meta_df))
    print(ntnb_meta_df.index.tolist()[:15])

    print("NTNB YA columns:", len(ntnb_ya_df.columns))
    print(ntnb_ya_df.columns.tolist()[:15])

    print("Matching:", len(set(ntnb_meta_df.index) & set(ntnb_ya_df.columns)))

    # ============================================================
    # CORPORATE BONDS
    # ============================================================
    corp_base_raw = load_corp_bond_data(CONFIG["CORP_PATH"])

    universes = {
        "di": {
            "yields_ts": load_yield_surface(CONFIG["YA_PATH"]),
            "surface": load_di_surface(CONFIG["HIST_CURVE_PATH"]),
            "tenors": CONFIG["TENORS"],
            "inflation_linked": "N",
            "use_real_curve": False,
        },
        "ipca": {
            "yields_ts": load_yield_surface(CONFIG["YA_PATH"]),
            "surface": None,
            "tenors": REAL_CURVE_TENORS,
            "inflation_linked": "Y",
            "use_real_curve": True,
        },
    }

    for tipo, params in universes.items():
        log_path = f"data/logs_{tipo}.txt"
        with open(log_path, "w", encoding="utf-8") as log_file:

            def print_fn(*args, **kwargs):
                print(*args, **kwargs)
                print(*args, **kwargs, file=log_file)

            print_fn(f"\n📊 Processando universo: {tipo.upper()}")

            yields_ts = params["yields_ts"]
            tenors = params["tenors"]
            inflation_linked = params["inflation_linked"]
            use_real_curve = params["use_real_curve"]

            corp_base = corp_base_raw.copy()
            corp_base = corp_base[corp_base["id"].isin(yields_ts.columns)]

            corp_base = filter_corporate_universe(
                corp_base, inflation_linked=inflation_linked, log=log_file
            )
            print_fn(f"🧮 Bonds disponíveis após filtro ({tipo}): {len(corp_base)}")

            obs_windows = build_observation_windows(
                corp_base, yields_ts, CONFIG["OBS_WINDOW"]
            )

            # ============================================================
            # BUILD SURFACE
            # ============================================================
            if tipo == "di":
                surface = load_di_surface(CONFIG["HIST_CURVE_PATH"])
                yc_table = interpolate_di_surface(surface, tenors)

            else:
                # NEW: Build REAL IPCA surface (0–30 years)
                surface_list = []
                for obs_date in yields_ts.index:
                    real_curve = build_real_curve_for_obs_date(
                        obs_date, ntnb_meta_df, ntnb_ya_df
                    )
                    if real_curve is None:
                        continue

                    for label, t in REAL_CURVE_TENORS.items():
                        surface_list.append({
                            "obs_date": obs_date,
                            "generic_ticker_id": label,
                            "yield": real_curve.yield_at(t),
                            "tenor": t,
                        })

                surface = pd.DataFrame(surface_list)
                yc_table = interpolate_surface(surface, tenors)

            # ============================================================
            # COMPUTE SPREADS
            # ============================================================
            if use_real_curve:
                corp_bonds, skipped = compute_spreads(
                    corp_base,
                    yields_ts,
                    yc_table,
                    obs_windows,
                    tenors,
                    build_real_curve_for_date=build_real_curve_for_obs_date,
                    ntnb_meta_df=ntnb_meta_df,
                    ntnb_ya_df=ntnb_ya_df,
                    wla_yield_func_for_date=wla_yield_for_date,
                )
            else:
                corp_bonds, skipped = compute_spreads(
                    corp_base, yields_ts, yc_table, obs_windows, tenors
                )

            print_fn(f"🧮 Spreads calculados ({tipo.upper()}): {len(corp_bonds)} | Ignorados: {len(skipped)}")

            corp_bonds = anomaly_filtering_results(corp_bonds)
            print_fn(f"🧼 Após remover anomalias: {len(corp_bonds)}")

            df_excel = corp_bonds[
                ["id", "OBS_DATE", "YAS_BOND_YLD", "DI_YIELD", "SPREAD"]
            ].copy()
            df_excel.columns = [
                "Bond ID",
                "Obs Date",
                "Corp Yield (%)",
                "DI Yield (%)",
                "Spread (bp)",
            ]
            df_excel.to_excel(f"data/corp_bonds_{tipo}_summary.xlsx", index=False)

    # ============================================================
    # GOVERNMENT BONDS
    # ============================================================
    govt_base_raw = load_govt_bond_data(CONFIG["GOVT_PATH"])

    govt_universes = {
        "ltn": {
            "yields_ts": load_yield_surface(CONFIG["GOVT_YA_PATH"]),
            "use_real_curve": False,
            "tenors": CONFIG["TENORS"],
            "inflation_linked": "N",
            "bond_type": "LTN",
        },
        "di": {
            "yields_ts": load_yield_surface(CONFIG["GOVT_YA_PATH"]),
            "use_real_curve": False,
            "tenors": CONFIG["TENORS"],
            "inflation_linked": "N",
            "bond_type": "NTNF",
        },
        "ipca": {
            "yields_ts": load_yield_surface(CONFIG["GOVT_YA_PATH"]),
            "use_real_curve": True,
            "tenors": REAL_CURVE_TENORS,
            "inflation_linked": "Y",
            "bond_type": "NTNB",
        },
    }

    for tipo, params in govt_universes.items():
        log_path = f"data/govt_logs_{tipo}.txt"
        with open(log_path, "w", encoding="utf-8") as log_file:

            def print_fn(*args, **kwargs):
                print(*args, **kwargs)
                print(*args, **kwargs, file=log_file)

            print_fn(f"\n📊 Processando universo GOVT: {tipo.upper()}")

            yields_ts = params["yields_ts"]
            tenors = params["tenors"]
            use_real_curve = params["use_real_curve"]
            inflation_linked = params["inflation_linked"]

            govt_base = govt_base_raw.copy()
            govt_base = govt_base[govt_base["id"].isin(yields_ts.columns)]

            govt_base = filter_government_universe(
                govt_base,
                inflation_linked=inflation_linked,
                bond_type=params.get("bond_type"),
                log=log_file,
            )
            print_fn(f"🧮 Bonds disponíveis após filtro ({tipo}): {len(govt_base)}")

            if tipo == "ltn":
                yc_table = interpolate_di_surface(load_di_surface(CONFIG["HIST_CURVE_PATH"]), tenors)
                if govt_base.empty:
                    print_fn("⚠️ Nenhum bond LTN encontrado.")
                    continue

                govt_bonds_list = []
                for bond_id in govt_base["id"].unique():
                    if bond_id not in yields_ts.columns:
                        continue
                    subset = pd.DataFrame({
                        "id": bond_id,
                        "OBS_DATE": yields_ts.index,
                        "YAS_BOND_YLD": yields_ts[bond_id],
                    })
                    subset = subset.merge(govt_base[["id", "MATURITY"]], on="id", how="left")
                    govt_bonds_list.append(subset)

                govt_bonds_expanded = pd.concat(govt_bonds_list, ignore_index=True)
                govt_bonds = compute_spreads_ltn(govt_bonds_expanded, yc_table)
                govt_bonds = anomaly_filtering_results(govt_bonds, is_ltn=True)
                df_excel = govt_bonds[
                    ["id", "OBS_DATE", "YAS_BOND_YLD", "DI_YIELD", "SPREAD"]
                ].copy()
                df_excel.columns = ["Bond ID", "Obs Date", "Govt Yield (%)", "DI Yield (%)", "Spread (bp)"]
                df_excel.to_excel("data/govt_bonds_ltn_summary.xlsx", index=False)
                continue

            # build observation windows
            obs_windows = build_observation_windows(govt_base, yields_ts, CONFIG["OBS_WINDOW"])

            # BUILD SURFACE
            if use_real_curve:
                surface_list = []
                for obs_date in yields_ts.index:
                    real_curve = build_real_curve_for_obs_date(obs_date, ntnb_meta_df, ntnb_ya_df)
                    if real_curve is None:
                        continue

                    for label, t in REAL_CURVE_TENORS.items():
                        surface_list.append({
                            "obs_date": obs_date,
                            "generic_ticker_id": label,
                            "yield": real_curve.yield_at(t),
                            "tenor": t,
                        })

                surface = pd.DataFrame(surface_list)
                yc_table = interpolate_surface(surface, tenors)

                govt_bonds, skipped = compute_spreads(
                    govt_base,
                    yields_ts,
                    yc_table,
                    obs_windows,
                    tenors,
                    build_real_curve_for_date=build_real_curve_for_obs_date,
                    ntnb_meta_df=ntnb_meta_df,
                    ntnb_ya_df=ntnb_ya_df,
                    wla_yield_func_for_date=wla_yield_for_date,
                )
            else:
                surface = load_di_surface(CONFIG["HIST_CURVE_PATH"])
                yc_table = interpolate_di_surface(surface, tenors)
                govt_bonds, skipped = compute_spreads(govt_base, yields_ts, yc_table, obs_windows, tenors)

            govt_bonds = anomaly_filtering_results(govt_bonds)
            df_excel = govt_bonds[
                ["id", "OBS_DATE", "YAS_BASE_YLD", "DI_YIELD", "SPREAD"]
            ].copy()
            df_excel.columns = ["Bond ID", "Obs Date", "Govt Yield (%)", "DI Yield (%)", "Spread (bp)"]
            df_excel.to_excel(f"data/govt_bonds_{tipo}_summary.xlsx", index=False)

    # ============================================================
    # FINAL BENCHMARK MERGE
    # ============================================================
    df_di = pd.read_excel("data/govt_bonds_di_summary.xlsx")[["Bond ID", "Obs Date", "Govt Yield (%)", "DI Yield (%)", "Spread (bp)"]]
    df_ipca = pd.read_excel("data/govt_bonds_ipca_summary.xlsx")[["Bond ID", "Obs Date", "Govt Yield (%)", "DI Yield (%)", "Spread (bp)"]]

    df_ltn = pd.read_excel("data/govt_bonds_ltn_summary.xlsx")[["Bond ID", "Obs Date", "Govt Yield (%)", "DI Yield (%)", "Spread (bp)"]]
    df_ltn["TYPE"] = "LTN"

    df_di["TYPE"] = "NTNF"
    df_ipca["TYPE"] = "NTNB"

    govt_all = pd.concat([df_ltn, df_di, df_ipca], axis=0, ignore_index=True)

    cols = ["id", "MATURITY"]
    govt_data = load_govt_bond_data(CONFIG["GOVT_PATH"])[cols].copy()
    govt_all = govt_all.merge(govt_data, left_on="Bond ID", right_on="id", how="left").drop(columns="id")

    govt_all["Maturity"] = govt_all["MATURITY"]
    govt_all.drop(columns=["MATURITY"], inplace=True)

    DAYCOUNT = DayCounts("bus/252", calendar="cdr_anbima")

    def calc_days_to_maturity(obs_date, maturity):
        try:
            return DAYCOUNT.days(obs_date, maturity) / 252
        except:
            return None

    govt_all["Days to Maturity"] = govt_all.apply(
        lambda r: calc_days_to_maturity(r["Obs Date"], r["Maturity"]),
        axis=1,
    )

    govt_all.to_excel("data/govt_bonds_all_consolidated.xlsx", index=False)

    benchmarks = govt_all[["Bond ID", "TYPE", "Maturity", "Days to Maturity"]].drop_duplicates()
    benchmarks.to_excel("data/govt_benchmark_summary_table.xlsx", index=False)

    html_output = show_benchmark_table(benchmarks)
    with open("templates/govt_benchmark_summary_table.html", "w", encoding="utf-8") as f:
        f.write(html_output)
