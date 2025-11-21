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

import pandas as pd
import os


if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    os.makedirs("templates", exist_ok=True)

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
        },
        "ipca": {
            "yields_ts": load_yield_surface(CONFIG["YA_PATH"]),
            "surface": load_ipca_surface(CONFIG["WLA_CURVE_PATH"]),
            "tenors": CONFIG["WLA_TENORS"],
            "inflation_linked": "Y",
        },
    }

    for tipo, params in universes.items():
        log_path = f"data/logs_{tipo}.txt"
        with open(log_path, "w", encoding="utf-8") as log_file:

            def print_fn(*args, **kwargs):
                print(*args, **kwargs)
                print(*args, **kwargs, file=log_file)

            print_fn(f"\n📊 Processando universo: {tipo.upper()}")

            surface = params["surface"]
            tenors = params["tenors"]
            yields_ts = params["yields_ts"]
            inflation_linked = params["inflation_linked"]

            corp_base = corp_base_raw.copy()
            corp_base = corp_base[corp_base["id"].isin(yields_ts.columns)]

            corp_base = filter_corporate_universe(
                corp_base,
                inflation_linked=inflation_linked,
                log=log_file
            )

            print_fn(f"🧮 Bonds disponíveis após filtro ({tipo}): {len(corp_base)}")

            obs_windows = build_observation_windows(corp_base, yields_ts, CONFIG["OBS_WINDOW"])

            yc_table = (
                interpolate_di_surface(surface, tenors)
                if tipo == "di"
                else interpolate_surface(surface, tenors)
            )

            corp_bonds, skipped = compute_spreads(corp_base, yields_ts, yc_table, obs_windows, tenors)
            print_fn(f"🧮 Spreads calculados ({tipo.upper()}): {len(corp_bonds)} | Ignorados: {len(skipped)}")

            corp_bonds = anomaly_filtering_results(corp_bonds)
            print_fn(f"🧼 Após remover anomalias: {len(corp_bonds)}")

            df_excel = corp_bonds[
                ["id", "OBS_DATE", "YAS_BOND_YLD", "TENOR_YRS", "DI_YIELD", "SPREAD"]
            ].copy()
            df_excel.columns = ["Bond ID", "Obs Date", "Corp Yield (%)", "Tenor (yrs)", "DI Yield (%)", "Spread (bp)"]
            df_excel.to_excel(f"data/corp_bonds_{tipo}_summary.xlsx", index=False)

    # ============================================================
    # GOVERNMENT BONDS
    # ============================================================
    govt_base_raw = load_govt_bond_data(CONFIG["GOVT_PATH"])

    govt_universes = {
        "ltn": {
            "yields_ts": load_yield_surface(CONFIG["GOVT_YA_PATH"]),
            "surface": load_di_surface(CONFIG["HIST_CURVE_PATH"]),
            "tenors": CONFIG["TENORS"],
            "inflation_linked": "N",
            "bond_type": "LTN",
        },
        "di": {
            "yields_ts": load_yield_surface(CONFIG["GOVT_YA_PATH"]),
            "surface": load_di_surface(CONFIG["HIST_CURVE_PATH"]),
            "tenors": CONFIG["TENORS"],
            "inflation_linked": "N",
            "bond_type": "NTNF",
        },
        "ipca": {
            "yields_ts": load_yield_surface(CONFIG["GOVT_YA_PATH"]),
            "surface": load_ipca_surface(CONFIG["WLA_CURVE_PATH"]),
            "tenors": CONFIG["WLA_TENORS"],
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

            surface = params["surface"]
            tenors = params["tenors"]
            yields_ts = params["yields_ts"]
            inflation_linked = params["inflation_linked"]

            govt_base = govt_base_raw.copy()
            govt_base = govt_base[govt_base["id"].isin(yields_ts.columns)]

            govt_base = filter_government_universe(
                govt_base,
                inflation_linked=inflation_linked,
                bond_type=params.get("bond_type"),
                log=log_file
            )

            print_fn(f"🧮 Bonds disponíveis após filtro ({tipo}): {len(govt_base)}")

            if tipo == "ltn":
                print_fn("⚙️ Calculando spreads diretos para LTNs (zero-coupon, sem bootstrapping)...")
                yc_table = interpolate_di_surface(surface, tenors)
                if govt_base.empty:
                    print_fn("⚠️ Nenhum bond LTN encontrado após o filtro.")
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

                if not govt_bonds_list:
                    print_fn("⚠️ Nenhum yield disponível para LTNs — nada a calcular.")
                    continue

                govt_bonds_expanded = pd.concat(govt_bonds_list, ignore_index=True)
                govt_bonds = compute_spreads_ltn(govt_bonds_expanded, yc_table)
                govt_bonds = anomaly_filtering_results(govt_bonds, is_ltn=True)
                print_fn(f"🧼 Após remover anomalias (LTN): {len(govt_bonds)}")

                df_excel = govt_bonds[
                    ["id", "OBS_DATE", "YAS_BOND_YLD", "TENOR_YRS", "DI_YIELD", "SPREAD"]
                ].copy()
                df_excel.columns = ["Bond ID", "Obs Date", "Govt Yield (%)", "Tenor (yrs)", "DI Yield (%)", "Spread (bp)"]
                df_excel.to_excel("data/govt_bonds_ltn_summary.xlsx", index=False)

                print_fn("✅ LTNs processadas com sucesso (sem bootstrapping).")
                continue

            obs_windows = build_observation_windows(govt_base, yields_ts, CONFIG["OBS_WINDOW"])
            yc_table = (
                interpolate_di_surface(surface, tenors)
                if tipo == "di"
                else interpolate_surface(surface, tenors)
            )

            govt_bonds, skipped = compute_spreads(govt_base, yields_ts, yc_table, obs_windows, tenors)
            print_fn(f"🧮 Spreads calculados ({tipo.upper()}): {len(govt_bonds)} | Ignorados: {len(skipped)}")
            govt_bonds = anomaly_filtering_results(govt_bonds)
            print_fn(f"🧼 Após remover anomalias: {len(govt_bonds)}")

            df_excel = govt_bonds[
                ["id", "OBS_DATE", "YAS_BOND_YLD", "TENOR_YRS", "DI_YIELD", "SPREAD"]
            ].copy()
            df_excel.columns = ["Bond ID", "Obs Date", "Govt Yield (%)", "Tenor (yrs)", "DI Yield (%)", "Spread (bp)"]
            df_excel.to_excel(f"data/govt_bonds_{tipo}_summary.xlsx", index=False)

    # ============================================================
    # MERGE FINAL DE BENCHMARKS GOVERNAMENTAIS + CONSOLIDADO
    # ============================================================
    import os

    df_di = pd.read_excel("data/govt_bonds_di_summary.xlsx")[["Bond ID", "Obs Date", "Govt Yield (%)", "Tenor (yrs)", "DI Yield (%)", "Spread (bp)"]]
    df_ipca = pd.read_excel("data/govt_bonds_ipca_summary.xlsx")[["Bond ID", "Obs Date", "Govt Yield (%)", "Tenor (yrs)", "DI Yield (%)", "Spread (bp)"]]

    if os.path.exists("data/govt_bonds_ltn_summary.xlsx"):
        df_ltn = pd.read_excel("data/govt_bonds_ltn_summary.xlsx")[["Bond ID", "Obs Date", "Govt Yield (%)", "Tenor (yrs)", "DI Yield (%)", "Spread (bp)"]]
        df_ltn["TYPE"] = "LTN"
    else:
        print("⚠️ Nenhum arquivo govt_bonds_ltn_summary.xlsx encontrado.")
        df_ltn = pd.DataFrame(columns=["Bond ID", "Obs Date", "Govt Yield (%)", "Tenor (yrs)", "DI Yield (%)", "Spread (bp)", "TYPE"])

    df_di["TYPE"] = "NTNF"
    df_ipca["TYPE"] = "NTNB"

    govt_all = pd.concat([df_ltn, df_di, df_ipca], axis=0, ignore_index=True)

    print(f"📊 Contagem de linhas — LTN: {len(df_ltn)}, DI: {len(df_di)}, IPCA: {len(df_ipca)}, TOTAL: {len(govt_all)}")

    cols = ["id", "ISSUER", "MATURITY"]
    govt_data = load_govt_bond_data(CONFIG["GOVT_PATH"])[cols].copy()
    govt_all = govt_all.merge(govt_data, left_on="Bond ID", right_on="id", how="left").drop(columns="id")
    govt_all["Issuer"] = govt_all["ISSUER"]
    govt_all["Maturity"] = govt_all["MATURITY"]
    govt_all.drop(columns=["ISSUER", "MATURITY"], inplace=True)

    DAYCOUNT = DayCounts("bus/252", calendar="cdr_anbima")

    # ✅ Calculate Days to Maturity (bus/252 years × 252 business days)
    govt_all["Days to Maturity"] = govt_all.apply(
        lambda r: int(DAYCOUNT.days(r["Obs Date"], r["Maturity"]))
        if pd.notna(r["Obs Date"]) and pd.notna(r["Maturity"])
        else None,
        axis=1
    )

    # ✅ Remove 'Issuer', keep 'Maturity' and add 'Days to Maturity'
    govt_all = govt_all[[
        "TYPE", "Bond ID", "Obs Date", "Govt Yield (%)", "DI Yield (%)", "Spread (bp)", "Maturity", "Days to Maturity"
    ]]
    govt_all.sort_values(by=["TYPE", "Obs Date"], inplace=True)


    govt_all.to_excel("data/govt_bonds_all_consolidated.xlsx", index=False)
    print(f"✅ govt_bonds_all_consolidated.xlsx gerado com {len(govt_all)} linhas.")

    benchmarks = govt_all[["Bond ID", "TYPE", "Issuer", "Maturity"]].drop_duplicates()
    benchmarks.rename(columns={"TYPE": "Benchmark"}, inplace=True)
    benchmarks.to_excel("data/govt_benchmark_summary_table.xlsx", index=False)

    html_output = show_benchmark_table(benchmarks)
    with open("templates/govt_benchmark_summary_table.html", "w", encoding="utf-8") as f:
        f.write(html_output)

    print(f"✅ govt_benchmark_summary_table.html gerado com sucesso (total: {len(benchmarks)} títulos).")
