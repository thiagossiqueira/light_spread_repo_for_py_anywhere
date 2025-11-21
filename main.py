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

            df_vis = yc_table[
                [k for k, _ in sorted(tenors.items(), key=lambda x: x[1]) if k in yc_table.columns]
            ]
            df_vis.index.name = "obs_date"

            # Curva interpolada
            surface_fig = plot_yield_curve_surface(
                df_vis,
                source_text=f"Source: {'DI' if tipo == 'di' else 'WLA'} B3 – cálculos próprios"
            )
            surface_fig.write_html(f"templates/{tipo}_surface.html")

            # Tabela resumo
            table_func = show_di_summary_table if tipo == "di" else show_ipca_summary_table
            summary_fig = table_func(df_vis)

            if summary_fig is not None:
                title = (
                    "Bond Yield vs DI Interpolated Yield and Spread Summary"
                    if tipo == "di"
                    else "Bond Yield vs IPCA Interpolated Yield and Spread Summary"
                )
                path = f"templates/{tipo.lower()}_summary_table.html"
                summary_fig.update_layout(title_text=title)
                summary_fig.write_html(path, include_plotlyjs="cdn", full_html=True)
                print_fn(f"✅ summary_{tipo.upper()}_table.html salvo com sucesso.")
            else:
                print_fn(f"⚠️ {tipo}_summary_table.html não foi gerado.")

            # Cálculo dos spreads corporativos
            corp_bonds, skipped = compute_spreads(corp_base, yields_ts, yc_table, obs_windows, tenors)
            print_fn(f"🧮 Spreads calculados ({tipo.upper()}): {len(corp_bonds)} | Ignorados: {len(skipped)}")

            corp_bonds = anomaly_filtering_results(corp_bonds)
            print_fn(f"🧼 Após remover anomalias: {len(corp_bonds)}")

            # Export Excel
            df_excel = corp_bonds[
                ["id", "OBS_DATE", "YAS_BOND_YLD", "TENOR_YRS", "DI_YIELD", "SPREAD"]
            ].copy()
            df_excel.columns = ["Bond ID", "Obs Date", "Corp Yield (%)", "Tenor (yrs)", "DI Yield (%)", "Spread (bp)"]
            df_excel.to_excel(f"data/corp_bonds_{tipo}_summary.xlsx", index=False)

            # Superfície de spreads
            spread_surface = corp_bonds.pivot_table(
                index="OBS_DATE",
                columns="TENOR_BUCKET",
                values="SPREAD",
                aggfunc="mean"
            ).sort_index()

            tenor_order = sorted(tenors.items(), key=lambda x: x[1])
            ordered_cols = [k for k, _ in tenor_order if k in spread_surface.columns]
            spread_surface = spread_surface[ordered_cols]

            fig = plot_surface_spread_with_bonds(
                df_surface=spread_surface,
                audit=corp_bonds,
                title=f"Corporate vs. {'DI' if tipo == 'di' else 'IPCA'} Spread Surface (Filtered Universe)",
                zmin=-200,
                zmax=2000,
            )
            fig.write_html(f"templates/{tipo}_spread_surface.html")

            # Tabela resumo geral
            table_fig = show_summary_table(corp_bonds)
            if table_fig is not None:
                table_fig.write_html(f"templates/summary_{tipo.upper()}_table.html")

            # Observações ignoradas
            pd.DataFrame(skipped, columns=["Bond ID", "Obs Date", "Reason"]).to_csv(
                f"data/skipped_{tipo}_yields.csv", index=False
            )

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

            # Caso especial: LTNs (zero-coupon)
            if tipo == "ltn":
                print_fn("⚙️ Calculando spreads diretos para LTNs (zero-coupon, sem bootstrapping)...")

                yc_table = interpolate_di_surface(surface, tenors)

                # Expandir govt_base com observações (OBS_DATE, yield)
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

                govt_bonds = anomaly_filtering_results(govt_bonds)
                print_fn(f"🧼 Após remover anomalias (LTN): {len(govt_bonds)}")

                df_excel = govt_bonds[
                    ["id", "OBS_DATE", "YAS_BOND_YLD", "TENOR_YRS", "DI_YIELD", "SPREAD"]
                ].copy()
                df_excel.columns = ["Bond ID", "Obs Date", "Govt Yield (%)", "Tenor (yrs)", "DI Yield (%)", "Spread (bp)"]
                df_excel.to_excel("data/govt_bonds_ltn_summary.xlsx", index=False)

                print_fn("✅ LTNs processadas com sucesso (sem bootstrapping).")
                continue

            # NTNF / NTNB – fluxo normal
            obs_windows = build_observation_windows(govt_base, yields_ts, CONFIG["OBS_WINDOW"])

            yc_table = (
                interpolate_di_surface(surface, tenors)
                if tipo == "di"
                else interpolate_surface(surface, tenors)
            )

            df_vis = yc_table[
                [k for k, _ in sorted(tenors.items(), key=lambda x: x[1]) if k in yc_table.columns]
            ]
            df_vis.index.name = "obs_date"

            surface_fig = plot_yield_curve_surface(
                df_vis,
                source_text=f"Source: {'DI' if tipo == 'di' else 'WLA'} B3 – cálculos próprios"
            )
            surface_fig.write_html(f"templates/govt_{tipo}_surface.html")

            table_func = show_di_summary_table if tipo == "di" else show_ipca_summary_table
            summary_fig = table_func(df_vis)

            if summary_fig is not None:
                title = (
                    "Sovereign Yield vs DI Interpolated Yield and Spread Summary"
                    if tipo == "di"
                    else "Sovereign Yield vs IPCA Interpolated Yield and Spread Summary"
                )
                path = f"templates/govt_{tipo.lower()}_summary_table.html"
                summary_fig.update_layout(title_text=title)
                summary_fig.write_html(path, include_plotlyjs="cdn", full_html=True)
                print_fn(f"✅ govt_summary_{tipo.upper()}_table.html salvo com sucesso.")
            else:
                print_fn(f"⚠️ govt_summary_{tipo}_table.html não foi gerado.")

            govt_bonds, skipped = compute_spreads(govt_base, yields_ts, yc_table, obs_windows, tenors)
            print_fn(f"🧮 Spreads calculados ({tipo.upper()}): {len(govt_bonds)} | Ignorados: {len(skipped)}")

            govt_bonds = anomaly_filtering_results(govt_bonds)
            print_fn(f"🧼 Após remover anomalias: {len(govt_bonds)}")

            df_excel = govt_bonds[
                ["id", "OBS_DATE", "YAS_BOND_YLD", "TENOR_YRS", "DI_YIELD", "SPREAD"]
            ].copy()
            df_excel.columns = ["Bond ID", "Obs Date", "Govt Yield (%)", "Tenor (yrs)", "DI Yield (%)", "Spread (bp)"]
            df_excel.to_excel(f"data/govt_bonds_{tipo}_summary.xlsx", index=False)

            spread_surface = govt_bonds.pivot_table(
                index="OBS_DATE", columns="TENOR_BUCKET", values="SPREAD", aggfunc="mean"
            ).sort_index()

            tenor_order = sorted(tenors.items(), key=lambda x: x[1])
            ordered_cols = [k for k, _ in tenor_order if k in spread_surface.columns]
            spread_surface = spread_surface[ordered_cols]

            fig = plot_surface_spread_with_bonds(
                df_surface=spread_surface,
                audit=govt_bonds,
                title=f"Sovereign vs. {'DI' if tipo == 'di' else 'IPCA'} Spread Surface (Filtered Universe)",
                zmin=-200,
                zmax=2000,
            )
            fig.write_html(f"templates/govt_{tipo}_spread_surface.html")

            table_fig = show_summary_table(govt_bonds)
            if table_fig is not None:
                table_fig.write_html(f"templates/govt_summary_{tipo.upper()}_table.html")

            pd.DataFrame(skipped, columns=["Bond ID", "Obs Date", "Reason"]).to_csv(
                f"data/govt_skipped_{tipo}_yields.csv", index=False
            )

    # ============================================================
    # MERGE FINAL DE BENCHMARKS GOVERNAMENTAIS + CONSOLIDADO
    # ============================================================
    import os

    df_di = pd.read_excel("data/govt_bonds_di_summary.xlsx")[["Bond ID", "OBS_DATE", "YAS_BOND_YLD", "TENOR_YRS", "DI_YIELD", "SPREAD"]].copy()
    df_ipca = pd.read_excel("data/govt_bonds_ipca_summary.xlsx")[["Bond ID", "OBS_DATE", "YAS_BOND_YLD", "TENOR_YRS", "DI_YIELD", "SPREAD"]].copy()

    if os.path.exists("data/govt_bonds_ltn_summary.xlsx"):
        df_ltn = pd.read_excel("data/govt_bonds_ltn_summary.xlsx")[["Bond ID", "OBS_DATE", "YAS_BOND_YLD", "TENOR_YRS", "DI_YIELD", "SPREAD"]].copy()
        df_ltn["TYPE"] = "LTN"
    else:
        print("⚠️ Nenhum arquivo govt_bonds_ltn_summary.xlsx encontrado.")
        df_ltn = pd.DataFrame(columns=["Bond ID", "OBS_DATE", "YAS_BOND_YLD", "TENOR_YRS", "DI_YIELD", "SPREAD", "TYPE"])

    df_di["TYPE"] = "NTNF"
    df_ipca["TYPE"] = "NTNB"

    govt_all = pd.concat([df_ltn, df_di, df_ipca], axis=0, ignore_index=True)
    govt_all = govt_all.drop_duplicates(subset=["Bond ID", "OBS_DATE"])

    govt_all.rename(columns={
        "OBS_DATE": "Obs Date",
        "YAS_BOND_YLD": "Govt Yield (%)",
        "TENOR_YRS": "Tenor (yrs)",
        "DI_YIELD": "DI Yield (%)",
        "SPREAD": "Spread (bp)"
    }, inplace=True)

    cols = ["id", "ISSUER", "MATURITY"]
    govt_data = load_govt_bond_data(CONFIG["GOVT_PATH"])[cols].copy()
    govt_all = govt_all.merge(govt_data, left_on="Bond ID", right_on="id", how="left").drop(columns="id")
    govt_all["Issuer"] = govt_all["ISSUER"]
    govt_all["Maturity"] = govt_all["MATURITY"]
    govt_all.drop(columns=["ISSUER", "MATURITY"], inplace=True)

    govt_all = govt_all[["TYPE", "Bond ID", "Obs Date", "Govt Yield (%)", "Tenor (yrs)", "DI Yield (%)", "Spread (bp)", "Issuer", "Maturity"]]
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