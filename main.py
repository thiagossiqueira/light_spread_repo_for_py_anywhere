# main.py

import os
import pandas as pd
import sys
sys.stdout.reconfigure(encoding="utf-8")

from tqdm import tqdm
from calendars.daycounts import DayCounts

from src.config import CONFIG

# Loaders
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
)
from src.core.windowing import build_observation_windows
from src.core.spread_calculator import compute_spreads, compute_spreads_ltn

# REAL CURVE modules
from src.core.curve_builder import (
    load_real_curve_support,
    build_real_curve_for_obs_date,
)

# --------------------------------------------------------
# Helper
# --------------------------------------------------------
def remove_unnamed(df):
    return df.loc[:, ~df.columns.str.contains("^Unnamed")]


# =====================================================================
#  1. INÍCIO
# =====================================================================
if __name__ == "__main__":

    os.makedirs("data", exist_ok=True)
    os.makedirs("templates", exist_ok=True)

    print("\n=== LOADING REAL CURVE SUPPORT ===")
    ntnb_meta_df, ntnb_ya_df = load_real_curve_support()

    # =====================================================================
    #  2. CORPORATE PROCESSING (DI + IPCA)
    # =====================================================================

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
            "tenors": CONFIG["REAL_CURVE_TENORS"],
            "inflation_linked": "Y",
            "use_real_curve": True,
        },
    }

    real_surface_corp = []

    for tipo, params in universes.items():

        print(f"\n=== CORPORATE: {tipo.upper()} ===")

        yields_ts = params["yields_ts"]
        tenors = params["tenors"]
        inflation_linked = params["inflation_linked"]
        use_real_curve = params["use_real_curve"]

        corp_base = corp_base_raw.copy()
        corp_base = corp_base[corp_base["id"].isin(yields_ts.columns)]
        corp_base = filter_corporate_universe(
            corp_base, inflation_linked=inflation_linked, log=None
        )

        obs_windows = build_observation_windows(
            corp_base, yields_ts, CONFIG["OBS_WINDOW"]
        )

        # Normal DI
        if tipo == "di":
            surface = params["surface"]
            yc_table = interpolate_di_surface(surface, tenors)

        else:
            # === REAL CURVE CORPORATE (WLA + NTNB) ===
            surface_list = []
            common_dates = yields_ts.index.intersection(ntnb_ya_df.index)

            real_curve_cache = {}

            for obs_date in tqdm(common_dates, desc="Corporate Real Curve"):

                if obs_date not in real_curve_cache:
                    real_curve_cache[obs_date] = build_real_curve_for_obs_date(
                        obs_date, ntnb_meta_df, ntnb_ya_df
                    )
                real_curve = real_curve_cache[obs_date]

                if real_curve is None:
                    continue

                for label, t in CONFIG["REAL_CURVE_TENORS"].items():
                    surface_list.append(
                        {
                            "obs_date": obs_date,
                            "tenor": t,
                            "generic_ticker_id": label,
                            "yield": real_curve.yield_at(t),
                        }
                    )

            surface = pd.DataFrame(surface_list)
            real_surface_corp.append(surface)

            if surface.empty:
                raise ValueError("Surface corporate real is empty.")

            yc_table = interpolate_surface(surface, tenors)

        corp_bonds, skipped = compute_spreads(
            corp_base, yields_ts, yc_table, obs_windows, tenors
        )
        corp_bonds = anomaly_filtering_results(corp_bonds)

        df_excel = corp_bonds[
            ["id", "OBS_DATE", "YAS_BOND_YLD", "DI_YIELD", "SPREAD"]
        ].copy()
        df_excel.columns = [
            "Bond ID", "Obs Date", "Corp Yield (%)", "DI Yield (%)", "Spread (bp)"
        ]

        df_excel = remove_unnamed(df_excel)
        df_excel.to_excel(f"data/corp_bonds_{tipo}_summary.xlsx", index=False)

    if real_surface_corp:
        df_real = pd.concat(real_surface_corp, ignore_index=True)
        df_real.to_excel("data/real_curve_surface_corp.xlsx", index=False)

    # =====================================================================
    #  3. GOVERNMENT PROCESSING (LTN, NTNF, NTNB)
    # =====================================================================

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
            "tenors": CONFIG["REAL_CURVE_TENORS"],
            "inflation_linked": "Y",
            "bond_type": "NTNB",
        },
    }

    real_surface_govt = []

    for tipo, params in govt_universes.items():

        print(f"\n=== GOVERNMENT: {tipo.upper()} ===")

        yields_ts = params["yields_ts"]
        tenors = params["tenors"]
        inflation_linked = params["inflation_linked"]
        use_real_curve = params["use_real_curve"]

        govt_base = govt_base_raw.copy()
        govt_base = govt_base[govt_base["id"].isin(yields_ts.columns)]
        govt_base = filter_government_universe(
            govt_base, inflation_linked=inflation_linked, bond_type=params["bond_type"], log=None
        )

        if tipo == "ltn":
            yc_table = interpolate_di_surface(
                load_di_surface(CONFIG["HIST_CURVE_PATH"]), tenors
            )

            # build expanded (no duration window)
            govt_list = []
            for bid in govt_base["id"]:
                if bid in yields_ts.columns:
                    subset = pd.DataFrame(
                        {
                            "id": bid,
                            "OBS_DATE": yields_ts.index,
                            "YAS_BOND_YLD": yields_ts[bid],
                        }
                    )
                    subset = subset.merge(
                        govt_base[["id", "MATURITY"]], on="id", how="left"
                    )
                    govt_list.append(subset)

            if govt_list:
                merged = pd.concat(govt_list, ignore_index=True)
                govt_bonds = compute_spreads_ltn(merged, yc_table)
                govt_bonds = anomaly_filtering_results(govt_bonds, is_ltn=True)

                govt_bonds.to_excel("data/govt_bonds_ltn_summary.xlsx", index=False)
            continue

        # NTNB (REAL CURVE)
        if tipo == "ipca":

            surface_list = []
            common_dates = yields_ts.index.intersection(ntnb_ya_df.index)

            real_curve_cache = {}

            for obs_date in tqdm(common_dates, desc="Government Real Curve"):

                if obs_date not in real_curve_cache:
                    real_curve_cache[obs_date] = build_real_curve_for_obs_date(
                        obs_date, ntnb_meta_df, ntnb_ya_df
                    )
                real_curve = real_curve_cache[obs_date]

                if real_curve is None:
                    continue

                for label, t in CONFIG["REAL_CURVE_TENORS"].items():
                    surface_list.append(
                        {
                            "obs_date": obs_date,
                            "tenor": t,
                            "generic_ticker_id": label,
                            "yield": real_curve.yield_at(t),
                        }
                    )

            surface = pd.DataFrame(surface_list)
            real_surface_govt.append(surface)

            if surface.empty:
                raise ValueError("Government real curve is empty.")

            yc_table = interpolate_surface(surface, tenors)

            govt_bonds, skipped = compute_spreads(
                govt_base, yields_ts, yc_table,
                build_observation_windows(govt_base, yields_ts, CONFIG["OBS_WINDOW"]),
                tenors,
            )
            govt_bonds = anomaly_filtering_results(govt_bonds)
            govt_bonds.to_excel("data/govt_bonds_ipca_summary.xlsx", index=False)
            continue

        # DI NTNF
        yc_table = interpolate_di_surface(load_di_surface(CONFIG["HIST_CURVE_PATH"]), tenors)
        govt_bonds, skipped = compute_spreads(
            govt_base, yields_ts, yc_table,
            build_observation_windows(govt_base, yields_ts, CONFIG["OBS_WINDOW"]),
            tenors,
        )
        govt_bonds = anomaly_filtering_results(govt_bonds)
        govt_bonds.to_excel(f"data/govt_bonds_{tipo}_summary.xlsx", index=False)

    # SAVE GOVT SURFACE
    if real_surface_govt:
        df_g = pd.concat(real_surface_govt, ignore_index=True)
        df_g.to_excel("data/real_curve_surface_govt.xlsx", index=False)


    # =====================================================================
    # BUILD REAL IPCA WLA+NTNB SURFACE 3D (igual CDS)
    # =====================================================================

    def build_and_save_real_ipca_surface(
        govt_surface_path="data/real_curve_surface_govt.xlsx",
        html_out="templates/wla_ntnb_surface.html",
        xlsx_out="data/real_curve_surface_all.xlsx",
        summary_out="templates/wla_ntnb_summary.html",
    ):
        if not os.path.exists(govt_surface_path):
            print("No govt surface available.")
            return

        df = pd.read_excel(govt_surface_path)
        df["obs_date"] = pd.to_datetime(df["obs_date"], errors="coerce")
        df["tenor"] = pd.to_numeric(df["tenor"], errors="coerce")
        df["yield"] = pd.to_numeric(df["yield"], errors="coerce")
        df = df.dropna(subset=["obs_date", "tenor", "yield"])

        surface = (
            df.pivot_table(
                index="obs_date",
                columns="tenor",
                values="yield",
                aggfunc="mean",
            ).sort_index()
        )

        surface = surface.reindex(sorted(surface.columns), axis=1)

        surface.to_excel(xlsx_out)
        summary_html = surface.tail(40).round(4).to_html(
            border=0, classes="table table-striped"
        )
        with open(summary_out, "w", encoding="utf-8") as f:
            f.write(summary_html)

        fig = plot_yield_curve_surface(
            surface,
            title="Real IPCA Curve Surface (WLA + NTNB)",
            zmin=0,
            zmax=10,
        )
        fig.write_html(html_out)

    build_and_save_real_ipca_surface()

    print("\n=== DONE ===")
