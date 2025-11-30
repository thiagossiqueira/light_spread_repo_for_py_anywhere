from flask import Flask, render_template, send_file
from routes.filters_routes import filters_blueprint
import pandas as pd
import os


from datetime import datetime
from markupsafe import Markup
from src.config import CONFIG


app = Flask(__name__, template_folder="templates")
app.register_blueprint(filters_blueprint)


# ----------- PÁGINA INICIAL ------------------------
@app.route("/")
def index():
    logs_di = logs_ipca = ""
    try:
        with open("data/logs_di.txt", "r", encoding="utf-8") as f:
            logs_di = f.read()
        with open("data/logs_ipca.txt", "r", encoding="utf-8") as f:
            logs_ipca = f.read()
    except FileNotFoundError:
        logs_di = "⚠️ Logs DI não encontrados."
        logs_ipca = "⚠️ Logs IPCA não encontrados."
    return render_template("index.html", logs_di=logs_di, logs_ipca=logs_ipca)

# ----------- SPREADS SUPERFÍCIE 3D -----------------
@app.route("/spread/<prefixo>")
def spread(prefixo):
    if prefixo not in ["di", "ipca"]:
        prefixo = "di"
    return send_file(f"templates/{prefixo}_spread_surface.html")


# ----------- TABELAS DOS SPREADS ------------------
@app.route("/spread-table/<prefixo>")
def spread_table(prefixo):
    file_map = {
        "di": "summary_DI_table.html",
        "ipca": "summary_IPCA_table.html"
    }
    if prefixo not in file_map:
        prefixo = "di"
    return send_file(f"templates/{file_map[prefixo]}")


# ----------- TABELAS DAS CURVAS INTERPOLADAS ------
@app.route("/summary/<prefixo>")
def summary(prefixo):
    if prefixo == "di":
        return send_file("templates/di_summary_table.html")
    elif prefixo == "ipca":
        return send_file("templates/ipca_summary_table.html")
    else:
        return "Tipo inválido", 400

# ----------- CURVAS DI e IPCA (WLA) ----------------
@app.route("/surface/<prefixo>")
def surface(prefixo):
    if prefixo == "di":
        return send_file("templates/di_surface.html")
    elif prefixo == "ipca":
        return send_file("templates/ipca_surface.html")
    else:
        return "Tipo inválido", 400


# ----------- FULL TABLES (Opcional) ----------------
@app.route("/summary-full")
def summary_full():
    df = pd.read_excel("data/corp_bonds_summary.xlsx")
    return render_template("summary_full.html", summary_data=df.to_dict(orient="records"))


@app.route("/wla-summary-full")
def wla_summary_full():
    with open("templates/ipca_summary_table.html") as f:
        content = f.read()
    return render_template("ipca_summary_full.html", table_html=content)


# ----------- DOWNLOAD DE EXCEL ---------------------
@app.route("/download/<prefixo>")
def download(prefixo):
    if prefixo == "di":
        return send_file(
            "data/corp_bonds_di_summary.xlsx",
            download_name="corp_bonds_di_summary.xlsx",
            as_attachment=True,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    elif prefixo == "ipca":
        return send_file(
            "data/corp_bonds_ipca_summary.xlsx",
            download_name="corp_bonds_ipca_summary.xlsx",
            as_attachment=True,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    elif prefixo == "benchmark":
        return send_file(
            "data/benchmark_summary_table.xlsx",
            download_name="benchmark_summary_table.xlsx",
            as_attachment=True,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        return "Tipo inválido", 400

@app.route("/benchmark-summary")
def benchmark_summary():
    return render_template("benchmark_summary_table.html")


#check later
# 3D Surface Chart (already done for DI, do for IPCA too)
@app.route("/sov_surface/<prefixo>")
def sov_surface(prefixo):
    if prefixo not in ["di", "ipca"]:
        prefixo = "di"
    return send_file(f"templates/govt_{prefixo}_surface.html")

# Spread Charts (3D spread charts)
@app.route("/sov_spread/<prefixo>")
def sov_spread(prefixo):
    if prefixo not in ["di", "ipca"]:
        prefixo = "di"
    return send_file(f"templates/govt_{prefixo}_spread_surface.html")

# Spread Tables (HTML)
@app.route("/sov-spread-table/<prefixo>")
def sov_spread_table(prefixo):
    if prefixo not in ["di", "ipca"]:
        prefixo = "di"
    return send_file(f"templates/govt_summary_{prefixo.upper()}_table.html")

# Table Downloads (XLSX)
@app.route("/sov-download/<prefixo>")
def sov_download(prefixo):
    if prefixo not in ["di", "ipca"]:
        return "Tipo inválido", 400
    return send_file(
        f"data/govt_bonds_{prefixo}_summary.xlsx",
        download_name=f"govt_bonds_{prefixo}_summary.xlsx",
        as_attachment=True,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# Final benchmark summary
@app.route("/sov-benchmark-summary")
def sov_benchmark_summary():
    return render_template("govt_benchmark_summary_table.html")

# ✅ nova rota: download do consolidado de títulos soberanos
@app.route("/sov-download/all")
def download_govt_all():
    """
    Permite baixar o arquivo consolidado de títulos soberanos (LTN + NTNF + NTNB).
    """
    file_path = "data/govt_bonds_all_consolidated.xlsx"
    if not os.path.exists(file_path):
        return "❌ Arquivo govt_bonds_all_consolidated.xlsx não encontrado.", 404
    return send_file(
        file_path,
        download_name="govt_bonds_all_consolidated.xlsx",
        as_attachment=True,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# --- BRL synthetic risk (CDS-BRL) ---
@app.route("/brl-risk/surface")
def brl_risk_surface():
    return send_file("templates/brl_risk_spread_surface.html")

@app.route("/brl-risk/table")
def brl_risk_table():
    return send_file("templates/brl_risk_summary.html")

@app.route("/brl-risk/download")
def brl_risk_download():
    return send_file(
        "data/synthetic_cds_brl_surface.xlsx",
        download_name="synthetic_cds_brl_surface.xlsx",
        as_attachment=True,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# --- Synthetic CDS-BRL applied to corporate panel ---
@app.route("/panel-cds/download")
def panel_cds_download():
    """
    Permite descargar el panel de bonos corporativos con la columna Synthetic_CDS_BRL agregada.
    """
    file_path = "datos_y_modelos/db/output_panel_data/panel_data_with_cds.xlsx"
    try:
        return send_file(
            file_path,
            download_name="panel_data_with_cds.xlsx",
            as_attachment=True,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except FileNotFoundError:
        return "❌ Archivo panel_data_with_cds.xlsx no encontrado.", 404


@app.route("/surface/wla_ntnb")
def surface_wla_ntnb():

    corp_path = "data/real_curve_surface_corp.xlsx"
    govt_path = "data/real_curve_surface_govt.xlsx"

    if not (os.path.exists(corp_path) or os.path.exists(govt_path)):
        return "Surface WLA+NTNB ainda não foi gerada. Execute main.py.", 500

    frames = []
    if os.path.exists(corp_path):
        frames.append(pd.read_excel(corp_path))
    if os.path.exists(govt_path):
        frames.append(pd.read_excel(govt_path))

    df = pd.concat(frames, ignore_index=True)

    # pivot
    pivot = df.pivot_table(
        index="obs_date",
        columns="tenor",
        values="yield",
        aggfunc="mean"
    ).sort_index()

    # Output JSON for plotly
    surface_json = pivot.reset_index().to_dict(orient="records")

    # Separate corp/gov if desired for UI toggles
    corp_json = (
        pd.read_excel(corp_path).pivot_table(index="obs_date", columns="tenor", values="yield", aggfunc="mean")
        if os.path.exists(corp_path) else pd.DataFrame()
    )
    corp_json = corp_json.reset_index().to_dict(orient="records")

    govt_json = (
        pd.read_excel(govt_path).pivot_table(index="obs_date", columns="tenor", values="yield", aggfunc="mean")
        if os.path.exists(govt_path) else pd.DataFrame()
    )
    govt_json = govt_json.reset_index().to_dict(orient="records")

    # build curves for matching display
    wla_t = sorted([t for t in CONFIG["WLA_TENORS"].values()])
    ntnb_t = sorted([t for t in CONFIG["REAL_CURVE_TENORS"].values() if t >= 5])

    # Use last available date
    last_date = pivot.index.max()

    wla_row = pivot.loc[last_date, wla_t].tolist()
    ntnb_row = pivot.loc[last_date, ntnb_t].tolist()

    combined_t = sorted(CONFIG["REAL_CURVE_TENORS"].values())
    combined_row = pivot.loc[last_date, combined_t].tolist()

    return render_template(
        "surface_real_ipca.html",
        surface_json=surface_json,
        corp_json=corp_json,
        govt_json=govt_json,
        wla_json={"tenors": wla_t, "yields": wla_row},
        ntnb_json={"tenors": ntnb_t, "yields": ntnb_row},
        combined_json={"tenors": combined_t, "yields": combined_row},
    )


if __name__ == "__main__":
    app.run(debug=True)
