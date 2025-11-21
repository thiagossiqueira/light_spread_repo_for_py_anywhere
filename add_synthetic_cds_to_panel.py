import pandas as pd
import numpy as np
from src.config import CONFIG
from src.finmath.termstructure.curve_models import NelsonSiegelSvensson

# Cargar panel y superficie CDS-BRL
panel = pd.read_excel(CONFIG["PANEL_DATA_PATH"])
cds_surface = pd.read_excel(CONFIG["SYNTHETIC_CDS_PATH"])

# Usar la última curva (fecha más reciente) de la superficie CDS-BRL
latest_row = cds_surface.iloc[-1].dropna()

# Extraer solo columnas que sean tenores válidos (excluye OBS_DATE)
tenor_labels = [x for x in latest_row.index if "year" in x or "month" in x]
tenor_values = np.array([
    float(x.replace("-year", "").replace("month", "").split("-")[0])
    for x in tenor_labels
])
spreads = latest_row[tenor_labels].values / 100

# Crear estructura tipo bonos ficticios para ajuste NSS
# (cash flows unitarios)

# Criar estrutura tipo bonos ficticios (dicionários com fluxos)
prices = np.ones_like(spreads)
ref_date = pd.Timestamp.today()

# Cada fluxo de caixa como dict: {data_vencimento: valor}
cash_flows = [{ref_date + pd.to_timedelta(int(t * 365), unit="D"): 1.0} for t in tenor_values]

# Ajustar modelo NSS
nss = NelsonSiegelSvensson(prices=prices, cash_flows=cash_flows, ref_date=ref_date)
nss.fit_curve(tenor_values, spreads)



# Calcular spreads sintéticos (en bps) para cada bono del panel
panel["Synthetic_CDS_BRL"] = [
    nss.rate_for_ytm(betas=nss.betas, ytm=t) * 100  # volver a basis points
    for t in panel["days_to_maturity"]
]

# Guardar nuevo archivo con la columna agregada
panel.to_excel(CONFIG["PANEL_DATA_OUTPUT_PATH"], index=False)
print(f"✅ Archivo actualizado con curva NSS: {CONFIG['PANEL_DATA_OUTPUT_PATH']}")