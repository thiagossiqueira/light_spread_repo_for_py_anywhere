#!/bin/bash
echo "🔁 Iniciando post-pull automático..."

# Detecta se estamos no PythonAnywhere
if [[ "$HOME" == "/home/tsiqueira4" ]]; then
    echo "🧠 Ambiente detectado: PythonAnywhere (produção)"
    ON_PYTHONANYWHERE=true
else
    echo "🧠 Ambiente detectado: Local (desenvolvimento)"
    ON_PYTHONANYWHERE=false
fi

# Habilita sparse checkout e define o que será baixado
echo "📂 Configurando sparse-checkout para evitar arquivos pesados..."
git config core.sparseCheckout true

cat > .git/info/sparse-checkout <<EOF
/*
!datos_y_modelos/db/one-day_interbank_deposit_futures_contract_di/hist_di_curve_contracts_db.xlsx
!datos_y_modelos/db/brazil_domestic_equities/*
!datos_y_modelos/db/brazil_domestic_corp_bonds/brazil_debentures_universe/Resultado/resultado_parte*
EOF

# Aplica o filtro antes do pull
echo "🧹 Aplicando filtro do sparse-checkout..."
git read-tree -mu HEAD

# Agora sim: executa git pull
echo "📥 Executando git pull..."
git pull origin master

# Ativa venv local se aplicável
if [ "$ON_PYTHONANYWHERE" = false ] && [ -d "venv" ]; then
    echo "📦 Ativando ambiente virtual local..."
    source venv/bin/activate
fi

# Instala dependências
echo "📦 Instalando dependências com pip install -e ."
pip install -e .

# Roda o script principal
echo "📊 Executando main.py..."
python main.py

python app.py


# === NEW SECTION ===
echo "💡 Generando superficie CDS-BRL (riesgo Brasil sintético)..."
if [ -f "synthetic_cds_brl.py" ]; then
  python synthetic_cds_brl.py
  echo "✅ synthetic_cds_brl.py ejecutado con éxito."
else
  echo "⚠️ Archivo synthetic_cds_brl.py no encontrado, omitiendo generación del CDS-BRL."
fi
# ====================

# Reinicia app no PythonAnywhere
if [ "$ON_PYTHONANYWHERE" = true ]; then
    echo "🌐 Recarregando aplicação com touch no wsgi.py"
    touch /var/www/tsiqueira4_pythonanywhere_com_wsgi.py
    echo "✅ Deploy finalizado: https://tsiqueira4.pythonanywhere.com"
else
    echo "✅ Projeto atualizado localmente com sucesso!"
fi
