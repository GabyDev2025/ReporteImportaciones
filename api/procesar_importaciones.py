from flask import Flask, request, send_file
import pandas as pd
import tempfile
import io

app = Flask(__name__)

# --- Tu misma configuración de mapeos ---
CODIGOS_PAISES = {
    "AR": "Argentina", "BO": "Bolivia", "BR": "Brasil", "CL": "Chile",
    "CO": "Colombia", "EC": "Ecuador", "PE": "Perú", "PY": "Paraguay", "UY": "Uruguay"
}

COLUMNAS_OBJETIVO = [
    "Aplica?", "País", "Impo/Expo", "Producto", "Año", "Mes", "Año.Mes", "DUA", "Fecha",
    "Código NCM", "País de Origen", "País de Procedencia", "Aduana", "Puerto de Embarque",
    "Vía Transporte", "Empresa Transportista", "FOB (Total)", "CIF (Total)",
    "FOB (Unitario Tn)", "CIF (Unitario Tn)", "Flete (Total)", "Seguro (Total)",
    "Cantidad Comercial", "Unidad de Medida", "Toneladas Finales", "Importador",
    "Proveedor", "Marca", "Descripción de Mercadería"
]

MAPEO_COSTOS_POR_PAIS = {  # mismo que en tu script
    "Argentina": {"FOB (Total)": "U$S FOB"},
    "Bolivia": {"FOB (Total)": "U$S FOB", "CIF (Total)": "U$S CIF", "Flete (Total)": "Flete", "Seguro (Total)": "Seguro"},
    "Brasil": {"FOB (Total)": "U$S FOB", "FOB (Unitario Tn)": "Unitario FOB"},
    "Chile": {"FOB (Total)": "FOB U$S", "CIF (Total)": "U$S CIF", "Flete (Total)": "Flete U$S", "Seguro (Total)": "Seguro U$S", "FOB (Unitario Tn)": "FOB Unitario U$S"},
    "Colombia": {"FOB (Total)": "U$S FOB", "CIF (Total)": "U$S CIF", "Flete (Total)": "Flete", "Seguro (Total)": "Seguro", "FOB (Unitario Tn)": "FOB Unitario"},
    "Ecuador": {"FOB (Total)": "U$S FOB", "CIF (Total)": "U$S CIF", "Flete (Total)": "Flete", "Seguro (Total)": "Seguro", "FOB (Unitario Tn)": "FOB Unitario"},
    "Perú": {"FOB (Total)": "U$S FOB", "CIF (Total)": "U$S CIF", "Flete (Total)": "Flete", "FOB (Unitario Tn)": "Unitario FOB"},
    "Paraguay": {"FOB (Total)": "U$S FOB", "CIF (Total)": "U$S CIF", "Flete (Total)": "Flete", "Seguro (Total)": "Seguro"},
    "Uruguay": {"FOB (Total)": "U$S FOB"}
}

def clasificar_transporte(valor):
    if pd.isna(valor):
        return "No disponible"
    valor = str(valor).lower()
    if any(p in valor for p in ["camión", "camion", "terrest", "ruta", "carretero"]):
        return "Terrestre"
    elif any(p in valor for p in ["mar", "acuático", "buque", "barco", "nav"]):
        return "Marítimo"
    elif any(p in valor for p in ["aer", "avión", "avion", "aéreo", "aereo"]):
        return "Aéreo"
    else:
        return "No disponible"

def normalizar_unidad(valor):
    if pd.isna(valor):
        return valor
    valor_original = str(valor).strip().upper()
    if "TONELADA" in valor_original:
        return "TONELADAS"
    elif any(p in valor_original for p in ["KILOGRAMO", "KG"]):
        return "KILOGRAMOS"
    else:
        return valor

@app.route('/procesar', methods=['POST'])
def procesar():
    files = request.files.getlist("archivos")
    dataframes = []

    for archivo in files:
        nombre = archivo.filename
        try:
            cod_pais = nombre.split("_")[1][:2].upper()
            pais = CODIGOS_PAISES.get(cod_pais)
            if not pais:
                continue
        except:
            continue

        if nombre.endswith(".xlsx"):
            df = pd.read_excel(archivo)
        else:
            df = pd.read_csv(archivo, encoding="utf-8")

        df["País"] = pais
        if "Fecha" in df.columns:
            df["Fecha_dt"] = pd.to_datetime(df["Fecha"], errors="coerce")
        else:
            df["Fecha_dt"] = pd.NaT
        df["Año"] = df["Fecha_dt"].dt.year
        df["Mes"] = df["Fecha_dt"].dt.month
        df["Año.Mes"] = df["Fecha_dt"].dt.strftime("%Y.%m")
        df["Fecha"] = df["Fecha_dt"].dt.strftime("%d/%m/%Y")
        df.drop(columns=["Fecha_dt"], inplace=True)

        df["Impo/Expo"] = "Importación"
        df["Producto"] = "Silicato de Sodio"
        df["Código NCM"] = "2839190000"

        if "Transporte" in df.columns:
            df["Vía Transporte"] = df["Transporte"].apply(clasificar_transporte)
        else:
            df["Vía Transporte"] = "No disponible"

        unidad_cruda = df.get("Unidad", df.get("Unidad de Medida", None))
        df["Unidad de Medida"] = unidad_cruda.apply(normalizar_unidad) if unidad_cruda is not None else None
        if pais == "Bolivia":
            df["Unidad de Medida"] = "KILOGRAMOS"

        df["Cantidad Comercial"] = df.get("Cantidad Comercial", df.get("Cantidad", None))
        df["Toneladas Finales"] = None
        df.loc[df["Unidad de Medida"] == "TONELADAS", "Toneladas Finales"] = df["Cantidad Comercial"]
        df.loc[df["Unidad de Medida"] == "KILOGRAMOS", "Toneladas Finales"] = df["Cantidad Comercial"] / 1000

        mapeo = MAPEO_COSTOS_POR_PAIS.get(pais, {})
        for col_final, col_fuente in mapeo.items():
            if col_fuente in df.columns:
                df[col_final] = df[col_fuente]

        df["Aplica?"] = df["Toneladas Finales"].apply(lambda x: "SI" if pd.notna(x) and x >= 1 else "NO")
        for col in COLUMNAS_OBJETIVO:
            if col not in df.columns:
                df[col] = None

        df = df[COLUMNAS_OBJETIVO]
        dataframes.append(df)

    if not dataframes:
        return "No se procesaron archivos", 400

    df_final = pd.concat(dataframes, ignore_index=True)

    output = io.BytesIO()
    df_final.to_excel(output, index=False)
    output.seek(0)

    return send_file(output, as_attachment=True, download_name="importaciones_unificadas.xlsx")

# Para Vercel
def handler(request):
    with app.request_context(request.environ):
        return app.full_dispatch_request()
