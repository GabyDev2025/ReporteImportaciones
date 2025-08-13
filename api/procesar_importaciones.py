import pandas as pd
import io
import cgi
from http.server import BaseHTTPRequestHandler

# Columnas destino y mapeos (igual que tu script local)
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

MAPEO_COSTOS_POR_PAIS = {
    "Argentina": { "FOB (Total)": "U$S FOB" },
    "Bolivia": {
        "FOB (Total)": "U$S FOB", "CIF (Total)": "U$S CIF",
        "Flete (Total)": "Flete", "Seguro (Total)": "Seguro"
    },
    "Brasil": {
        "FOB (Total)": "U$S FOB", "FOB (Unitario Tn)": "Unitario FOB"
    },
    "Chile": {
        "FOB (Total)": "FOB U$S", "CIF (Total)": "U$S CIF",
        "Flete (Total)": "Flete U$S", "Seguro (Total)": "Seguro U$S",
        "FOB (Unitario Tn)": "FOB Unitario U$S"
    },
    "Colombia": {
        "FOB (Total)": "U$S FOB", "CIF (Total)": "U$S CIF",
        "Flete (Total)": "Flete", "Seguro (Total)": "Seguro",
        "FOB (Unitario Tn)": "FOB Unitario"
    },
    "Ecuador": {
        "FOB (Total)": "U$S FOB", "CIF (Total)": "U$S CIF",
        "Flete (Total)": "Flete", "Seguro (Total)": "Seguro",
        "FOB (Unitario Tn)": "FOB Unitario"
    },
    "Perú": {
        "FOB (Total)": "U$S FOB", "CIF (Total)": "U$S CIF",
        "Flete (Total)": "Flete", "FOB (Unitario Tn)": "Unitario FOB"
    },
    "Paraguay": {
        "FOB (Total)": "U$S FOB", "CIF (Total)": "U$S CIF",
        "Flete (Total)": "Flete", "Seguro (Total)": "Seguro"
    },
    "Uruguay": { "FOB (Total)": "U$S FOB" }
}

# Funciones auxiliares
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
    elif any(p in valor_original for p in ["KILOGRAMO", "KILOGRAMO BRUTO", "KILOS NETOS", "KG"]):
        return "KILOGRAMOS"
    else:
        return valor

def procesar_excel(df, pais):
    # Fecha
    if "Fecha" in df.columns:
        df["Fecha_dt"] = pd.to_datetime(df["Fecha"], errors="coerce")
    else:
        df["Fecha_dt"] = pd.NaT
    df["Año"] = df["Fecha_dt"].dt.year
    df["Mes"] = df["Fecha_dt"].dt.month
    df["Año.Mes"] = df["Fecha_dt"].dt.strftime("%Y.%m")
    df["Fecha"] = df["Fecha_dt"].dt.strftime("%d/%m/%Y")
    df.drop(columns=["Fecha_dt"], inplace=True)

    # Datos fijos
    df["País"] = pais
    df["Impo/Expo"] = "Importación"
    df["Producto"] = "Silicato de Sodio"
    df["Código NCM"] = "2839190000"

    # Transporte
    if "Transporte" in df.columns:
        df["Vía Transporte"] = df["Transporte"].apply(clasificar_transporte)
    elif "Vía Transporte" in df.columns:
        df["Vía Transporte"] = df["Vía Transporte"].apply(clasificar_transporte)
    else:
        df["Vía Transporte"] = "No disponible"

    # Unidad de medida
    unidad_cruda = df.get("Unidad", df.get("Unidad de Medida", None))
    df["Unidad de Medida"] = unidad_cruda.apply(normalizar_unidad) if unidad_cruda is not None else None
    if pais == "Bolivia":
        df["Unidad de Medida"] = "KILOGRAMOS"

    # Cantidad Comercial
    df["Cantidad Comercial"] = df.get("Cantidad Comercial", df.get("Cantidad", None))

    # Toneladas Finales
    df["Toneladas Finales"] = None
    df.loc[df["Unidad de Medida"] == "TONELADAS", "Toneladas Finales"] = df["Cantidad Comercial"]
    df.loc[df["Unidad de Medida"] == "KILOGRAMOS", "Toneladas Finales"] = df["Cantidad Comercial"] / 1000

    # Costos por país
    mapeo = MAPEO_COSTOS_POR_PAIS.get(pais, {})
    for col_final, col_fuente in mapeo.items():
        if col_fuente in df.columns:
            df[col_final] = df[col_fuente]

    # FOB Unitario Argentina
    if pais == "Argentina":
        if "FOB (Unitario Tn)" not in df.columns or df["FOB (Unitario Tn)"].isna().all():
            df["FOB (Unitario Tn)"] = df.apply(
                lambda row: round(row["FOB (Total)"] / row["Toneladas Finales"], 2)
                if pd.notna(row["FOB (Total)"]) and pd.notna(row["Toneladas Finales"]) and row["Toneladas Finales"] != 0
                else None, axis=1)

    # Columna Aplica?
    df["Aplica?"] = df["Toneladas Finales"].apply(lambda x: "SI" if pd.notna(x) and x >= 1 else "NO")

    # Columnas faltantes
    for col in COLUMNAS_OBJETIVO:
        if col not in df.columns:
            df[col] = None

    return df[COLUMNAS_OBJETIVO]

# Función principal del handler Vercel
class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        form = cgi.FieldStorage(fp=self.rfile, headers=self.headers,
                                environ={'REQUEST_METHOD':'POST',
                                         'CONTENT_TYPE': self.headers['Content-Type']})
        archivos = form['files']
        if not isinstance(archivos, list):
            archivos = [archivos]

        dfs = []
        for archivo in archivos:
            contenido = archivo.file.read()
            df = pd.read_excel(io.BytesIO(contenido))
            # Intentar detectar país desde nombre de archivo
            nombre = archivo.filename
            cod_pais = nombre.split("_")[1][:2].upper() if "_" in nombre else "AR"
            pais = CODIGOS_PAISES.get(cod_pais, "Argentina")
            df_proc = procesar_excel(df, pais)
            dfs.append(df_proc)

        df_final = pd.concat(dfs, ignore_index=True)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_final.to_excel(writer, index=False)
        output.seek(0)

        self.send_response(200)
        self.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        self.send_header("Content-Disposition", "attachment; filename=importaciones_unificadas.xlsx")
        self.end_headers()
        self.wfile.write(output.read())
