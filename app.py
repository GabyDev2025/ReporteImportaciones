import io
import os
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
import pandas as pd

app = FastAPI()

# Código y mapeos que ya tenés (adaptado para uso interno)

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
    "Uruguay": {
        "FOB (Total)": "U$S FOB"
    }
}

def clasificar_transporte(valor):
    if pd.isna(valor):
        return "No disponible"
    valor = str(valor).lower()
    if any(p in valor for p in ["camión", "camion", "terrest", "ruta", "carretero"]):
        return "Terrestre"
    elif any(p in valor for p in ["mar", "acuático", "acuatico", "buque", "barco", "nav"]):
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

def procesar_archivo(file_bytes, filename):
    # Solo procesa archivos que empiezan con detalle_
    if not filename.startswith("detalle_"):
        raise HTTPException(status_code=400, detail=f"Archivo {filename} no comienza con 'detalle_'")

    cod_pais = filename.split("_")[1][:2].upper()
    pais = CODIGOS_PAISES.get(cod_pais)
    if not pais:
        raise HTTPException(status_code=400, detail=f"Código país no reconocido en {filename}")

    # Leer archivo según extensión
    if filename.endswith(".xlsx"):
        df = pd.read_excel(io.BytesIO(file_bytes))
    elif filename.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(file_bytes))
    else:
        raise HTTPException(status_code=400, detail=f"Formato no soportado en {filename}")

    df["País"] = pais

    # Procesar columnas similares al código que tenés (simplificado para brevity)
    if "Fecha" in df.columns:
        df["Fecha_dt"] = pd.to_datetime(df["Fecha"], errors="coerce")
    elif "Fecha Canc." in df.columns:
        df["Fecha_dt"] = pd.to_datetime(df["Fecha Canc."], errors="coerce")
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

    # Transporte
    if "Transporte" in df.columns:
        df["Vía Transporte"] = df["Transporte"].apply(clasificar_transporte)
    elif "Vía Transporte" in df.columns:
        df["Vía Transporte"] = df["Vía Transporte"].apply(clasificar_transporte)
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

    if pais == "Argentina":
        if "FOB (Unitario Tn)" not in df.columns or df["FOB (Unitario Tn)"].isna().all():
            df["FOB (Unitario Tn)"] = df.apply(
                lambda row: round(row["FOB (Total)"] / row["Toneladas Finales"], 2)
                if pd.notna(row.get("FOB (Total)")) and pd.notna(row.get("Toneladas Finales")) and row["Toneladas Finales"] != 0
                else None,
                axis=1
            )

    # Aquí podrías agregar más ajustes por país, como en tu código original

    # Columnas faltantes
    for col in COLUMNAS_OBJETIVO:
        if col not in df.columns:
            df[col] = None

    df["Aplica?"] = df["Toneladas Finales"].apply(lambda x: "SI" if pd.notna(x) and x >= 1 else "NO")

    df = df[COLUMNAS_OBJETIVO]

    return df

@app.post("/unificar")
async def unificar_archivos(files: list[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="No se subieron archivos")

    dataframes = []
    for file in files:
        content = await file.read()
        try:
            df = procesar_archivo(content, file.filename)
            dataframes.append(df)
        except HTTPException as e:
            raise e
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error procesando {file.filename}: {str(e)}")

    if not dataframes:
        raise HTTPException(status_code=400, detail="No se generaron datos de los archivos")

    df_final = pd.concat(dataframes, ignore_index=True)

    output = io.BytesIO()
    df_final.to_excel(output, index=False)
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="importaciones_unificadas.xlsx"'}
    )
