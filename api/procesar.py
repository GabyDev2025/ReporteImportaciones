import io
import pandas as pd
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import StreamingResponse
import uvicorn

app = FastAPI()

# --- Aquí puedes pegar TODA tu configuración ---
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

MAPEO_COSTOS_POR_PAIS = { ... }  # ← aquí va tu mapeo igual que antes

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
    return "No disponible"

def normalizar_unidad(valor):
    if pd.isna(valor):
        return valor
    valor_original = str(valor).strip().upper()
    if "TONELADA" in valor_original:
        return "TONELADAS"
    elif any(p in valor_original for p in ["KILOGRAMO", "KILOS NETOS", "KG"]):
        return "KILOGRAMOS"
    return valor

@app.post("/api/procesar")
async def procesar_archivos(files: list[UploadFile] = File(...)):
    dataframes = []

    for file in files:
        try:
            cod_pais = file.filename.split("_")[1][:2].upper()
            pais = CODIGOS_PAISES.get(cod_pais)
            if not pais:
                continue

            # Leer desde memoria
            if file.filename.endswith(".xlsx"):
                df = pd.read_excel(io.BytesIO(await file.read()))
            else:
                df = pd.read_csv(io.BytesIO(await file.read()), encoding="utf-8")

            # ... Aquí pegas toda tu lógica de transformación por país ...
            # Por ejemplo:
            df["País"] = pais
            # (El resto igual que tu código original, pero eliminando rutas locales y guardado final)

            for col in COLUMNAS_OBJETIVO:
                if col not in df.columns:
                    df[col] = None

            df["Aplica?"] = df["Toneladas Finales"].apply(lambda x: "SI" if pd.notna(x) and x >= 1 else "NO")

            df = df[COLUMNAS_OBJETIVO]
            dataframes.append(df)

        except Exception as e:
            print(f"Error procesando {file.filename}: {e}")

    if not dataframes:
        return {"error": "No se procesaron archivos"}

    df_final = pd.concat(dataframes, ignore_index=True)

    # Guardar en memoria y devolver como descarga
    output = io.BytesIO()
    df_final.to_excel(output, index=False)
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=importaciones_unificadas.xlsx"}
    )

# Para correr localmente:
# if __name__ == "__main__":
#     uvicorn.run(app, host="0.0.0.0", port=8000)
