import os
import pandas as pd

# Rutas
CARPETA_DATOS = "./datos_importaciones"
ARCHIVO_SALIDA = "importaciones_unificadas.xlsx"

# Códigos de país por archivo
CODIGOS_PAISES = {
    "AR": "Argentina", "BO": "Bolivia", "BR": "Brasil", "CL": "Chile",
    "CO": "Colombia", "EC": "Ecuador", "PE": "Perú", "PY": "Paraguay", "UY": "Uruguay"
}

# Columnas destino (orden final)
COLUMNAS_OBJETIVO = [
    "Aplica?", "País", "Impo/Expo", "Producto", "Año", "Mes", "Año.Mes", "DUA", "Fecha",
    "Código NCM", "País de Origen", "País de Procedencia", "Aduana", "Puerto de Embarque",
    "Vía Transporte", "Empresa Transportista", "FOB (Total)", "CIF (Total)",
    "FOB (Unitario Tn)", "CIF (Unitario Tn)", "Flete (Total)", "Seguro (Total)",
    "Cantidad Comercial", "Unidad de Medida", "Toneladas Finales", "Importador",
    "Proveedor", "Marca", "Descripción de Mercadería"
]

# Mapeo de costos por país
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

# Clasificador de medio de transporte
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

# Lector y procesador
def leer_archivos_desde_carpeta():
    archivos = [f for f in os.listdir(CARPETA_DATOS) if f.endswith(('.xlsx', '.csv')) and f.startswith("detalle_")]
    dataframes = []

    print(f"🔍 Archivos encontrados: {len(archivos)}")

    for archivo in archivos:
        print(f"📄 Procesando: {archivo}")
        ruta = os.path.join(CARPETA_DATOS, archivo)

        try:
            cod_pais = archivo.split("_")[1][:2].upper()
            pais = CODIGOS_PAISES.get(cod_pais)
            if not pais:
                print(f"⚠️ Código país no reconocido en {archivo}")
                continue
        except Exception:
            print(f"⚠️ Error identificando país en {archivo}")
            continue

        try:
            df = pd.read_excel(ruta) if archivo.endswith(".xlsx") else pd.read_csv(ruta, encoding="utf-8")
            df["País"] = pais

            # Fecha → convertir y formatear
            if "Fecha" in df.columns:
                df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce").dt.strftime("%d/%m/%Y")
            elif "Fecha Canc." in df.columns:
                df["Fecha"] = pd.to_datetime(df["Fecha Canc."], errors="coerce").dt.strftime("%d/%m/%Y")
            else:
                df["Fecha"] = None

            # Año, Mes, Año.Mes
            df["Año"] = pd.to_datetime(df["Fecha"], errors="coerce").dt.year
            df["Mes"] = pd.to_datetime(df["Fecha"], errors="coerce").dt.month
            df["Año.Mes"] = df["Año"].astype(str) + "." + df["Mes"].astype(str)

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

            # Aduana
            df["Aduana"] = df["Puerto"] if "Puerto" in df.columns else None

            # Unidad de medida
            df["Unidad de Medida"] = df.get("Unidad", df.get("Unidad de Medida", None))

            # Cantidad Comercial
            df["Cantidad Comercial"] = df.get("Cantidad Comercial", df.get("Cantidad", None))

            # Asignar campos de costos por país
            mapeo = MAPEO_COSTOS_POR_PAIS.get(pais, {})
            for col_final, col_fuente in mapeo.items():
                if col_fuente in df.columns:
                    df[col_final] = df[col_fuente]
                else:
                    print(f"⚠️ '{col_fuente}' no encontrado en {archivo}")

            # Asegurar columnas faltantes
            for col in COLUMNAS_OBJETIVO:
                if col not in df.columns:
                    df[col] = None

            # Reordenar
            df = df[COLUMNAS_OBJETIVO]
            dataframes.append(df)

        except Exception as e:
            print(f"❌ Error al procesar {archivo}: {e}")

    return dataframes

def main():
    dataframes = leer_archivos_desde_carpeta()

    if not dataframes:
        print("❌ No se procesaron archivos.")
        return

    df_final = pd.concat(dataframes, ignore_index=True)
    print(f"✅ Archivos procesados: {len(dataframes)}")
    print(f"📊 Total de filas: {len(df_final)}")

    df_final.to_excel(ARCHIVO_SALIDA, index=False)
    print(f"💾 Archivo guardado como {ARCHIVO_SALIDA}")
    print("✅ Proceso finalizado.")

if __name__ == "__main__":
    main()
