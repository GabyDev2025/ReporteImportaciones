import os
import pandas as pd

# Ruta de entrada y salida
CARPETA_DATOS = "./datos_importaciones"
ARCHIVO_SALIDA = "importaciones_unificadas.xlsx"

# Códigos y nombres de países
CODIGOS_PAISES = {
    "AR": "Argentina",
    "BO": "Bolivia",
    "BR": "Brasil",
    "CL": "Chile",
    "CO": "Colombia",
    "EC": "Ecuador",
    "PE": "Perú",
    "PY": "Paraguay",
    "UY": "Uruguay"
}

# Columnas requeridas
COLUMNAS_OBJETIVO = [
    "Aplica?", "País", "Impo/Expo", "Producto", "Año", "Mes", "Año.Mes", "DUA",
    "Fecha", "Código NCM", "País de Origen", "País de Procedencia", "Aduana",
    "Puerto de Embarque", "Vía Transporte", "Empresa Transportista",
    "U$S FOB (Total)", "U$S CIF (Total)", "U$S FOB (Unitario Tn)",
    "U$S CIF (Unitario Tn)", "U$S Flete (Total)", "U$S Seguro (Total)",
    "Cantidad Comercial", "Unidad de Medida", "Toneladas Finales",
    "Importador", "Proveedor", "Marca", "Descripción de Mercadería"
]

# Función para normalizar medios de transporte
def clasificar_transporte(valor):
    if pd.isna(valor):
        return "No disponible"
    valor = str(valor).lower()
    if any(palabra in valor for palabra in ["camión", "camion", "terrest", "ruta", "Carretero", "Terrestre"]):
        return "Terrestre"
    elif any(palabra in valor for palabra in ["mar", "acuático", "buque", "barco", "nav", "ACUATICO"]):
        return "Marítimo"
    elif any(palabra in valor for palabra in ["aer", "avión", "avion", "aéreo", "aereo", "Aéreo", "Aereo"]):
        return "Aéreo"
    else:
        return "No disponible"

def leer_archivos_desde_carpeta(carpeta):
    archivos = [f for f in os.listdir(carpeta) if f.endswith(('.csv', '.xlsx'))]
    dataframes = []

    print(f"🔍 Archivos encontrados: {len(archivos)}")

    for archivo in archivos:
        ruta_completa = os.path.join(carpeta, archivo)
        print(f"📄 Procesando: {archivo}")

        try:
            cod_pais = archivo.split("_")[1][:2].upper()
            pais = CODIGOS_PAISES.get(cod_pais, "Desconocido")
        except Exception:
            pais = "Desconocido"

        try:
            if archivo.endswith('.csv'):
                df = pd.read_csv(ruta_completa, encoding="utf-8")
            else:
                df = pd.read_excel(ruta_completa)

            df["País"] = pais

            # Usar "Fecha" o "Fecha Canc."
            if "Fecha" in df.columns:
                df["Fecha"] = pd.to_datetime(df["Fecha"], errors='coerce')
            elif "Fecha Canc." in df.columns:
                df["Fecha"] = pd.to_datetime(df["Fecha Canc."], errors='coerce')
            else:
                df["Fecha"] = pd.NaT

            # Formato de fecha: día/mes/año
            df["Fecha"] = df["Fecha"].dt.strftime('%-m/%-d/%Y')

            # Año, Mes y Año.Mes
            df["Año"] = pd.to_datetime(df["Fecha"], errors='coerce').dt.year
            df["Mes"] = pd.to_datetime(df["Fecha"], errors='coerce').dt.month
            df["Año.Mes"] = df["Año"].astype(str) + '.' + df["Mes"].astype(str)

            # Completado fijo
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

            # Aduana = Puerto si existe
            if "Puerto" in df.columns:
                df["Aduana"] = df["Puerto"]
            else:
                df["Aduana"] = None

            # Unidad de Medida
            if "Unidad" in df.columns:
                df["Unidad de Medida"] = df["Unidad"]
            elif "Unidad de Medida" in df.columns:
                df["Unidad de Medida"] = df["Unidad de Medida"]
            else:
                df["Unidad de Medida"] = None

            # Cantidad Comercial
            if "Cantidad Comercial" in df.columns:
                df["Cantidad Comercial"] = df["Cantidad Comercial"]
            elif "Cantidad" in df.columns:
                df["Cantidad Comercial"] = df["Cantidad"]
            else:
                df["Cantidad Comercial"] = None

            # FOB y CIF
            if "U$S FOB" in df.columns:
                df["U$S FOB (Total)"] = df["U$S FOB"]
            if "U$S CIF" in df.columns:
                df["U$S CIF (Total)"] = df["U$S CIF"]

            # Completar columnas faltantes
            for col in COLUMNAS_OBJETIVO:
                if col not in df.columns:
                    df[col] = None

            df = df[COLUMNAS_OBJETIVO]
            dataframes.append(df)

        except Exception as e:
            print(f"⚠️ Error procesando {archivo}: {e}")

    if not dataframes:
        print("❌ No se procesó ningún archivo correctamente.")
        return pd.DataFrame(columns=COLUMNAS_OBJETIVO)

    df_unificado = pd.concat(dataframes, ignore_index=True)
    print(f"✅ Archivos procesados: {len(dataframes)}")
    print(f"📊 Filas totales en el Excel final: {len(df_unificado)}")

    return df_unificado

def main():
    print("🚀 Iniciando proceso de unificación...")
    df_final = leer_archivos_desde_carpeta(CARPETA_DATOS)
    print(f"💾 Guardando archivo final como: {ARCHIVO_SALIDA}")
    df_final.to_excel(ARCHIVO_SALIDA, index=False, engine='openpyxl')
    print("✅ Proceso completado exitosamente.")

if __name__ == "__main__":
    main()
