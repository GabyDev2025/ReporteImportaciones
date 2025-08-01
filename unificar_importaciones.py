import os
import pandas as pd

# Ruta de entrada y salida
CARPETA_DATOS = "./datos_importaciones"
ARCHIVO_SALIDA = "importaciones_unificadas.xlsx"

# Códigos de país por archivo
CODIGOS_PAISES = {
    "AR": "Argentina",
    "BO": "Bolivia",
    "BR": "Brasil",
    "CL": "Chile",
    "CO": "Colombia",
    "EC": "Ecuador",
    "PE": "Perú",
    "PY": "Paraguay",
    "UY": "Uruguay",
}

# Columnas esperadas finales (orden correcto)
COLUMNAS_OBJETIVO = [
    "Aplica?", "País", "Impo/Expo", "Producto", "Año", "Mes", "Año.Mes", "DUA", "Fecha",
    "Código NCM", "País de Origen", "País de Procedencia", "Aduana", "Puerto de Embarque",
    "Vía Transporte", "Empresa Transportista", "FOB (Total)", "CIF (Total)",
    "FOB (Unitario Tn)", "CIF (Unitario Tn)", "Flete (Total)", "Seguro (Total)",
    "Cantidad Comercial", "Unidad de Medida", "Toneladas Finales", "Importador",
    "Proveedor", "Marca", "Descripción de Mercadería"
]

# Mapeo por país
MAPEO_COSTOS_POR_PAIS = {
    "Argentina": {
        "FOB (Total)": "U$S FOB"
    },
    "Bolivia": {
        "FOB (Total)": "U$S FOB",
        "CIF (Total)": "U$S CIF",
        "Flete (Total)": "Flete",
        "Seguro (Total)": "Seguro"
    },
    "Brasil": {
        "FOB (Total)": "U$S FOB",
        "FOB (Unitario Tn)": "Unitario FOB"
    },
    "Chile": {
        "FOB (Total)": "FOB U$S",
        "CIF (Total)": "U$S CIF",
        "Flete (Total)": "Flete U$S",
        "Seguro (Total)": "Seguro U$S",
        "FOB (Unitario Tn)": "FOB Unitario U$S"
    },
    "Colombia": {
        "FOB (Total)": "U$S FOB",
        "CIF (Total)": "U$S CIF",
        "Flete (Total)": "Flete",
        "Seguro (Total)": "Seguro",
        "FOB (Unitario Tn)": "FOB Unitario"
    },
    "Ecuador": {
        "FOB (Total)": "U$S FOB",
        "CIF (Total)": "U$S CIF",
        "Flete (Total)": "Flete",
        "Seguro (Total)": "Seguro",
        "FOB (Unitario Tn)": "FOB Unitario"
    },
    "Perú": {
        "FOB (Total)": "U$S FOB",
        "CIF (Total)": "U$S CIF",
        "Flete (Total)": "Flete",
        "FOB (Unitario Tn)": "Unitario FOB"
    },
    "Paraguay": {
        "FOB (Total)": "U$S FOB",
        "CIF (Total)": "U$S CIF",
        "Flete (Total)": "Flete",
        "Seguro (Total)": "Seguro"
    },
    "Uruguay": {
        "FOB (Total)": "U$S FOB"
    }
}

def leer_archivos_desde_carpeta():
    dataframes = []
    archivos = [f for f in os.listdir(CARPETA_DATOS) if f.endswith(".xlsx") and f.startswith("detalle_")]

    print("🚀 Iniciando proceso de unificación...")
    print(f"🔍 Archivos encontrados: {len(archivos)}")

    for archivo in archivos:
        print(f"📄 Procesando: {archivo}")
        ruta = os.path.join(CARPETA_DATOS, archivo)

        try:
            df = pd.read_excel(ruta)
        except Exception as e:
            print(f"❌ Error al leer {archivo}: {e}")
            continue

        # Identificar país por código en nombre del archivo
        try:
            cod_pais = archivo.split("_")[1][:2].upper()
            pais = CODIGOS_PAISES.get(cod_pais)
            if not pais:
                print(f"⚠️ No se reconoce el código de país '{cod_pais}' en el archivo {archivo}")
                continue
            print(f"🌎 País detectado: {pais}")
        except Exception as e:
            print(f"❌ Error al detectar país en archivo {archivo}: {e}")
            continue

        # Insertar columna 'País'
        df["País"] = pais

        # Normalizar columnas según mapeo
        if pais in MAPEO_COSTOS_POR_PAIS:
            mapeo = MAPEO_COSTOS_POR_PAIS[pais]
            for col_destino, col_fuente in mapeo.items():
                if col_fuente in df.columns:
                    df[col_destino] = df[col_fuente]
                    print(f"   ✅ {col_destino} completado desde '{col_fuente}'")
                else:
                    print(f"   ⚠️  '{col_fuente}' no encontrado en el archivo {archivo}")
        else:
            print(f"⚠️ No hay mapeo definido para {pais}.")

        # Convertir la fecha si existe
        if "Fecha" in df.columns:
            df["Fecha"] = pd.to_datetime(df["Fecha"], errors='coerce')

        # Agregar columnas faltantes vacías
        for columna in COLUMNAS_OBJETIVO:
            if columna not in df.columns:
                df[columna] = None

        # Reordenar columnas
        df = df[COLUMNAS_OBJETIVO]
        dataframes.append(df)

    return dataframes

def main():
    dataframes = leer_archivos_desde_carpeta()

    if not dataframes:
        print("❌ No se procesaron archivos.")
        return

    df_unificado = pd.concat(dataframes, ignore_index=True)
    print(f"✅ Archivos procesados: {len(dataframes)}")
    print(f"📊 Filas totales en el Excel final: {len(df_unificado)}")

    df_unificado.to_excel(ARCHIVO_SALIDA, index=False)
    print(f"💾 Guardando archivo final como: {os.path.basename(ARCHIVO_SALIDA)}")
    print("✅ Proceso completado exitosamente.")

if __name__ == "__main__":
    main()
