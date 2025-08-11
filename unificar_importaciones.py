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
        # No incluye FOB (Unitario Tn) aquí porque lo asignaremos directamente
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

# Normalizador de unidad
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

            # Fecha
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

            # Datos fijos
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
            
            # Para Bolivia, forzar Unidad de Medida a "KILOGRAMOS"
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
                else:
                    print(f"⚠️ '{col_fuente}' no encontrado en {archivo}")

            # Cálculo FOB Unitario para Argentina si no existe
            if pais == "Argentina":
                if "FOB (Unitario Tn)" not in df.columns or df["FOB (Unitario Tn)"].isna().all():
                    df["FOB (Unitario Tn)"] = df.apply(
                        lambda row: round(row["FOB (Total)"] / row["Toneladas Finales"], 2)
                        if pd.notna(row["FOB (Total)"]) and pd.notna(row["Toneladas Finales"]) and row["Toneladas Finales"] != 0
                        else None,
                        axis=1
                    )

            # Copiar descripción para Argentina
            if pais == "Argentina" and "Descripción" in df.columns:
                df["Descripción de Mercadería"] = df["Descripción"]

            # Bolivia
            if pais == "Bolivia" and "País de Proveedor" in df.columns:
                df["País de Procedencia"] = df["País de Proveedor"]
            if pais == "Bolivia" and "Descripción Arancelaria" in df.columns:
                df["Descripción de Mercadería"] = df["Descripción Arancelaria"]

            # Chile
            if pais == "Chile" and "País de Adquisición" in df.columns:
                df["País de Procedencia"] = df["País de Adquisición"]
            if pais == "Chile" and "Transportista" in df.columns:
                df["Empresa Transportista"] = df["Transportista"]
            if pais == "Chile" and "U$S Unitario" in df.columns:
                df["CIF (Unitario Tn)"] = df["U$S Unitario"]

            # Colombia
            if pais == "Colombia" and "Transportista" in df.columns:
                df["Empresa Transportista"] = df["Transportista"]
            if pais == "Colombia" and "CIF Unitario" in df.columns:
                df["CIF (Unitario Tn)"] = df["CIF Unitario"]
            if pais == "Colombia" and "Descripción Arancelaria" in df.columns:
                df["Descripción de Mercadería"] = df["Descripción Arancelaria"]

            # Ecuador
            if pais == "Ecuador" and "País de Embarque" in df.columns:
                df["País de Procedencia"] = df["País de Embarque"]
            if pais == "Ecuador" and "Aduana" in df.columns:
                df["Aduana"] = df["Aduana"]
            if pais == "Ecuador" and "Provincia" in df.columns:
                df["Puerto de Embarque"] = df["Provincia"]
            if pais == "Ecuador" and "Transportista" in df.columns:
                df["Empresa Transportista"] = df["Transportista"]
            if pais == "Ecuador" and "CIF Unitario" in df.columns:
                df["CIF (Unitario Tn)"] = df["CIF Unitario"]
            if pais == "Ecuador" and "Descripción Comercial" in df.columns:
                df["Descripción de Mercadería"] = df["Descripción Comercial"]

            # Paraguay
            if pais == "Paraguay" and "Probable Importador" in df.columns:
                df["Importador"] = df["Probable Importador"]
            if pais == "Paraguay" and "Probable Proveedor" in df.columns:
                df["Proveedor"] = df["Probable Proveedor"]
            if pais == "Paraguay" and "Descripción" in df.columns:
                df["Descripción de Mercadería"] = df["Descripción"]

            # Perú
            if pais == "Perú":
                if "Puerto" in df.columns:
                    df["Puerto de Embarque"] = df["Puerto"]
                if "Transportista" in df.columns:
                    df["Empresa Transportista"] = df["Transportista"]
                if "Unitario CIF" in df.columns:
                    df["CIF (Unitario Tn)"] = df["Unitario CIF"]
                if "Descripción" in df.columns:
                    df["Descripción de Mercadería"] = df["Descripción"]

            # Uruguay (nueva instrucción)
            if pais == "Uruguay" and "Unitario VNA" in df.columns:
                df["FOB (Unitario Tn)"] = df["Unitario VNA"]

            # Columnas faltantes
            for col in COLUMNAS_OBJETIVO:
                if col not in df.columns:
                    df[col] = None

            # Reordenar columnas
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
