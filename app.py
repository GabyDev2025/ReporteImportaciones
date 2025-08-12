import os
import io
import shutil
import pandas as pd
import streamlit as st

# --- Configuración de página ---
st.set_page_config(page_title="Unificador de Importaciones", page_icon="📦", layout="wide")
st.title("📦 Unificador de Importaciones")
st.write("Subí los archivos de importaciones (empiecen con 'detalle_') para generar un reporte unificado en Excel.")

# --- Constantes y mapeos (tal como los tenías) ---
CARPETA_DATOS = "./datos_importaciones"  # usado si querés ejecutar localmente sin subir archivos
ARCHIVO_SALIDA = "importaciones_unificadas.xlsx"

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
        # FOB (Unitario Tn) para Uruguay lo asignaste usando "Unitario VNA" en tu script original
    }
}

# --- Funciones auxiliares (misma lógica que usás) ---
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

def leer_archivos_desde_carpeta(carpeta):
    """
    Lee archivos que empiezan con 'detalle_' en la carpeta provista y aplica
    todo el procesamiento que ya tenías. Devuelve una lista de dataframes.
    """
    archivos = [f for f in os.listdir(carpeta) if f.endswith(('.xlsx', '.csv')) and f.startswith("detalle_")]
    dataframes = []

    print(f"🔍 Archivos encontrados en {carpeta}: {len(archivos)}")

    for archivo in archivos:
        ruta = os.path.join(carpeta, archivo)
        print(f"📄 Procesando: {archivo}")

        try:
            cod_pais = archivo.split("_")[1][:2].upper()
            pais = CODIGOS_PAISES.get(cod_pais)
            if not pais:
                print(f"⚠️ Código país no reconocido en {archivo} -> {cod_pais}")
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

            # Costos por país (mapeo genérico)
            mapeo = MAPEO_COSTOS_POR_PAIS.get(pais, {})
            for col_final, col_fuente in mapeo.items():
                if col_fuente in df.columns:
                    df[col_final] = df[col_fuente]
                else:
                    # no rompemos si falta columna fuente; sólo avisamos en consola
                    print(f"⚠️ '{col_fuente}' no encontrado en {archivo}")

            # Cálculo FOB Unitario para Argentina si no existe
            if pais == "Argentina":
                if "FOB (Unitario Tn)" not in df.columns or df["FOB (Unitario Tn)"].isna().all():
                    df["FOB (Unitario Tn)"] = df.apply(
                        lambda row: round(row["FOB (Total)"] / row["Toneladas Finales"], 2)
                        if pd.notna(row.get("FOB (Total)")) and pd.notna(row.get("Toneladas Finales")) and row["Toneladas Finales"] != 0
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

            # COMPLETAR LA COLUMNA "Aplica?" POR FILA SEGÚN "Toneladas Finales"
            df["Aplica?"] = df["Toneladas Finales"].apply(lambda x: "SI" if pd.notna(x) and x >= 1 else "NO")

            # Reordenar columnas
            df = df[COLUMNAS_OBJETIVO]
            dataframes.append(df)

        except Exception as e:
            print(f"❌ Error al procesar {archivo}: {e}")

    return dataframes

# --- Interfaz Streamlit ---
st.markdown("### 1) Subir archivos")
st.write("Subí uno o varios archivos `.xlsx` o `.csv`. Los archivos deben comenzar con `detalle_` y el código de país después del guion bajo (ej: `detalle_AR_...xlsx`).")

uploaded_files = st.file_uploader(
    "Seleccioná uno o varios archivos (.xlsx o .csv) que empiecen con 'detalle_':",
    type=["xlsx", "csv"],
    accept_multiple_files=True
)

if uploaded_files:
    st.success(f"Archivos cargados: {[f.name for f in uploaded_files]}")

    generar = st.button("🔄 Generar reporte unificado")
    if generar:
        temp_dir = "./temp_uploads"
        # limpiar y crear temp
        if os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
            except Exception:
                pass
        os.makedirs(temp_dir, exist_ok=True)

        # Guardar archivos subidos en temp_dir
        for file in uploaded_files:
            save_path = os.path.join(temp_dir, file.name)
            with open(save_path, "wb") as f:
                f.write(file.getbuffer())

        # Procesar
        try:
            dataframes = leer_archivos_desde_carpeta(temp_dir)
            if not dataframes:
                st.warning("No se generaron datos. Verificá que los archivos empiecen con 'detalle_' y contengan el código de país en el nombre.")
            else:
                df_final = pd.concat(dataframes, ignore_index=True)
                st.write(f"✅ Archivos procesados: {len(dataframes)} — Filas totales: {len(df_final)}")

                # Guardar en buffer para descarga y ofrecer vista previa
                buffer = io.BytesIO()
                df_final.to_excel(buffer, index=False)
                buffer.seek(0)

                st.download_button(
                    label="📥 Descargar Excel unificado",
                    data=buffer,
                    file_name="importaciones_unificadas.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

                st.markdown("#### Vista previa (primeras 10 filas):")
                st.dataframe(df_final.head(10))

        except Exception as e:
            st.error(f"Ocurrió un error durante el procesamiento: {e}")

        # limpiar temp
        try:
            shutil.rmtree(temp_dir)
        except Exception:
            pass

else:
    st.info("Aún no subiste archivos. También podés ejecutar el script localmente (usa la carpeta ./datos_importaciones).")

# Botón para ejecutar local (opcional)
if st.button("Ejecutar local (procesar ./datos_importaciones)"):
    if not os.path.exists(CARPETA_DATOS):
        st.error(f"No existe la carpeta local {CARPETA_DATOS}")
    else:
        try:
            dataframes = leer_archivos_desde_carpeta(CARPETA_DATOS)
            if not dataframes:
                st.warning("No se generaron datos desde la carpeta local.")
            else:
                df_final = pd.concat(dataframes, ignore_index=True)
                df_final.to_excel(ARCHIVO_SALIDA, index=False)
                st.success(f"Archivo guardado localmente como {ARCHIVO_SALIDA}")
        except Exception as e:
            st.error(f"Error al procesar carpeta local: {e}")
