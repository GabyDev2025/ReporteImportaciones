from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
import io
import pandas as pd

from procesar_importaciones import procesar_archivo

app = FastAPI()

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
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
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
