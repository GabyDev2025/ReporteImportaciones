import React, { useState } from "react";

function App() {
  const [files, setFiles] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const handleFileChange = (e) => {
    setFiles(e.target.files);
    setError(null);
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError(null);

    if (!files.length) {
      setError("Por favor, selecciona al menos un archivo.");
      return;
    }

    const formData = new FormData();
    for (let i = 0; i < files.length; i++) {
      formData.append("files", files[i]);
    }

    setLoading(true);
    try {
      const response = await fetch("/unificar", {
        method: "POST",
        body: formData,
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.detail || "Error al procesar archivos.");
      }

      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "importaciones_unificadas.xlsx";
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(url);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ maxWidth: 600, margin: "2rem auto", fontFamily: "Arial, sans-serif" }}>
      <h1>Reporte COMEX - Unificar Importaciones</h1>
      <form onSubmit={handleSubmit}>
        <label>
          Selecciona archivos Excel (.xlsx) o CSV (.csv) (mínimo 1):
          <input
            type="file"
            multiple
            onChange={handleFileChange}
            accept=".xlsx,.csv"
            style={{ display: "block", marginTop: 10, marginBottom: 20 }}
          />
        </label>
        <button type="submit" disabled={loading} style={{ padding: "10px 20px", fontSize: 16 }}>
          {loading ? "Procesando..." : "Unificar y Descargar"}
        </button>
      </form>
      {error && <p style={{ color: "red", marginTop: 20 }}>{error}</p>}
    </div>
  );
}

export default App;
