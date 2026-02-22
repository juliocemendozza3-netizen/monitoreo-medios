if __name__ == "__main__":
    print("Iniciando monitoreo...")
    main()
    print("Monitoreo terminado")

import feedparser
import pandas as pd
from datetime import datetime, timedelta

TOPICOS = {
    "Victimas": ["víctimas", "reparación", "snariv"],
    "Firmantes de paz": ["firmantes de paz", "excombatientes", "reincorporación"],
    "PDET": ["pdet"],
    "Circunscripciones de paz": ["circunscripciones de paz"],
    "Partido Comunes": ["partido comunes", "comunes"],
    "Reforma rural integral": ["reforma rural integral"],
    "Estatuto de la oposición": ["estatuto de la oposición"],
    "Protesta social": ["protesta", "movilización social"],
    "Reintegración": ["reintegración política", "reintegración económica"],
    "Fin del conflicto": ["fin del conflicto", "acuerdo de paz"],
    "Lucha contra las drogas": ["lucha contra las drogas", "narcotráfico"],
    "Sustitución de cultivos": ["sustitución de cultivos", "pnís"],
    "Derechos de las víctimas": ["derechos de las víctimas"],
    "JEP": ["jep", "jurisdicción especial para la paz"],
    "Comisión de la Verdad": ["comisión de la verdad"],
    "Unidad de Búsqueda": ["unidad de búsqueda"],
    "Unidad para las Víctimas": ["unidad para las víctimas"],
    "Medidas de protección": ["medidas de protección"],
    "UNP": ["unp", "unidad nacional de protección"]
}

FUENTES = {
    "El Tiempo": "https://www.eltiempo.com/rss/colombia.xml",
    "El Espectador": "https://www.elespectador.com/rss/colombia/",
    "Semana": "https://www.semana.com/rss",
    "Caracol Radio": "https://caracol.com.co/rss/",
    "Blu Radio": "https://www.bluradio.com/rss.xml",
    "RCN Radio": "https://www.rcnradio.com/rss",
    "W Radio": "https://www.wradio.com.co/rss.aspx",
    "Noticias Caracol": "https://noticias.caracoltv.com/rss",
    "Noticias RCN": "https://www.noticiasrcn.com/rss",
    "La Silla Vacía": "https://lasillavacia.com/rss",
    "Cuestión Pública": "https://cuestionpublica.com/feed/",
    "Razón Pública": "https://razonpublica.com/feed/",
    "Verdad Abierta": "https://verdadabierta.com/feed/",
    "Pares": "https://pares.com.co/feed/",
    "Cerosetenta": "https://cerosetenta.uniandes.edu.co/feed/"
}

def recolectar():
    filas = []
    for medio, url in FUENTES.items():
        feed = feedparser.parse(url)
        for e in feed.entries:
            filas.append({
                "medio": medio,
                "titulo": e.title,
                "resumen": e.get("summary"),
                "fecha_publicacion": e.get("published"),
                "fecha_consulta": datetime.now()
            })
    return pd.DataFrame(filas)

df_raw = recolectar()

def clasificar(texto):
    texto = str(texto).lower()
    return [t for t, palabras in TOPICOS.items() if any(p in texto for p in palabras)]

df_raw["temas"] = df_raw["titulo"].apply(clasificar)
df = df_raw[df_raw["temas"].str.len() > 0]

df["resumen"] = df["resumen"].fillna("No informado")
df["fecha_publicacion"] = pd.to_datetime(df["fecha_publicacion"], errors="coerce")
df["fecha_publicacion"] = df["fecha_publicacion"].fillna(df["fecha_consulta"])

from datetime import timezone
df_24h = df[df["fecha_publicacion"] >= datetime.now(timezone.utc) - timedelta(days=1)]

df_1h = df_24h[df_24h["fecha_publicacion"] >= datetime.now(timezone.utc) - timedelta(hours=1)]

conteo_1h = df_1h.explode("temas").groupby("temas").size().reset_index(name="menciones_1h")

baseline = df_24h.explode("temas").groupby("temas").size().reset_index(name="menciones_24h")
baseline["promedio_hora"] = baseline["menciones_24h"] / 24

import pandas as pd

if conteo_1h.empty or baseline.empty:
    # Initialize crisis with the expected columns if inputs are empty
    crisis = pd.DataFrame(columns=["temas", "menciones_1h", "promedio_hora", "indice_crisis"])
else:
    crisis = conteo_1h.merge(baseline[["temas", "promedio_hora"]], on="temas", how="left")
    # Fill NaN for 'promedio_hora' if a 'temas' from conteo_1h is not in baseline
    crisis["promedio_hora"] = crisis["promedio_hora"].fillna(0)
    # Calculate 'indice_crisis', handling potential division by zero
    crisis["indice_crisis"] = crisis.apply(lambda row: row["menciones_1h"] / row["promedio_hora"] if row["promedio_hora"] != 0 else pd.NA, axis=1)

def clasificar_crisis(x):
    if x >= 3: return "CRISIS ALTA"
    if x >= 2: return "ALERTA TEMPRANA"
    return "NORMAL"

crisis["nivel_crisis"] = crisis["indice_crisis"].apply(clasificar_crisis)
alertas_crisis = crisis[crisis["nivel_crisis"] != "NORMAL"]

TEMAS_SENSIBLES = ["JEP", "Protesta social", "Victimas", "UNP"]

def riesgo(row):
    score = 0
    # Ensure 'temas' is iterable, handling cases where it might be missing or not a list
    temas = row.get("temas", [])
    if any(t in TEMAS_SENSIBLES for t in temas): score += 2
    if row["medio"] in ["El Tiempo","Semana","Noticias Caracol","Noticias RCN"]: score += 1
    if isinstance(temas, list) and len(temas) > 1: score += 1
    return score

# Conditionally apply the function only if df_24h is not empty.
# If df_24h is empty, we explicitly add an empty Series for the new column.
if not df_24h.empty:
    df_24h["indice_riesgo"] = df_24h.apply(riesgo, axis=1)
else:
    # When df_24h is empty, trying to assign a Series created by apply might
    # lead to unexpected behavior or the specific ValueError encountered.
    # Explicitly create an empty Series for the column.
    # The dtype should match the expected output of 'riesgo', which is integer.
    df_24h["indice_riesgo"] = pd.Series([], dtype='int64', index=df_24h.index)

import pandas as pd

if 'df_24h' in globals():
    if not df_24h.empty:
        df_24h["nivel_riesgo"] = pd.cut(
            df_24h["indice_riesgo"],
            bins=[0,2,4,6],
            labels=["Bajo","Medio","Alto"]
        )
    else:
        # If df_24h is empty, but defined, create an empty 'nivel_riesgo' column
        df_24h["nivel_riesgo"] = pd.Series([], dtype='category', index=df_24h.index)
else:
    print("Error: df_24h is not defined. Cannot calculate nivel_riesgo.")
    # Optionally, you might want to initialize df_24h here with expected columns if this path is unexpected

if 'df_24h' in globals():
    df_24h.to_excel("monitoreo_integral_24h.xlsx", index=False)
else:
    print("Error: df_24h is not defined. Cannot save 'monitoreo_integral_24h.xlsx'.")
alertas_crisis.to_excel("alertas_crisis_1h.xlsx", index=False)

df_24h.shape

alertas_crisis

df_24h["nivel_riesgo"].value_counts()

df_1h[["medio", "titulo", "temas"]]

df_1h["medio"].value_counts()

vista_temporal = (
    df_24h
    .explode("temas")
    .assign(
        hora=lambda x: x["fecha_publicacion"].dt.floor("H"),
        dia=lambda x: x["fecha_publicacion"].dt.date
    )
    .groupby(["dia", "hora", "temas", "medio"])
    .agg(
        menciones=("titulo", "count"),
        riesgo_promedio=("indice_riesgo", "mean")
    )
    .reset_index()
)

vista_temporal.head()

vista_tematica = (
    df_24h
    .explode("temas")
    .groupby(["temas"])
    .agg(
        total_menciones=("titulo", "count"),
        medios_distintos=("medio", "nunique"),
        riesgo_medio=("indice_riesgo", "mean")
    )
    .reset_index()
    .sort_values("total_menciones", ascending=False)
)

vista_tematica

vista_riesgo = (
    crisis
    .merge(
        vista_tematica[["temas", "riesgo_medio"]],
        on="temas",
        how="left"
    )
    .assign(
        nivel_riesgo=lambda x: pd.cut(
            x["riesgo_medio"],
            bins=[0,2,4,6],
            labels=["Bajo","Medio","Alto"]
        )
    )
)

vista_riesgo

vista_temporal.to_excel("vista_minable_temporal.xlsx", index=False)
vista_tematica.to_excel("vista_minable_tematica.xlsx", index=False)
vista_riesgo.to_excel("vista_minable_riesgo.xlsx", index=False)

print("Vistas minables exportadas correctamente")

!pip install nltk scikit-learn

import re
import nltk
from sklearn.feature_extraction.text import CountVectorizer
nltk.download('stopwords')
from nltk.corpus import stopwords

stop_es = set(stopwords.words("spanish"))

def limpiar_texto(texto):
    texto = str(texto).lower()
    texto = re.sub(r"http\S+|[^a-záéíóúñ\s]", "", texto)
    palabras = [p for p in texto.split() if p not in stop_es and len(p) > 2]
    return " ".join(palabras)

df_24h["texto_analisis"] = (
    df_24h["titulo"].fillna("") + " " + df_24h["resumen"].fillna("")
).apply(limpiar_texto)

vectorizer = CountVectorizer(
    ngram_range=(2,3),   # bigramas y trigramas
    min_df=2             # aparece al menos 2 veces
)

if not df_24h.empty:
    X = vectorizer.fit_transform(df_24h["texto_analisis"])
    narrativas = pd.DataFrame(
        X.toarray(),
        columns=vectorizer.get_feature_names_out()
    )
    narrativas["temas"] = df_24h["temas"].apply(lambda x: x[0] if len(x) > 0 else None) # Handle empty temas list
    narrativas["fecha"] = df_24h["fecha_publicacion"].dt.floor("H")
else:
    # If df_24h is empty, initialize narrativas as an empty DataFrame with expected columns
    narrativas = pd.DataFrame(columns=["temas", "fecha"]) # Add other expected columns if any

vista_narrativas = (
    narrativas
    .melt(id_vars=["temas","fecha"], var_name="narrativa", value_name="frecuencia")
    .query("frecuencia > 0")
    .groupby(["temas","fecha","narrativa"])
    .agg(menciones=("frecuencia","sum"))
    .reset_index()
    .sort_values("menciones", ascending=False)
)

vista_narrativas.head(10)

from datetime import datetime, timedelta, timezone
import pandas as pd

# Define vista_narrativas_1h first, filtering for the last hour
vista_narrativas_1h = vista_narrativas[vista_narrativas["fecha"] >= datetime.now(timezone.utc) - timedelta(hours=1)]

baseline_narr = (
    vista_narrativas
    .groupby(["temas","narrativa"])
    .agg(media=("menciones","mean"))
    .reset_index()
)

# Handle cases where vista_narrativas_1h or baseline_narr might be empty
if vista_narrativas_1h.empty or baseline_narr.empty:
    # Initialize picos_narrativos as an empty DataFrame with the expected columns
    picos_narrativos = pd.DataFrame(columns=["temas", "fecha", "narrativa", "menciones", "media", "indice_pico"])
else:
    picos_narrativos = vista_narrativas_1h.merge(
        baseline_narr,
        on=["temas","narrativa"],
        how="left"
    )
    # Fill NaN for 'media' if a narrative from vista_narrativas_1h is not in baseline_narr
    picos_narrativos["media"] = picos_narrativos["media"].fillna(0)

    # Calculate 'indice_pico', handling potential division by zero
    picos_narrativos["indice_pico"] = picos_narrativos.apply(
        lambda row: row["menciones"] / row["media"] if row["media"] != 0 else pd.NA, axis=1
    )
    picos_narrativos = picos_narrativos.sort_values("indice_pico", ascending=False)

picos_narrativos.head(10)

def nivel_narrativa(x):
    if x >= 3: return "CRÍTICA"
    if x >= 2: return "EMERGENTE"
    return "NORMAL"

picos_narrativos["nivel_narrativa"] = picos_narrativos["indice_pico"].apply(nivel_narrativa)
alertas_narrativas = picos_narrativos[picos_narrativos["nivel_narrativa"] != "NORMAL"]
alertas_narrativas

vista_narrativas.to_excel("vista_minable_narrativas.xlsx", index=False)
alertas_narrativas.to_excel("alertas_narrativas_1h.xlsx", index=False)

import os
import pandas as pd # Ensure pandas is imported if not already in the cell

RUTA_BASE = "base_historica_monitoreo.xlsx"

def actualizar_base(df_nuevo):
    # Ensure df_nuevo's datetime columns are timezone-naive before any concatenation
    for col in df_nuevo.columns:
        if pd.api.types.is_datetime64_any_dtype(df_nuevo[col]):
            if df_nuevo[col].dt.tz is not None:
                df_nuevo[col] = df_nuevo[col].dt.tz_localize(None)

    if os.path.exists(RUTA_BASE):
        base = pd.read_excel(RUTA_BASE)
        # Ensure 'fecha_publicacion' in the base is correctly parsed as datetime
        # and if it was timezone-aware, ensure it's handled consistently
        base["fecha_publicacion"] = pd.to_datetime(base["fecha_publicacion"], errors='coerce')
        # Also ensure base's other datetime columns (if any) are naive after reading
        for col in base.columns:
            if pd.api.types.is_datetime64_any_dtype(base[col]):
                if base[col].dt.tz is not None:
                    base[col] = base[col].dt.tz_localize(None)

        combinado = pd.concat([base, df_nuevo])
    else:
        combinado = df_nuevo.copy()

    combinado = combinado.drop_duplicates(
        subset=["medio", "titulo", "fecha_publicacion"]
    )

    # Final check on 'combinado' for any remaining timezone-aware datetimes (for robustness)
    for col in combinado.columns:
        if pd.api.types.is_datetime64_any_dtype(combinado[col]):
            if combinado[col].dt.tz is not None:
                combinado[col] = combinado[col].dt.tz_localize(None)

    combinado.to_excel(RUTA_BASE, index=False)
    return combinado

df_base = actualizar_base(df)
df_base.shape

!python monitoreo_medios.py

# feedparser
# pandas
# nltk
# scikit-learn
# openpyxl

# .github/workflows/monitoreo.yml

# name: Monitoreo de Medios

# on:
#   schedule:
#     - cron: '0 * * * *'   # cada hora
#   workflow_dispatch:

# jobs:
#   run-monitor:
#     runs-on: ubuntu-latest
#     steps:
#       - uses: actions/checkout@v3

#       - name: Set up Python
#         uses: actions/setup-python@v4
#         with:
#           python-version: '3.12'

#       - name: Install dependencies
#         run: pip install -r requirements.txt

#       - name: Run monitoring
#         run: python monitoreo_medios.py

import requests

TOKEN = "https://api.telegram.org/bot8036539281:AAHPbw_8qPHJoONYFY0fgB0yqj6lsH3YuM8/getUpdates"
CHAT_ID = 5522007396   # solo el número, sin comillas

def enviar_telegram(mensaje):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    data = {
        "chat_id": CHAT_ID,
        "text": mensaje
    }
    requests.post(url, data=data)

enviar_telegram("✅ Bot conectado correctamente")

RSS_FEEDS = {
    "El Tiempo": "https://www.eltiempo.com/rss/justicia.xml",
    "Semana": "https://www.semana.com/rss",
    "Caracol": "https://www.caracol.com.co/rss.aspx",
    "Blu Radio": "https://www.bluradio.com/rss",
    "RCN Radio": "https://www.rcnradio.com/rss",
    "La Silla Vacía": "https://www.lasillavacia.com/rss.xml"
}

import feedparser
import pandas as pd
from datetime import datetime, timedelta
import pytz

noticias = []

for medio, url in RSS_FEEDS.items():
    feed = feedparser.parse(url)
    for e in feed.entries:
        noticias.append({
            "medio": medio,
            "titulo": e.title,
            "link": e.link,
            "fecha": datetime(*e.published_parsed[:6], tzinfo=pytz.UTC)
        })

df = pd.DataFrame(noticias)

# Solo últimas 24 horas
ahora = datetime.now(pytz.UTC)
df = df[df["fecha"] >= ahora - timedelta(days=1)]

TEMAS = {
    "Víctimas": ["víctima", "reparación"],
    "JEP": ["jep", "justicia especial"],
    "Protesta social": ["protesta", "manifestación"],
    "Firmantes de paz": ["excombatiente", "firmante"],
    "Drogas": ["cultivos ilícitos", "narcotráfico"],
}

def clasificar(texto):
    texto = texto.lower()
    temas = []
    for t, palabras in TEMAS.items():
        if any(p in texto for p in palabras):
            temas.append(t)
    return temas if temas else ["Otros"]

df["temas"] = df["titulo"].apply(clasificar)

# Convert 'fecha' column to timezone-naive before saving to Excel
df["fecha"] = df["fecha"].dt.tz_localize(None)
df.to_excel("base_monitoreo.xlsx", index=False)

df["hora"] = df["fecha"].dt.floor("h")

crisis = (
    df.explode("temas")
      .groupby(["temas", "hora"])
      .size()
      .reset_index(name="menciones")
)

alertas = crisis[crisis["menciones"] >= 5]

for _, r in alertas.iterrows():
    enviar_telegram(
        f"🚨 ALERTA MEDIÁTICA\n"
        f"Tema: {r['temas']}\n"
        f"Menciones última hora: {r['menciones']}"
    )
