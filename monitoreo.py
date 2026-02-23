import feedparser
import pandas as pd
import requests
from datetime import datetime
import os
import json
import gspread
from google.oauth2.service_account import Credentials

# ---------------- FUENTES ----------------
FUENTES = {
    "El Tiempo": "https://www.eltiempo.com/rss/colombia.xml",
    "El Espectador": "https://www.elespectador.com/rss/colombia/",
    "Semana": "https://www.semana.com/rss",
    "Caracol Radio": "https://caracol.com.co/rss/",
    "Blu Radio": "https://www.bluradio.com/rss.xml",
    "RCN Radio": "https://www.rcnradio.com/rss",
    "Google News Colombia": "https://news.google.com/rss?hl=es-419&gl=CO&ceid=CO:es-419"
}

# ---------------- TEMAS INSTITUCIONALES ----------------
TOPICOS = {
    "Víctimas": ["víctima", "reparación", "unidad de víctimas"],
    "JEP": ["jep", "jurisdicción especial"],
    "Protesta social": ["protesta", "paro", "movilización"],
    "Firmantes de paz": ["excombatiente", "reincorporación"],
    "Drogas": ["cultivos ilícitos", "narcotráfico"],
    "Seguridad": ["ataque", "homicidio", "masacre", "violencia", "grupos armados"],
    "Política": ["gobierno", "congreso", "ministro", "presidente", "senado"]
}

# ---------------- ACTORES POLÍTICOS ----------------
ACTORES = [
    "petro", "gobierno", "congreso", "fiscalía", "corte", "ministro",
    "senado", "alcalde", "gobernador", "partido", "oposición"
]

# ---------------- TONO NEGATIVO ----------------
PALABRAS_NEGATIVAS = [
    "crisis", "denuncia", "escándalo", "polémica",
    "ataque", "violencia", "corrupción", "irregular",
    "investigación", "conflicto", "paro", "protesta"
]

TOKEN = "8006599024:AAGrWiOsP5TvwMnAay6h1bSxlMPNzahPosM"
CHAT_ID = "8006599024"

# ---------------- TELEGRAM ----------------
def enviar_telegram(mensaje):
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": mensaje}, timeout=10)
    except:
        print("Telegram no respondió")

# ---------------- CLASIFICACION ----------------
def clasificar(texto):
    texto = str(texto).lower()
    temas = [t for t, palabras in TOPICOS.items() if any(p in texto for p in palabras)]
    return temas if temas else ["Otros"]

# ---------------- ACTORES ----------------
def detectar_actores(texto):
    texto = str(texto).lower()
    encontrados = [a for a in ACTORES if a in texto]
    return ", ".join(encontrados) if encontrados else ""

# ---------------- TONO ----------------
def detectar_tono(texto):
    texto = str(texto).lower()
    if any(p in texto for p in PALABRAS_NEGATIVAS):
        return "NEGATIVO"
    return "NEUTRO"

# ---------------- RECOLECCION ----------------
def recolectar():
    noticias = []
    for medio, url in FUENTES.items():
        feed = feedparser.parse(url)
        for e in feed.entries:
            noticias.append({
                "medio": medio,
                "titulo": e.title,
                "link": e.link,
                "fecha": datetime.now()
            })
    return pd.DataFrame(noticias)

# ---------------- GOOGLE SHEETS ----------------
def guardar_en_sheets(df):
    creds_json = os.environ.get("GOOGLE_DRIVE_JSON")
    if not creds_json:
        print("No hay credenciales")
        return

    info = json.loads(creds_json)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = Credentials.from_service_account_info(info, scopes=scopes)
    client = gspread.authorize(creds)

    sheet_id = "1Lq0tTUSnsBAoJ7OClP8DsdvPcNuCI3Fdviup-gBAteY"
    sh = client.open_by_key(sheet_id)
    ws = sh.sheet1

    df = df.astype(str)
    ws.clear()
    ws.update([df.columns.values.tolist()] + df.values.tolist())

    print("Datos actualizados en Google Sheets")

# ---------------- MAIN ----------------
def main():
    print("Iniciando monitoreo...")
    enviar_telegram("🤖 Monitoreo ejecutado correctamente")

    df = recolectar()
    if df.empty:
        return

    df["temas"] = df["titulo"].apply(clasificar)
    df = df[df["temas"].apply(lambda x: x != ["Otros"])]

    df["actores"] = df["titulo"].apply(detectar_actores)
    df["tono"] = df["titulo"].apply(detectar_tono)

    crisis = (
        df.explode("temas")
        .groupby("temas")
        .size()
        .reset_index(name="menciones")
    )

    alertas = crisis[crisis["menciones"] >= 5]

    for _, r in alertas.iterrows():
        enviar_telegram(
            f"🚨 ALERTA MEDIÁTICA\nTema: {r['temas']}\nMenciones: {r['menciones']}"
        )

    df.to_excel("monitoreo.xlsx", index=False)
    guardar_en_sheets(df)

    print("Monitoreo terminado correctamente")

# ---------------- RUN ----------------
if __name__ == "__main__":
    main()
