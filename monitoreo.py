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

TOPICOS = {
    "Víctimas": ["víctima", "reparación"],
    "JEP": ["jep", "justicia especial"],
    "Protesta social": ["protesta", "manifestación"],
    "Firmantes de paz": ["excombatiente", "firmante"],
    "Drogas": ["cultivos ilícitos", "narcotráfico"],
}

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
    temas = []
    for t, palabras in TOPICOS.items():
        if any(p in texto for p in palabras):
            temas.append(t)
    return temas if temas else ["Otros"]

# ---------------- RECOLECCION RSS ----------------
def recolectar():
    noticias = []
    for medio, url in FUENTES.items():
        feed = feedparser.parse(url)
        for e in feed.entries:
            noticias.append({
                "medio": medio,
                "titulo": e.title,
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

    # 🔴 ARREGLO CLAVE: convertir todo a texto
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
        print("No hay noticias")
        return

    df["temas"] = df["titulo"].apply(clasificar)

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
