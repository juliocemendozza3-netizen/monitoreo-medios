import feedparser
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import requests
import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

def subir_a_drive(nombre_archivo):
    creds_json = os.environ.get("GOOGLE_DRIVE_JSON")
    print("Credenciales Drive detectadas:", bool(creds_json))

    if not creds_json:
        print("No hay credenciales de Drive")
        return

    info = json.loads(creds_json)
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/drive"]
    )

    servicio = build("drive", "v3", credentials=creds)

    # ID de la carpeta de Drive
    folder_id = "d/1Lq0tTUSnsBAoJ7OClP8DsdvPcNuCI3Fdviup-gBAteY/edit?gid=0#gid=0"

    archivo_metadata = {
        "name": nombre_archivo,
        "parents": [folder_id]
    }

    media = MediaFileUpload(nombre_archivo, resumable=True)

    servicio.files().create(
        body=archivo_metadata,
        media_body=media,
        fields="id"
    ).execute()

    print("Archivo subido a Drive correctamente")
    print("Archivo subido a Drive")

FUENTES = {
    "El Tiempo": "https://www.eltiempo.com/rss/colombia.xml",
    "El Espectador": "https://www.elespectador.com/rss/colombia/",
    "Semana": "https://www.semana.com/rss",
    "Caracol Radio": "https://caracol.com.co/rss/",
    "Blu Radio": "https://www.bluradio.com/rss.xml",
    "RCN Radio": "https://www.rcnradio.com/rss",  
    "Google News Colombia": "https://news.google.com/rss?hl=es-419&gl=CO&ceid=CO:es-419",
    "Google Paz": "https://news.google.com/rss/search?q=acuerdo+de+paz+colombia&hl=es-419&gl=CO&ceid=CO:es-419",
    "Google Protestas": "https://news.google.com/rss/search?q=protestas+colombia&hl=es-419&gl=CO&ceid=CO:es-419",
    "Google JEP": "https://news.google.com/rss/search?q=JEP+colombia&hl=es-419&gl=CO&ceid=CO:es-419",
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


def enviar_telegram(mensaje):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": mensaje}
    requests.post(url, data=data)


def clasificar(texto):
    texto = texto.lower()
    temas = []
    for t, palabras in TOPICOS.items():
        if any(p in texto for p in palabras):
            temas.append(t)
    return temas if temas else ["Otros"]

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
def recolectar_portadas():
    urls = {
        "El Tiempo portada": "https://www.eltiempo.com/",
        "Semana portada": "https://www.semana.com/",
        "El Espectador portada": "https://www.elespectador.com/"
    }

    noticias = []

    for medio, url in urls.items():
        try:
            r = requests.get(url, timeout=10)
            soup = BeautifulSoup(r.text, "html.parser")

            titulares = soup.find_all("h2")[:10]

            for t in titulares:
                texto = t.get_text(strip=True)
                if texto:
                    noticias.append({
                        "medio": medio,
                        "titulo": texto,
                        "fecha": datetime.now()
                    })
        except:
            pass

    return pd.DataFrame(noticias)
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


def main():
    print("Iniciando monitoreo...")
    enviar_telegram("🤖 Monitoreo ejecutado correctamente")

    df = recolectar()
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
    subir_a_drive("monitoreo.xlsx")
    print("Monitoreo terminado correctamente")


if __name__ == "__main__":
    main()
