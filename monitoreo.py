import feedparser
import pandas as pd
import requests
from datetime import datetime
import os
import json
import gspread
from google.oauth2.service_account import Credentials

# ---------------- FUENTES AMPLIADAS ----------------
FUENTES = {
    "El Tiempo": "https://www.eltiempo.com/rss/colombia.xml",
    "El Espectador": "https://www.elespectador.com/rss/colombia/",
    "Semana": "https://www.semana.com/rss",
    "Caracol Radio": "https://caracol.com.co/rss/",
    "Blu Radio": "https://www.bluradio.com/rss.xml",
    "RCN Radio": "https://www.rcnradio.com/rss",
    "Infobae": "https://www.infobae.com/america/colombia/rss.xml",
    "Portafolio": "https://www.portafolio.co/files/rss/colombia.xml",
    "La República": "https://www.larepublica.co/rss/colombia",
    "Google News Colombia": "https://news.google.com/rss?hl=es-419&gl=CO&ceid=CO:es-419"
}

# ---------------- TEMAS AMPLIADOS ----------------
TOPICOS = {
    "Víctimas": [
        "víctima", "reparación", "unidad de víctimas",
        "conflicto armado", "desplazados", "memoria histórica"
    ],
    "JEP": [
        "jep", "jurisdicción especial", "justicia transicional",
        "tribunal de paz", "acuerdo de paz", "verdad"
    ],
    "Protesta social": [
        "protesta", "paro", "movilización",
        "manifestación", "bloqueo", "marchas"
    ],
    "Firmantes de paz": [
        "excombatiente", "reincorporación",
        "firmantes", "desmovilizados", "farc"
    ],
    "Drogas": [
        "cultivos ilícitos", "narcotráfico",
        "coca", "erradicación", "droga"
    ],
    "Seguridad": [
        "ataque", "homicidio", "masacre",
        "violencia", "grupos armados",
        "asesinato", "enfrentamiento"
    ],
    "Política": [
        "gobierno", "congreso", "ministro",
        "presidente", "senado", "reforma",
        "ley", "debate político"
    ]
}

# ---------------- ACTORES ----------------
ACTORES = [
    "petro", "gobierno", "congreso", "fiscalía",
    "corte", "ministro", "senado",
    "alcalde", "gobernador", "partido",
    "oposición", "presidente"
]

# ---------------- TONO NEGATIVO ----------------
PALABRAS_NEGATIVAS = [
    "crisis", "denuncia", "escándalo", "polémica",
    "ataque", "violencia", "corrupción",
    "irregular", "investigación",
    "conflicto", "paro", "protesta"
]

TOKEN = "8036539281:AAHPbw_8qPHJoONYFY0fgB0yqj6lsH3YuM8"
CHAT_ID = "5522007396"

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
    return ", ".join(temas) if temas else "Otros"

# ---------------- ACTORES ----------------
def detectar_actores(texto):
    texto = str(texto).lower()
    encontrados = [a for a in ACTORES if a in texto]
    return ", ".join(encontrados)

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
    df = pd.DataFrame(noticias)
    df.drop_duplicates(subset=["titulo"], inplace=True)
    return df

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
    enviar_telegram("🤖 Monitoreo ejecutado")

    df = recolectar()
    if df.empty:
        print("No se recolectaron noticias")
        return

    df["temas"] = df["titulo"].apply(clasificar)
    df["actores"] = df["titulo"].apply(detectar_actores)
    df["tono"] = df["titulo"].apply(detectar_tono)

    # Alertas temáticas
    crisis = df[df["temas"] != "Otros"].groupby("temas").size().reset_index(name="menciones")
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
