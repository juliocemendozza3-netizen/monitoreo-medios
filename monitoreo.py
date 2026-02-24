import feedparser
import pandas as pd
import requests
from datetime import datetime
import os
import json
import gspread
from google.oauth2.service_account import Credentials

# ---------------- FUENTES NACIONALES + REGIONALES ----------------
FUENTES = {

    # Nacionales
    "El Tiempo": "https://www.eltiempo.com/rss/colombia.xml",
    "El Espectador": "https://www.elespectador.com/rss/colombia/",
    "Semana": "https://www.semana.com/rss",
    "Infobae": "https://www.infobae.com/america/colombia/rss.xml",
    "Google News Colombia": "https://news.google.com/rss?hl=es-419&gl=CO&ceid=CO:es-419",

    # Radio
    "Caracol Radio": "https://caracol.com.co/rss/",
    "Blu Radio": "https://www.bluradio.com/rss.xml",
    "RCN Radio": "https://www.rcnradio.com/rss",

    # Economía
    "Portafolio": "https://www.portafolio.co/files/rss/colombia.xml",
    "La República": "https://www.larepublica.co/rss/colombia",

    # Regionales
    "El Heraldo": "https://www.elheraldo.co/rss.xml",
    "El Universal": "https://www.eluniversal.com.co/rss.xml",
    "El Colombiano": "https://www.elcolombiano.com/rss",
    "El País Cali": "https://www.elpais.com.co/rss.xml",
    "Vanguardia": "https://www.vanguardia.com/rss",
    "La Patria": "https://www.lapatria.com/rss.xml",
    "La Opinión": "https://www.laopinion.com.co/rss.xml"
}

# ---------------- TEMAS ----------------
TOPICOS = {
    "Víctimas": ["víctima","reparación","desplazados","memoria histórica"],
    "JEP": ["jep","justicia transicional","acuerdo de paz","verdad"],
    "Protesta": ["protesta","paro","manifestación","bloqueo","marchas"],
    "Firmantes": ["reincorporación","excombatiente","farc"],
    "Drogas": ["narcotráfico","coca","erradicación","droga"],
    "Seguridad": ["homicidio","masacre","violencia","ataque","grupos armados"],
    "Política": ["gobierno","congreso","presidente","reforma","ley"]
}

# ---------------- ACTORES ----------------
ACTORES = [
    "petro","gobierno","congreso","fiscalía","corte",
    "ministro","senado","alcalde","gobernador",
    "partido","oposición","presidente"
]

# ---------------- TONO NEGATIVO ----------------
PALABRAS_NEGATIVAS = [
    "crisis","escándalo","polémica","corrupción",
    "investigación","conflicto","violencia"
]

TOKEN = "8036539281:AAHPbw_8qPHJoONYFY0fgB0yqj6lsH3YuM8"
CHAT_ID = "5522007396"

# ---------------- TELEGRAM ----------------
def enviar_telegram(mensaje):
    try:
        url = f"https://api.telegram.org/bot{8036539281:AAHPbw_8qPHJoONYFY0fgB0yqj6lsH3YuM8}/sendMessage"
        requests.post(url, data={"chat_id": 5522007396, "text": mensaje}, timeout=10)
    except:
        print("Telegram no respondió")

# ---------------- CLASIFICACION ----------------
def clasificar(texto):
    texto = str(texto).lower()
    temas = [t for t,pal in TOPICOS.items() if any(p in texto for p in pal)]
    if temas:
        relevancia = "INSTITUCIONAL"
    else:
        relevancia = "GENERAL"
    return ", ".join(temas) if temas else "Otros", relevancia

# ---------------- ACTORES ----------------
def detectar_actores(texto):
    texto = str(texto).lower()
    return ", ".join([a for a in ACTORES if a in texto])

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

# ---------------- MAIN ----------------
def main():

    enviar_telegram("🤖 Monitoreo ejecutado")

    df = recolectar()

    enviar_telegram(f"🧭 Se recolectaron {len(df)} noticias del RSS")

    if df.empty:
        enviar_telegram("⚠️ No se encontraron noticias nuevas")
        return

    # Clasificación
    df[["temas","relevancia"]] = df["titulo"].apply(
        lambda x: pd.Series(clasificar(x))
    )

    df["actores"] = df["titulo"].apply(detectar_actores)
    df["tono"] = df["titulo"].apply(detectar_tono)

    # Nivel crítico
    df["nivel"] = df.apply(
        lambda r: "CRITICO"
        if r["tono"]=="NEGATIVO" and r["relevancia"]=="INSTITUCIONAL"
        else "NORMAL",
        axis=1
    )

    # Alertas
    alertas = df[df["nivel"]=="CRITICO"]
    if len(alertas) > 0:
        enviar_telegram(f"🚨 {len(alertas)} noticias críticas detectadas")

    guardar_en_sheets(df)

    enviar_telegram("✅ Monitoreo terminado correctamente")

# ---------------- RUN ----------------
if __name__ == "__main__":
    main()
