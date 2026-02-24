import feedparser
import pandas as pd
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
import os
import json
import gspread
from google.oauth2.service_account import Credentials

# ---------------- TELEGRAM ----------------
TOKEN = "TU_TOKEN"
CHAT_ID = "TU_CHAT"

def enviar_telegram(msg):
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg}, timeout=10)
    except Exception as e:
        print("Telegram error:", e)

# ---------------- FUENTES ----------------
FUENTES = {
    "El Tiempo": "https://www.eltiempo.com/rss/colombia.xml",
    "El Espectador": "https://www.elespectador.com/rss/colombia/",
    "Semana": "https://www.semana.com/rss",
    "Infobae": "https://www.infobae.com/america/colombia/rss.xml",
    "Google News Colombia": "https://news.google.com/rss/search?q=colombia&hl=es-419&gl=CO&ceid=CO:es-419"
}

# ---------------- DICCIONARIOS ----------------
CIUDADES = {
    "Bogotá":["bogotá","bogota"],
    "Medellín":["medellín","medellin","antioquia"],
    "Cali":["cali","valle del cauca"],
    "Barranquilla":["barranquilla","atlántico"],
    "Cartagena":["cartagena","bolívar"]
}

TOPICOS = {
    "Seguridad":["homicidio","masacre","violencia","ataque"],
    "Política":["gobierno","congreso","presidente","reforma","ley"],
    "Protesta":["protesta","paro","manifestación","marchas"]
}

ACTORES = ["petro","gobierno","congreso","fiscalía","ministro"]
NEGATIVAS = ["crisis","escándalo","corrupción","violencia","conflicto"]

# ---------------- TEXTO ----------------
def limpiar_titulo(t):
    return " ".join(str(t).replace("\n"," ").split())

def procesar_google_news(titulo, medio):
    titulo = limpiar_titulo(titulo)
    if medio=="Google News Colombia" and " - " in titulo:
        p=titulo.rsplit(" - ",1)
        return p[1].strip(), p[0].strip()
    return medio, titulo

def detectar_ciudad(t):
    t=str(t).lower()
    for c,p in CIUDADES.items():
        if any(x in t for x in p):
            return c
    return "Nacional"

def clasificar(t):
    t=str(t).lower()
    temas=[k for k,v in TOPICOS.items() if any(p in t for p in v)]
    return ", ".join(temas) if temas else "Otros"

def actores(t):
    t=str(t).lower()
    return ", ".join([a for a in ACTORES if a in t])

def tono(t):
    t=str(t).lower()
    return "NEGATIVO" if any(p in t for p in NEGATIVAS) else "NEUTRO"

# ---------------- RECOLECTAR ----------------
def recolectar():
    datos=[]
    for medio,url in FUENTES.items():
        feed=feedparser.parse(url)
        for e in feed.entries:
            datos.append({
                "medio":medio,
                "titulo":e.title,
                "link":e.link,
                "fecha":datetime.now(ZoneInfo("America/Bogota")).strftime("%Y-%m-%d %H:%M")
            })
    df=pd.DataFrame(datos)
    df.drop_duplicates(subset=["titulo"], inplace=True)
    return df

# ---------------- DASHBOARD ----------------
def dashboard(client, df):

    try:
        sh=client.open_by_key("1Lq0tTUSnsBAoJ7OClP8DsdvPcNuCI3Fdviup-gBAteY")

        try:
            ws=sh.worksheet("Dashboard")
            ws.clear()
        except:
            ws=sh.add_worksheet(title="Dashboard",rows="50",cols="10")

        total=len(df)
        negativas=len(df[df["tono"]=="NEGATIVO"])

        top_ciudades=df["ciudad"].value_counts().head(5)
        top_temas=df["temas"].str.split(", ").explode().value_counts().head(5)

        data=[
            ["RESUMEN"],
            ["Total noticias",total],
            ["Negativas",negativas],
            [],
            ["TOP CIUDADES"]
        ]

        for c,v in top_ciudades.items():
            data.append([c,v])

        data.append([])
        data.append(["TOP TEMAS"])

        for t,v in top_temas.items():
            data.append([t,v])

        ws.update(values=data, range_name="A1")

    except Exception as e:
        enviar_telegram(f"⚠️ Dashboard error: {e}")

# ---------------- INDICE RIESGO ----------------
def indice_riesgo(df):
    if df.empty: return
    total=len(df)
    neg=len(df[df["tono"]=="NEGATIVO"])
    ratio=neg/total if total>0 else 0

    if ratio<0.15: nivel="🟢 BAJO"
    elif ratio<0.35: nivel="🟡 PRESIÓN"
    else: nivel="🔴 CRISIS"

    enviar_telegram(f"📊 Índice mediático\nNoticias:{total}\nNegativas:{neg}\nNivel:{nivel}")

# ---------------- GUARDAR ----------------
def guardar_en_sheets(df):

    creds_json=os.environ.get("GOOGLE_DRIVE_JSON")
    if not creds_json:
        enviar_telegram("❌ Sin credenciales Google")
        return

    creds=Credentials.from_service_account_info(
        json.loads(creds_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    )

    client=gspread.authorize(creds)
    ws=client.open_by_key("1Lq0tTUSnsBAoJ7OClP8DsdvPcNuCI3Fdviup-gBAteY").sheet1

    columnas=["medio","titulo","link","fecha","ciudad","temas","actores","tono"]

    for c in columnas:
        if c not in df.columns:
            df[c]=""

    df=df[columnas]
    df=df.replace([float("inf"),float("-inf")],"")
    df=df.fillna("")
    df=df.astype(str)

    existentes=ws.get_all_values()

    if existentes:
        old=pd.DataFrame(existentes[1:],columns=existentes[0])
        for c in columnas:
            if c not in old.columns:
                old[c]=""
        old=old[columnas]
        df=pd.concat([old,df],ignore_index=True)

    df.drop_duplicates(subset=["titulo"], inplace=True)
    ws.update(values=[columnas]+df.values.tolist(), range_name="A1")

    dashboard(client, df)

# ---------------- MAIN ----------------
def main():

    enviar_telegram("🤖 Monitoreo ejecutado")

    df=recolectar()
    if df.empty:
        enviar_telegram("⚠️ Sin noticias")
        return

    df[["medio","titulo"]]=df.apply(
        lambda r: pd.Series(procesar_google_news(r["titulo"],r["medio"])),
        axis=1
    )

    df["ciudad"]=df["titulo"].apply(detectar_ciudad)
    df["temas"]=df["titulo"].apply(clasificar)
    df["actores"]=df["titulo"].apply(actores)
    df["tono"]=df["titulo"].apply(tono)

    guardar_en_sheets(df)
    indice_riesgo(df)

    enviar_telegram("✅ Monitoreo actualizado con dashboard y riesgo")

if __name__=="__main__":
    main()
