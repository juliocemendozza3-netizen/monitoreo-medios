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
    "Google News Colombia": "https://news.google.com/rss/search?q=colombia&hl=es-419&gl=CO&ceid=CO:es-419",
    "Caracol Radio": "https://caracol.com.co/rss/",
    "Blu Radio": "https://www.bluradio.com/rss.xml",
    "RCN Radio": "https://www.rcnradio.com/rss",
    "Portafolio": "https://www.portafolio.co/files/rss/colombia.xml",
    "La República": "https://www.larepublica.co/rss/colombia"
}

# ---------------- CIUDADES ----------------
CIUDADES = {
    "Bogotá":["bogotá","bogota"],
    "Medellín":["medellín","medellin","antioquia"],
    "Cali":["cali","valle del cauca"],
    "Barranquilla":["barranquilla","atlántico"],
    "Cartagena":["cartagena","bolívar"],
    "Bucaramanga":["bucaramanga","santander"],
    "Cúcuta":["cúcuta","norte de santander"],
    "Pasto":["pasto","nariño"],
    "Manizales":["manizales","caldas"],
    "Pereira":["pereira","risaralda"],
    "Ibagué":["ibagué","tolima"],
    "Villavicencio":["villavicencio","meta"]
}

# ---------------- PALABRAS ----------------
TOPICOS = {
    "Seguridad":["homicidio","masacre","violencia","ataque"],
    "Política":["gobierno","congreso","presidente","reforma","ley"],
    "Protesta":["protesta","paro","manifestación","marchas"]
}

ACTORES = ["petro","gobierno","congreso","fiscalía","ministro","senado"]
NEGATIVAS = ["crisis","escándalo","corrupción","violencia","conflicto"]

# ---------------- FUNCIONES TEXTO ----------------
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

def es_colombia(t):
    t=str(t).lower()
    claves=["colombia","bogotá","medellín","cali","barranquilla","cartagena"]
    return any(x in t for x in claves)

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

# ---------------- GUARDAR SHEETS ESTABLE ----------------
def guardar_en_sheets(df):

    creds_json=os.environ.get("GOOGLE_DRIVE_JSON")
    if not creds_json:
        enviar_telegram("❌ Sin credenciales Google")
        return

    creds=Credentials.from_service_account_info(
        json.loads(creds_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    )

    ws=gspread.authorize(creds).open_by_key(
        "1Lq0tTUSnsBAoJ7OClP8DsdvPcNuCI3Fdviup-gBAteY"
    ).sheet1

    columnas=["medio","titulo","link","fecha","ciudad","temas","actores","tono"]

    for c in columnas:
        if c not in df.columns:
            df[c]=""

    df=df[columnas]

    # 🔴 LIMPIEZA DEFINITIVA ANTI JSON ERROR
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

# ---------------- ANALISIS ----------------
def alertas(df):
    if df.empty: return
    act=df["actores"].str.split(", ").explode().value_counts()
    if not act.empty and act.iloc[0]>=8:
        enviar_telegram(f"⚠️ Actor dominante: {act.index[0]}")

def resumen(df):
    if df.empty: return
    top=df["temas"].str.split(", ").explode().value_counts().head(3)
    msg="📊 Agenda reciente:\n"
    for t,v in top.items():
        if t!="Otros":
            msg+=f"• {t}: {v}\n"
    enviar_telegram(msg)

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

    df=df[df["titulo"].apply(es_colombia)]
    df["ciudad"]=df["titulo"].apply(detectar_ciudad)
    df["temas"]=df["titulo"].apply(clasificar)
    df["actores"]=df["titulo"].apply(actores)
    df["tono"]=df["titulo"].apply(tono)

    guardar_en_sheets(df)
    alertas(df)
    resumen(df)

    enviar_telegram("✅ Monitoreo actualizado")

if __name__=="__main__":
    main()
