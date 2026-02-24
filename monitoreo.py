import feedparser
import pandas as pd
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
import os
import json
import gspread
from google.oauth2.service_account import Credentials

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

# ---------------- TEMAS ----------------
TOPICOS = {
    "Víctimas":["víctima","reparación","desplazados","memoria histórica"],
    "JEP":["jep","justicia transicional","acuerdo de paz","verdad"],
    "Protesta":["protesta","paro","manifestación","bloqueo","marchas"],
    "Firmantes":["reincorporación","excombatiente","farc"],
    "Drogas":["narcotráfico","coca","erradicación","droga"],
    "Seguridad":["homicidio","masacre","violencia","ataque"],
    "Política":["gobierno","congreso","presidente","reforma","ley"]
}

ACTORES = [
    "petro","gobierno","congreso","fiscalía","corte",
    "ministro","senado","alcalde","gobernador","partido"
]

PALABRAS_NEGATIVAS = [
    "crisis","escándalo","polémica","corrupción",
    "investigación","conflicto","violencia"
]

TOKEN = "TU_TOKEN"
CHAT_ID = "TU_CHAT"

# ---------------- TELEGRAM ----------------
def enviar_telegram(msg):
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg}, timeout=10)
    except:
        pass

# ---------------- UTILIDADES ----------------
def limpiar_titulo(texto):
    return " ".join(str(texto).replace("\n"," ").split())

def procesar_google_news(titulo, medio):
    titulo = limpiar_titulo(titulo)
    if medio == "Google News Colombia" and " - " in titulo:
        partes = titulo.rsplit(" - ",1)
        return partes[1].strip(), partes[0].strip()
    return medio, titulo

def detectar_ciudad(texto):
    texto = str(texto).lower()
    for ciudad, palabras in CIUDADES.items():
        if any(p in texto for p in palabras):
            return ciudad
    return "Nacional"

def es_colombia(texto):
    texto = str(texto).lower()
    claves = ["colombia","bogotá","medellín","cali","barranquilla","cartagena"]
    return any(p in texto for p in claves)

def clasificar(texto):
    texto = str(texto).lower()
    temas = [t for t,pal in TOPICOS.items() if any(p in texto for p in pal)]
    return ", ".join(temas) if temas else "Otros"

def detectar_actores(texto):
    texto = str(texto).lower()
    return ", ".join([a for a in ACTORES if a in texto])

def detectar_tono(texto):
    texto = str(texto).lower()
    return "NEGATIVO" if any(p in texto for p in PALABRAS_NEGATIVAS) else "NEUTRO"

# ---------------- RECOLECCION ----------------
def recolectar():
    noticias=[]
    for medio,url in FUENTES.items():
        feed=feedparser.parse(url)
        for e in feed.entries:
            noticias.append({
                "medio":medio,
                "titulo":e.title,
                "link":e.link,
                "fecha":datetime.now(ZoneInfo("America/Bogota")).strftime("%Y-%m-%d %H:%M")
            })
    df=pd.DataFrame(noticias)
    df.drop_duplicates(subset=["titulo"], inplace=True)
    return df

# ---------------- GOOGLE SHEETS ESTABLE ----------------
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

    # 🔴 Forzar mismas columnas siempre
    columnas=["medio","titulo","link","fecha","ciudad","temas","actores","tono"]
    for col in columnas:
        if col not in df.columns:
            df[col]=""

    df=df[columnas]

    # 🔴 limpiar valores incompatibles
    df=df.replace([float("inf"),float("-inf")],"")
    df=df.fillna("")
    df=df.astype(str)

    existentes=ws.get_all_values()

    if existentes:
        df_old=pd.DataFrame(existentes[1:], columns=existentes[0])
        for col in columnas:
            if col not in df_old.columns:
                df_old[col]=""
        df_old=df_old[columnas]
        df=pd.concat([df_old,df], ignore_index=True)

    df.drop_duplicates(subset=["titulo"], inplace=True)

    ws.update(values=[columnas]+df.values.tolist(), range_name="A1")

# ---------------- MAIN ----------------
def main():

    enviar_telegram("🤖 Monitoreo ejecutado")

    df=recolectar()
    if df.empty:
        enviar_telegram("⚠️ Sin noticias")
        return

    df[["medio","titulo"]]=df.apply(lambda r: pd.Series(procesar_google_news(r["titulo"],r["medio"])),axis=1)
    df=df[df["titulo"].apply(es_colombia)]
    df["ciudad"]=df["titulo"].apply(detectar_ciudad)
    df["temas"]=df["titulo"].apply(clasificar)
    df["actores"]=df["titulo"].apply(detectar_actores)
    df["tono"]=df["titulo"].apply(detectar_tono)

    guardar_en_sheets(df)

    enviar_telegram("✅ Monitoreo estable y actualizado")

# ---------------- RUN ----------------
if __name__=="__main__":
    main()
