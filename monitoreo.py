# === TODO TU SCRIPT IGUAL HASTA guardar_en_sheets ===
# (no cambio nada arriba para no romper lo estable)

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

    columnas=["medio","titulo","link","fecha","ciudad","temas","actores","tono"]

    for col in columnas:
        if col not in df.columns:
            df[col]=""

    df=df[columnas]
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


# ================== NUEVAS FUNCIONES ==================

def resumen_semanal(df):

    if df.empty:
        return

    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    semana = df[df["fecha"] >= (pd.Timestamp.now() - pd.Timedelta(days=7))]

    if semana.empty:
        return

    temas = semana["temas"].str.split(", ").explode().value_counts().head(5)
    ciudades = semana["ciudad"].value_counts().head(5)
    actores = semana["actores"].str.split(", ").explode().value_counts().head(5)
    negativas = semana[semana["tono"]=="NEGATIVO"].shape[0]

    msg="📊 AGENDA SEMANAL\n\n"

    msg+="🗞 Temas:\n"
    for t,v in temas.items():
        if t!="Otros":
            msg+=f"• {t}: {v}\n"

    msg+="\n🏙 Ciudades:\n"
    for c,v in ciudades.items():
        msg+=f"• {c}: {v}\n"

    msg+="\n🧑 Actores:\n"
    for a,v in actores.items():
        if a:
            msg+=f"• {a}: {v}\n"

    msg+=f"\n🚨 Noticias negativas: {negativas}"

    enviar_telegram(msg)


def alertas_inteligentes(df):

    if df.empty:
        return

    actores=df["actores"].str.split(", ").explode().value_counts()
    if not actores.empty and actores.iloc[0]>=10:
        enviar_telegram(f"⚠️ Actor dominante: {actores.index[0]} ({actores.iloc[0]} menciones)")

    negativas=df[df["tono"]=="NEGATIVO"]["ciudad"].value_counts()
    if not negativas.empty and negativas.iloc[0]>=6:
        enviar_telegram(f"🚨 Alta negatividad en {negativas.index[0]} ({negativas.iloc[0]} noticias)")

    temas=df["temas"].str.split(", ").explode().value_counts()
    if not temas.empty and temas.iloc[0]>=15:
        enviar_telegram(f"📈 Tema dominante: {temas.index[0]} ({temas.iloc[0]} menciones)")


# ================== MAIN ==================

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

    # 🔴 NUEVO
    alertas_inteligentes(df)
    resumen_semanal(df)

    enviar_telegram("✅ Monitoreo estable y actualizado")


if __name__=="__main__":
    main()
