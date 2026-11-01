import altair as alt
import pandas as pd
import streamlit as st
from pymongo import MongoClient

SEUIL = 3.0  # doit être identique à celui de streaming.scala
JOURS = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]
MOIS = ["Jan", "Fév", "Mar", "Avr", "Mai", "Juin", "Juil", "Août", "Sep", "Oct", "Nov", "Déc"]
ZONES = {
    "cuisine_w": "Cuisine",
    "buanderie_w": "Buanderie",
    "chauffe_eau_clim_w": "Chauffe-eau / clim.",
    "reste_w": "Autres appareils (non mesurés)",
}
COLS = list(ZONES)

st.set_page_config(page_title="Consommation électrique", layout="wide")
st.title("Consommation électrique d'un foyer")


@st.cache_resource
def get_db():
    return MongoClient("mongodb://localhost:27017")["energie"]


db = get_db()

nb = st.sidebar.slider("Fenêtres affichées (temps réel)", 100, 2000, 400, step=100)
rafraichir = st.sidebar.slider("Rafraîchissement du temps réel (secondes)", 2, 30, 5)


@st.cache_data(ttl=300)
def charger(collection):
    docs = list(get_db()[collection].find({}, {"_id": 0}))
    return pd.DataFrame(docs)


def barres_empilees(df_large, nom_x, ordre):
    long = df_large.rename_axis(nom_x).reset_index().melt(
        id_vars=nom_x, var_name="Zone", value_name="W"
    )
    return alt.Chart(long).mark_bar().encode(
        x=alt.X(nom_x + ":N", sort=ordre, title=None),
        y=alt.Y("W:Q", title="Puissance moyenne (W)"),
        color=alt.Color("Zone:N", title="Zone"),
        tooltip=[nom_x, "Zone", alt.Tooltip("W:Q", format=".0f")],
    )


@st.fragment(run_every=rafraichir)
def temps_reel():
    docs = list(db.fenetres.find({}, {"_id": 0}).sort("debut", -1).limit(nb))
    if not docs:
        st.info("Aucune donnée pour l'instant. Lancez le streaming puis le producteur.")
        return
    df = pd.DataFrame(docs).sort_values("debut")
    df["debut"] = pd.to_datetime(df["debut"])
    df = df.set_index("debut")
    df["seuil"] = df["ref_moy"] + SEUIL * df["ref_std"]
    d = df.iloc[-1]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Puissance (moyenne 5 min)", f"{d['puissance_moy'] * 1000:.0f} W")
    c2.metric("Référence habituelle", f"{d['ref_moy'] * 1000:.0f} W")
    c3.metric("Z-score", f"{d['z']:.1f}")
    c4.metric("Fenêtres en alerte (total)", db.alertes.count_documents({}))
    st.caption("Dernière fenêtre analysée : " + str(df.index[-1]))

    st.subheader("Puissance active (kW) : mesure, référence et seuil d'alerte")
    st.line_chart(df[["puissance_moy", "ref_moy", "seuil"]])

    if "cuisine_w" in df.columns:
        z = df[["cuisine_w", "buanderie_w", "chauffe_eau_clim_w"]].dropna().copy()
        # écart non mesuré ; les rares valeurs négatives sont ramenées à 0
        z["reste_w"] = (df.loc[z.index, "puissance_moy"] * 1000 - z.sum(axis=1)).clip(lower=0)
        st.subheader("Répartition par partie de la maison (W, moyenne sur 5 min)")
        st.area_chart(z.rename(columns=ZONES))

    st.subheader("Dernières alertes")
    alertes = list(db.alertes.find({}, {"_id": 0}).sort("debut", -1).limit(20))
    if alertes:
        a = pd.DataFrame(alertes)[["debut", "fin", "puissance_moy", "ref_moy", "z"]]
        st.dataframe(a)
    else:
        st.write("Aucune alerte.")


def vue_semaine():
    hj = charger("par_heure_jour")
    if hj.empty:
        st.info("Données absentes : lancez d'abord le batch agregats.scala.")
        return
    hj = hj.copy()
    hj["Jour"] = hj["jour"].astype(int).map(lambda j: JOURS[j - 1])

    par_jour = hj.groupby("Jour")[COLS].mean().reindex(JOURS).rename(columns=ZONES)
    st.subheader("Puissance moyenne par jour de la semaine, par partie de la maison")
    st.altair_chart(barres_empilees(par_jour, "Jour", JOURS))

    st.subheader("Profil horaire d'un jour")
    jour = st.selectbox("Jour", JOURS)
    sel = hj[hj["Jour"] == jour].sort_values("heure").set_index("heure")[COLS]
    st.area_chart(sel.rename(columns=ZONES))

    st.subheader("Carte de chaleur : puissance totale moyenne (W)")
    chaleur = alt.Chart(hj).mark_rect().encode(
        x=alt.X("heure:O", title="Heure"),
        y=alt.Y("Jour:O", sort=JOURS, title=None),
        color=alt.Color("global_w:Q", title="W"),
        tooltip=["Jour", "heure", alt.Tooltip("global_w:Q", format=".0f")],
    )
    st.altair_chart(chaleur)


def vue_mensuelle():
    m = charger("par_mois")
    if m.empty:
        st.info("Données absentes : lancez d'abord le batch agregats.scala.")
        return
    m = m.sort_values(["annee", "mois"]).copy()
    m["periode"] = (
        m["annee"].astype(int).astype(str) + "-" + m["mois"].astype(int).astype(str).str.zfill(2)
    )

    st.subheader("Évolution mensuelle de la puissance moyenne (W)")
    st.area_chart(m.set_index("periode")[COLS].rename(columns=ZONES))
    st.caption("Décembre 2006 et novembre 2010 sont des mois partiels.")

    st.subheader("Saisonnalité : moyenne de chaque mois de l'année, toutes années confondues")
    saison = m.groupby("mois")[COLS].mean().rename(columns=ZONES)
    saison.index = [MOIS[int(i) - 1] for i in saison.index]
    st.altair_chart(barres_empilees(saison, "Mois", MOIS))


tab_reel, tab_semaine, tab_mois = st.tabs(["Temps réel", "Semaine", "Mensuel"])
with tab_reel:
    temps_reel()
with tab_semaine:
    vue_semaine()
with tab_mois:
    vue_mensuelle()