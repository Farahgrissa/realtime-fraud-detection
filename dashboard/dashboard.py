"""Dashboard Streamlit : affiche les transactions notées par streaming.py (MongoDB).

Lancer :  streamlit run dashboard.py
"""
import time

import pandas as pd
import streamlit as st
from pymongo import MongoClient

st.set_page_config(page_title="Détection de fraude", page_icon="🛡️", layout="wide")
st.title("🛡️ Détection de fraude en temps réel")

# ---------------------------------------------------------------- Sidebar
with st.sidebar:
    st.header("Paramètres")
    mongo_uri = st.text_input("MongoDB URI", "mongodb://localhost:27017")
    db_name = st.text_input("Base", "fraud")
    coll_name = st.text_input("Collection", "transactions")
    limit = st.number_input("Nb max de transactions chargées", 1000, 500000, 50000, step=5000)
    threshold = st.slider("Seuil de probabilité d'alerte", 0.0, 1.0, 0.5, 0.05,
                          help="Une transaction est affichée comme alerte si probability >= seuil")
    auto = st.checkbox("Rafraîchissement automatique", value=True)
    every = st.slider("Intervalle (secondes)", 2, 60, 5)
    if st.button("Rafraîchir maintenant"):
        st.cache_data.clear()


# ---------------------------------------------------------------- Données
@st.cache_data(ttl=2)
def load(uri, db, coll, limit):
    client = MongoClient(uri, serverSelectionTimeoutMS=3000)
    try:
        docs = list(client[db][coll].find({}, {"_id": 0}).sort("_id", -1).limit(int(limit)))
    finally:
        client.close()
    return pd.DataFrame(docs)


try:
    df = load(mongo_uri, db_name, coll_name, limit)
except Exception as e:
    st.error("Connexion à MongoDB impossible : %s" % e)
    st.stop()

if df.empty:
    st.info("Aucune transaction pour l'instant. Lancez streaming.py et déposez des fichiers dans /stream/in.")
    if auto:
        time.sleep(every)
        st.rerun()
    st.stop()

df["processed_at"] = pd.to_datetime(df["processed_at"], errors="coerce")
df["alert"] = (df["probability"] >= threshold).astype(int)

# ---------------------------------------------------------------- KPIs
total = len(df)
n_alerts = int(df["alert"].sum())
n_real = int(df["isFraud"].sum())
tp = int(((df["alert"] == 1) & (df["isFraud"] == 1)).sum())
precision = tp / n_alerts if n_alerts else 0.0
recall = tp / n_real if n_real else 0.0

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Transactions", f"{total:,}")
c2.metric("Alertes", f"{n_alerts:,}", f"{n_alerts / total:.2%} du total")
c3.metric("Fraudes réelles", f"{n_real:,}")
c4.metric("Precision", f"{precision:.1%}")
c5.metric("Recall", f"{recall:.1%}")

st.divider()

# ---------------------------------------------------------------- Graphiques
left, right = st.columns(2)

with left:
    st.subheader("Transactions par type")
    by_type = (df.groupby(["type", "alert"]).size().unstack(fill_value=0)
                 .rename(columns={0: "normale", 1: "alerte"}))
    st.bar_chart(by_type)

with right:
    st.subheader("Alertes au cours du temps")
    ts = df.dropna(subset=["processed_at"]).set_index("processed_at")
    if not ts.empty:
        st.line_chart(ts["alert"].resample("10s").sum().rename("alertes"))

left2, right2 = st.columns(2)

with left2:
    st.subheader("Distribution de la probabilité de fraude")
    hist = pd.cut(df["probability"], bins=[i / 10 for i in range(11)], include_lowest=True)
    st.bar_chart(hist.value_counts().sort_index().rename(lambda x: str(x)))

with right2:
    st.subheader("Matrice de confusion (alerte vs fraude réelle)")
    cm = pd.crosstab(df["isFraud"].map({0: "Réel : normale", 1: "Réel : fraude"}),
                     df["alert"].map({0: "Prédit : normale", 1: "Prédit : fraude"}))
    st.dataframe(cm, use_container_width=True)

# ---------------------------------------------------------------- Tableaux
st.divider()
st.subheader("🚨 Dernières alertes")
cols = ["processed_at", "step", "type", "amount", "nameOrig", "nameDest", "isFraud", "probability"]
alerts = df[df["alert"] == 1].sort_values("processed_at", ascending=False)
st.dataframe(alerts[cols].head(100), use_container_width=True, hide_index=True)

with st.expander("Dernières transactions (toutes)"):
    st.dataframe(df[cols + ["prediction"]].head(200), use_container_width=True, hide_index=True)

# ---------------------------------------------------------------- Auto-refresh
if auto:
    time.sleep(every)
    st.rerun()