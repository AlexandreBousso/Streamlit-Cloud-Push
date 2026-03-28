import streamlit as st
import pandas as pd
import json
import plotly.express as px

fichier = st.file_uploader("Uploadez votre fichier CSV", type=["csv"])

if fichier is not None:
    df = pd.read_csv(fichier, encoding="utf-8-sig")   
else:
    st.info("Merci d'uploader un fichier CSV pour commencer.")
    st.stop()  

try:
    df["Année"] = df["Année"].astype(int)
    df["Mois"] = df["Mois"].astype(int)
    df["Montant de la vente"] = df["Montant de la vente"].astype(float)
    df["Quantité commandée"] = df["Quantité commandée"].astype(float)
except ValueError:
    st.error("❌ Le fichier ne semble pas avoir le bon format. Vérifiez que le pipeline a bien été exécuté.")
    st.stop()

#---------------------- Préparation des données --------------------------------
#-----------------------------------------------------------------------------------------------

df["Mois"] = df["Mois"].astype(int)
mois_labels = {
    1: "Janvier", 2: "Février", 3: "Mars",
    4: "Avril", 5: "Mai", 6: "Juin",
    7: "Juillet", 8: "Août", 9: "Septembre",
    10: "Octobre", 11: "Novembre", 12: "Décembre"
}
df["Mois_num"] = df["Mois"].astype(int)
df["Mois"] = df["Mois"].map(mois_labels)
df["Période"] = df["Année"].astype(str) + df["Mois_num"].astype(str).str.zfill(2)
mois = list(mois_labels.values())
périodes = sorted(df["Période"].unique())

# ------------------- SLICERS ----------------------------------------
#------------------------------------------------------------------------------------------


df_filtré_pays = df.copy()


with st.sidebar:
    st.subheader("Vue générale")
    #Slicer Année
    années = sorted(df["Année"].unique().tolist())
    année_selectionnée = st.selectbox("Sélectionne une année", années)
    #Slicer Mois
    mois = list(mois_labels.values())
    mois_sélectionné = st.selectbox("Sélectionne un mois", mois)

    st.subheader("Vue par pays")
    pays = ["Tous les pays"] + sorted(df["COUNTRY"].unique().tolist())
    pays_sélectionné = st.selectbox("Pays", pays)
    #SLICER année/pays
    années_pays = ["Toutes les années"] + sorted(df["Année"].unique().tolist())
    année_pays = st.selectbox("Année", années_pays)
    #Slicer Mois/Pays
    mois_pays = ["Tous les mois"] + list(mois_labels.values())
    mois_pays_sélectionné = st.selectbox("Mois", mois_pays)






#DF Filtré Pays
df_filtré_pays = df[df["COUNTRY"] == pays_sélectionné]
#DF Filtré Année
df_filtré = df[df["Année"] == année_selectionnée]
#DF Filtré Mois
df_filtré_mois = df[(df["Année"] == année_selectionnée) & (df["Mois"] == mois_sélectionné)]


if année_pays != "Toutes les années":
    df_filtré_pays = df_filtré_pays[df_filtré_pays["Année"] == année_pays]

if mois_pays_sélectionné != "Tous les mois":
    df_filtré_pays = df_filtré_pays[df_filtré_pays["Mois"] == mois_pays_sélectionné]

if pays_sélectionné != "Tous les pays":
    df_filtré_pays = df_filtré_pays[df_filtré_pays["COUNTRY"] == pays_sélectionné]



ca_mois = df_filtré_mois["Montant de la vente"].sum()
mois_index = list(mois_labels.values()).index(mois_sélectionné)

# Liste triée de toutes les périodes disponibles
périodes = sorted(df["Période"].unique())
# Période actuelle sélectionnée
période_actuelle = str(année_selectionnée) + str(mois_index + 1).zfill(2)
# Index de la période actuelle dans la liste
idx = périodes.index(période_actuelle)





ca_total = df["Montant de la vente"].sum()
prix_moyen = df["Prix Moyen Unitaire"].mean()
quantite_totale = df["Quantité commandée"].sum()
quantité_mois= df_filtré_mois["Quantité commandée"].sum()


# ---------------- Affichage --------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------------------------------

st.title("Dashboard Ventes")

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Chiffre d'affaires total", f"{ca_total:,.0f} €")
with col2:
    st.metric("Prix unitaire moyen", f"{prix_moyen:,.2f} €")
with col3:
    st.metric("Quantités vendues", f"{quantite_totale:,.0f}")

if idx == 0:  # première période disponible, pas de précédent
    st.metric("CA du mois", f"{ca_mois:,.0f} €", delta="N/A")
    st.metric("Quantité vendue", f"{quantité_mois:,.0f}", delta="N/A")
else:
    période_précédente = périodes[idx - 1]

    ca_précédent = df[df["Période"] == période_précédente]["Montant de la vente"].sum()
    quantité_précédent = df[df["Période"] == période_précédente]["Quantité commandée"].sum()

    variation_quantité = (quantité_mois - quantité_précédent) / quantité_précédent * 100 
    variation = (ca_mois - ca_précédent) / ca_précédent * 100

    col1, col2 = st.columns(2)
    with col1:
        st.metric("CA du mois", f"{ca_mois:,.0f} €", delta=f"{variation:,.1f} %")
    with col2:
        st.metric("Quantité vendue", f"{quantité_mois:,.0f}", delta=f"{variation_quantité:,.1f} %")

st.subheader("Evolution du CA par mois")


# Graphique sur l'année filtrée
ca_par_mois = df_filtré.groupby(["Mois", "Mois_num"])["Montant de la vente"].sum().reset_index().sort_values("Mois_num")
fig_mois = px.line(
    ca_par_mois,
    x="Mois",
    y="Montant de la vente",
    labels={
        "Mois": "Mois",
        "Montant de la vente": "Chiffre d'affaires (€)"
    },
    title=f"Evolution du CA — {année_selectionnée}", markers= True
)

st.plotly_chart(fig_mois)

#Graphique de répartition par produit
st.subheader("Répartition du CA par produit")

ca_par_produit = df_filtré.groupby("PRODUCTCODE")["Montant de la vente"].sum().sort_values(ascending=False).reset_index()

fig = px.bar(
    ca_par_produit,
    x="PRODUCTCODE",
    y="Montant de la vente",
    labels={
        "PRODUCTCODE": "Produit",
        "Montant de la vente": "Chiffre d'affaires (€)"
    },
    title="Répartition du CA par produit"
)


st.plotly_chart(fig)

#Meilleurs produits par mois
st.subheader("Meilleurs ventes de produits par mois")


best_seller_month = df_filtré_mois.groupby("PRODUCTCODE")["Quantité commandée"].sum().sort_values(ascending=False).reset_index()
fig_mois=px.bar(
    best_seller_month,
    x="PRODUCTCODE",
    y="Quantité commandée",
    labels={
        "PRODUCTCODE": "Produit",
        "Quantité commandée": "Quantité vendue"
    },
    title=f" Meilleurs ventes de produits par mois - {mois_sélectionné}",
)

st.plotly_chart(fig_mois)