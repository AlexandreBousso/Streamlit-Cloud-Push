import streamlit as st
import pandas as pd
import json
import plotly.express as px
from Pipeline_data2 import run_pipeline

fichier = st.file_uploader("Uploader votre fichier CSV", type=["csv"])

#Set up du fichier utilisateur

if fichier is not None:
    df_brut = pd.read_csv(fichier, encoding="utf-8-sig")
    
    with st.expander("Associer vos colonnes", expanded=True):
        colonnes = df_brut.columns.tolist()
        a_une_colonne_date = st.checkbox("Mon fichier a une colonne date complète (ex: 2026-04-11)")

        col1, col2 = st.columns(2)
        with col1:
            col_annee = st.selectbox("Colonne Année", ["--Sélectionner"] + colonnes) if not a_une_colonne_date else None
            col_mois = st.selectbox("Colonne Mois", ["--Sélectionner"] + colonnes) if not a_une_colonne_date else None
            col_ca = st.selectbox("Colonne Chiffre d'affaires", ["--Sélectionner"] + colonnes)
        with col2:
            col_qty = st.selectbox("Colonne Quantité", ["--Sélectionner"] + colonnes)
            col_produit = st.selectbox("Colonne Produit", ["--Sélectionner"] + colonnes)
            col_pays = st.selectbox("Colonne Pays", ["--Sélectionner"] + colonnes)

        if a_une_colonne_date:
            col_date = st.selectbox("Colonne Date", ["--Sélectionner"] + colonnes)
        else:
            col_date = None


    if a_une_colonne_date:
        if "--Sélectionner" in [col_date, col_ca, col_qty, col_produit]:
            st.warning("Merci d'associer toutes les colonnes pour continuer.")
            st.stop()
    else:
        if "--Sélectionner" in [col_annee, col_mois, col_ca, col_qty, col_produit]:
            st.warning("Merci d'associer toutes les colonnes pour continuer.")
            st.stop()
        
    mapping_rename = {
        col_annee: "Année",
        col_mois: "Mois",
        col_ca: "Montant de la vente",
        col_qty: "Quantité commandée",
        col_produit: "PRODUCTCODE",
    }
    if col_pays != "--Sélectionner":
        mapping_rename[col_pays] = "COUNTRY"
    
    groupby =["Année","Mois","Mois_num","Période","PRODUCTCODE"]
    if col_pays != "--Sélectionner":
        groupby.append("COUNTRY")

    agg_logic = {
    "Montant de la vente": "sum",
    "Quantité commandée": "sum"}

    col_assign_conf = {
    "new_col_name": "Prix Moyen Unitaire",
    "arg1": "Montant de la vente",
    "arg2": "Quantité commandée"}

    config = {
        "mapping_rename": mapping_rename,
        "group_by": groupby,
        "agg_logic": agg_logic,
        "col_assign": col_assign_conf,
        "data_types": {},
        "date_column": None,
        "export_path": "export"
    }
    df= run_pipeline(df_brut, config)
    
else:
    st.info("Merci d'uploader un fichier CSV pour commencer.")
    st.stop()  




#---------------------- Préparation des données --------------------------------
#-----------------------------------------------------------------------------------------------

mois_labels = {
    1: "Janvier", 2: "Février", 3: "Mars",
    4: "Avril", 5: "Mai", 6: "Juin",
    7: "Juillet", 8: "Août", 9: "Septembre",
    10: "Octobre", 11: "Novembre", 12: "Décembre"
}
mois = list(mois_labels.values())
périodes = sorted(df["Période"].unique())

# ------------------- SLICERS ----------------------------------------
#------------------------------------------------------------------------------------------



#DF Filtré Pays
#if col_pays !="--Sélectionner":
 #   df_filtré_pays = df[df["COUNTRY"] == pays_sélectionné]

# ---------------- Affichage --------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------------------------------

st.title("Dashboard Ventes")

#Selectbox Année et mois disponibles dans l'année sélectionnée
années = sorted(df["Année"].unique().tolist())
année_selectionnée = st.selectbox("Sélectionner une année", années)
mois_disponibles = df[df["Année"]== année_selectionnée]["Mois"].unique()
mois_disponibles = [m for m in mois_labels.values() if m in mois_disponibles]
mois = ["Tous les mois"] + list(mois_labels.values())


#Selectbox Mois
mois_sélectionné = st.selectbox("Mois", mois_disponibles)

#DF filtrés
df_filtré = df[df["Année"] == année_selectionnée]
df_filtré_mois = df[(df["Année"] == année_selectionnée) & (df["Mois"] == mois_sélectionné)]

#Section Pays
if col_pays != "--Sélectionner":
    pays = ["Tous les pays"] + sorted(df["COUNTRY"].unique().tolist())
    pays_sélectionné = st.selectbox("Pays", pays)


    
    df_filtré_pays = df.copy()
    if pays_sélectionné != "Tous les pays":
        df_filtré_pays = df_filtré_pays[df_filtré_pays["COUNTRY"] == pays_sélectionné]
    #if année_selectionnée != "Toutes les années":
        #Remettre ici df_filtré pays si jamais 
    df_filtré_pays = df_filtré_pays[df_filtré_pays["Année"] == année_selectionnée]
    df_filtré_pays = df_filtré_pays[df_filtré_pays["Mois"] == mois_sélectionné]
else:
    pays_sélectionné ="Tous les pays"
    df_filtré_pays = df.copy()


#Calculs des KPIs
ca_total = df["Montant de la vente"].sum()
prix_moyen = df["Prix Moyen Unitaire"].mean()
quantite_totale = df["Quantité commandée"].sum()
ca_mois = df_filtré_mois["Montant de la vente"].sum()
mois_index = list(mois_labels.values()).index(mois_sélectionné)
quantité_mois= df_filtré_mois["Quantité commandée"].sum()


périodes = sorted(df["Période"].unique()) # Liste triée de toutes les périodes disponibles
période_actuelle = str(année_selectionnée) + str(mois_index + 1).zfill(2) # Période actuelle sélectionnée
idx = périodes.index(période_actuelle) # Index de la période actuelle dans la liste

#Year to year
année_précédente = année_selectionnée - 1
période_y_t_y = str(année_précédente) + str(mois_index + 1).zfill(2)
ca_yoy = None
delta_yoy = "N/A"


if période_y_t_y in périodes:
    ca_yoy = df[df["Période"] == période_y_t_y]["Montant de la vente"].sum()
    variation_yoy = (ca_mois - ca_yoy) / ca_yoy * 100
    delta_yoy = f"{variation_yoy:,.1f}%"
else:
    variation_yoy = None
    delta_yoy = "N/A"

#Display du total de ventes (toutes années confondues)
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Chiffre d'affaires total (toutes années confondues)", f"{ca_total:,.0f} €")
with col2:
    st.metric("Prix unitaire moyen des produits", f"{prix_moyen:,.2f} €")
with col3:
    st.metric("Quantités vendues (toutes années confondues)", f"{quantite_totale:,.0f}")

#Métriques du mois
période_précédente = périodes[idx - 1] if idx > 0 else None

if idx == 0:
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("CA du mois", f"{ca_mois:,.0f} €", delta="N/A")
    with col2:
        st.metric("CA même mois an-1", f"{ca_yoy:,.0f} €" if ca_yoy else "N/A", delta=delta_yoy)
    with col3:
        st.metric("Quantité vendue", f"{quantité_mois:,.0f}", delta="N/A")
else:
    ca_précédent = df[df["Période"] == période_précédente]["Montant de la vente"].sum()
    quantité_précédent = df[df["Période"] == période_précédente]["Quantité commandée"].sum()
    variation = (ca_mois - ca_précédent) / ca_précédent * 100
    variation_quantité = (quantité_mois - quantité_précédent) / quantité_précédent * 100

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("CA du mois", f"{ca_mois:,.0f} €", delta=f"{variation:,.1f} %")
    with col2:
        st.metric("CA même mois an-1", f"{ca_yoy:,.0f} €" if ca_yoy else "N/A", delta=delta_yoy)
    with col3:
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

with st.expander("Afficher le graphique", expanded = True):
    st.plotly_chart(fig_mois)

#Graphique de répartition par produit
st.subheader("Répartition du CA par produit")

with st.expander("Afficher le graphique", expanded = True):
    top_x = st.slider("Nombre de produits à afficher", min_value=3, max_value=20, value=10)

    ca_par_produit = df_filtré.groupby("PRODUCTCODE")["Montant de la vente"].sum().sort_values(ascending=False).reset_index()
    top_produits = ca_par_produit.head(top_x)

    fig = px.bar(
        top_produits,
        x="PRODUCTCODE",
        y="Montant de la vente",
        labels={
        "PRODUCTCODE": "Produit",
        "Montant de la vente": "Chiffre d'affaires (€)"
    },
    title=f"Top {top_x} produits par CA"
)

    st.plotly_chart(fig)

with st.expander("Voir tous les produits"):
    st.dataframe(ca_par_produit)


#Meilleurs produits par mois
st.subheader("Meilleurs ventes de produits par mois")


with st.expander("Afficher le graphique", expanded = True):
    top_x2 = st.slider("Nombre de produits à afficher", min_value=3, max_value=20, value=11)


    best_seller_month = df_filtré_mois.groupby("PRODUCTCODE")["Quantité commandée"].sum().sort_values(ascending=False).reset_index()
    top_produits_mois = best_seller_month.head(top_x2)

    fig_mois=px.bar(
        top_produits_mois,
        x="PRODUCTCODE",
     y="Quantité commandée",
        labels={
        "PRODUCTCODE": "Produit",
        "Quantité commandée": "Quantité vendue"
    },
    title=f" Meilleurs ventes de produits par mois - {mois_sélectionné}",
)


    st.plotly_chart(fig_mois)

with st.expander("Voir tous les produits"):
    st.dataframe(best_seller_month)
