import streamlit as st
import pandas as pd
import json
import logging
import plotly.express as px
from Pipeline_data_streamlit import run_pipeline
import io
from fpdf import FPDF, XPos, YPos

fichier = st.file_uploader("Uploader votre fichier CSV contenant des données de ventes", type=["csv"])
logging.getLogger().setLevel(logging.WARNING)
@st.cache_data
def get_cached_data(df_brut, config):
    return run_pipeline(df_brut, config)


#Set up du fichier utilisateur

def generate_pdf(métriques, figures):
    pdf = FPDF()
    pdf.add_font("DejaVu","", "DejaVuSans.ttf")
    pdf.add_font("DejaVu", "B", "DejaVuSans.ttf")
    pdf.add_page()
    
    # Titre
    pdf.set_fill_color(41, 128, 185)  # Bleu professionnel
    pdf.rect(0, 0, 210, 40, 'F')     # Bandeau de titre
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("DejaVu", "B", 20)
    pdf.cell(0, 20, "RAPPORT ANALYTIQUE DES VENTES", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C")
    pdf.set_font("DejaVu", size=10)
    pdf.cell(0, 5, "Généré automatiquement via Dashboard Streamlit", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C")
    pdf.ln(20)
    
    # Métriques
    pdf.set_text_color(0, 0, 0)
    pdf.set_font("DejaVu", "B", 12)
    pdf.cell(0, 10, "Indicateurs clés", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(2)

    pdf.set_fill_color(245, 245, 245)
    pdf.set_font("DejaVu", size=11)

    for label, value in métriques.items():
        pdf.set_x(15)
        pdf.cell(180, 10, f"  {label} : {value}", new_x=XPos.LMARGIN, new_y=YPos.NEXT, fill=True, border='B')
        pdf.ln(2)
    pdf.ln(10)
    #Section graphiques
    pdf.set_font("DejaVu", "B", 14)
    pdf.cell(0, 10, "2. Visualisations de données", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(5)

    for titre, fig in figures.items():
        if pdf.get_y() > 200: 
            pdf.add_page()
            pdf.ln(10)
        img_bytes = fig.to_image(format="png", width=800, height=400, scale=2)
        img_stream = io.BytesIO(img_bytes)

        # Titre du graphique
        pdf.set_font("DejaVu", "B", 12)
        pdf.set_text_color(41, 128, 185)
        pdf.cell(0, 10, f"    > {titre}", ln=True)
        pdf.image(img_stream, x=15, w=180)
        pdf.ln(55)
    return bytes(pdf.output())

figures = {}

if fichier is not None:
    try:
        df_brut = pd.read_csv(fichier, encoding="utf-8-sig")
    except UnicodeDecodeError:
        # Si l'UTF-8 échoue, on tente l'encodage Excel/Windows standard
        df_brut = pd.read_csv(fichier, encoding="ISO-8859-1", sep=None, engine='python')
    
    with st.expander("Associer vos colonnes", expanded=True):
        st.info("Les noms de colonnes par défaut sont : Année, Mois, Chiffre d'affaires, Quantité, Produit etc... Lorsque vous cliquez sur le menu déroulant, les noms qui s'affichent sont ceux de votre fichier. Vous devez juste faire correspondre les bonnes colonnes entre elles.")
        st.info("En fonction de votre fichier, vous pouvez soit associer une colonne date complète (ex: 2026-04-11), auquel cas, merci de cocher la case correspondante, soit associer une colonne année et une colonne mois séparément.")
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
     #CONFIG POUR LE PIPELINE   
    mapping_rename = {
        col_ca: "Montant de la vente",
        col_qty: "Quantité commandée",
        col_produit: "PRODUCTCODE",
      
    }
    if not a_une_colonne_date:
        mapping_rename[col_annee] = "Année"
        mapping_rename[col_mois]='Mois'  
        
    if col_pays != "--Sélectionner":
        mapping_rename[col_pays] = "COUNTRY"
    
    groupby =["Année","Mois","Mois_num","Plage_Horaire","Période","PRODUCTCODE"]
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
        "date_column": col_date if a_une_colonne_date else None,
        "export_path": "export"
    }
    df = get_cached_data(df_brut, config)
    
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

df_filtré = df[df["Année"] == année_selectionnée]
df_mois = df[(df["Année"] == année_selectionnée) & (df["Mois"] == mois_sélectionné)]

#if mois_sélectionné == "Tous les mois":
 #   df_mois = df_filtré.copy()
#else:
 #   num_mois = [k for k, v in mois_labels.items() if v == mois_sélectionné][0]
  #  df_mois = df_filtré[df_filtré["Mois"] == num_mois]



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
ca_annuel =df_filtré["Montant de la vente"].sum()
quantité_annuelle =df_filtré["Quantité commandée"].sum()
prix_moyen = df["Prix Moyen Unitaire"].mean()
quantite_totale = df["Quantité commandée"].sum()
ca_mensuel = df_mois["Montant de la vente"].sum()
mois_index = list(mois_labels.values()).index(mois_sélectionné)
quantité_mensuelle= df_mois["Quantité commandée"].sum()


périodes = sorted(df["Période"].unique()) # Liste triée de toutes les périodes disponibles
période_actuelle = str(année_selectionnée) + str(mois_index + 1).zfill(2) # Période actuelle sélectionnée
idx = périodes.index(période_actuelle) # Index de la période actuelle dans la liste

#Year to year
année_précédente = année_selectionnée - 1
ca_année_précédente =  df[df["Année"] == année_précédente]["Montant de la vente"].sum()
variation_ca_annuel = (ca_annuel - ca_année_précédente) / ca_année_précédente * 100
pourcentage_ca_total = (ca_annuel / ca_total)*100
période_y_t_y = str(année_précédente) + str(mois_index + 1).zfill(2)
ca_yoy = None
delta_yoy = "N/A"


if période_y_t_y in périodes:
    ca_yoy = df[df["Période"] == période_y_t_y]["Montant de la vente"].sum()
    variation_yoy = (ca_mensuel - ca_yoy) / ca_yoy * 100
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

#Métrique année

col1, col2, col3 = st.columns(3)
with col1:
    st.metric(f"Chiffre d'affaires en {année_selectionnée}", f"{ca_annuel:,.0f} €")
with col2:
    st.metric(f"Pourcentage du chiffre d'affaires total", f"{pourcentage_ca_total:,.1f}%")
with col3:
    st.metric(f"Quantité vendue en {année_selectionnée}", f"{quantité_annuelle:,.0f}")

#Métriques du mois
période_précédente = périodes[idx - 1] if idx > 0 else None

if idx == 0:
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("CA du mois", f"{ca_mensuel:,.0f} €", delta="N/A")
    with col2:
        st.metric("CA même mois an-1", f"{ca_yoy:,.0f} €" if ca_yoy else "N/A", delta=delta_yoy)
    with col3:
        st.metric("Quantité vendue", f"{quantité_mensuelle:,.0f}", delta="N/A")
else:
    ca_précédent = df[df["Période"] == période_précédente]["Montant de la vente"].sum()
    quantité_précédent = df[df["Période"] == période_précédente]["Quantité commandée"].sum()
    variation = (ca_mensuel - ca_précédent) / ca_précédent * 100
    variation_quantité = (quantité_mensuelle - quantité_précédent) / quantité_précédent * 100

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("CA du mois", f"{ca_mensuel:,.0f} €", delta=f"{variation:,.1f} %")
    with col2:
        st.metric("CA même mois an-1", f"{ca_yoy:,.0f} €" if ca_yoy else "N/A", delta=delta_yoy)
    with col3:
        st.metric("Quantité vendue", f"{quantité_mensuelle:,.0f}", delta=f"{variation_quantité:,.1f} %")


## =========================== COURBE CA PAR MOIS ===============================
#================================================================================
st.subheader(f"Evolution du CA par mois au cours de l'année {année_selectionnée}")
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
include_fig_mois = st.checkbox("Inclure dans le rapport PDF", key="fig_mois")
if include_fig_mois:
    figures["Evolution du CA par mois"] = fig_mois


#Graphique de répartition par produit
st.subheader("Répartition du CA par produit (Statistiques annuelles)")

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
    title=f"Top {top_x} produits par CA au cours de l'année {année_selectionnée}"
)

    st.plotly_chart(fig)
include_fig_produit = st.checkbox("Inclure dans le rapport PDF", key="fig_produit")
if include_fig_produit:
    figures["Répartition du CA par produit"] = fig

with st.expander("Voir tous les produits"):
    st.dataframe(ca_par_produit)


#Meilleurs produits par mois
st.subheader("Meilleurs ventes de produits par mois")


with st.expander("Afficher le graphique", expanded = True):
    top_x2 = st.slider("Nombre de produits à afficher", min_value=3, max_value=20, value=11)


    best_seller_month = df_mois.groupby("PRODUCTCODE")["Quantité commandée"].sum().sort_values(ascending=False).reset_index()
    top_produits_mois = best_seller_month.head(top_x2)

    fig_mois_best=px.bar(
        top_produits_mois,
        x="PRODUCTCODE",
     y="Quantité commandée",
        labels={
        "PRODUCTCODE": "Produit",
        "Quantité commandée": "Quantité vendue"
    },
    title=f" Meilleurs ventes de produits par mois - {mois_sélectionné}",
)


    st.plotly_chart(fig_mois_best)

include_fig_best = st.checkbox("Inclure dans le rapport PDF", key="fig_mois_best")
if include_fig_best:
    figures["Meilleures ventes par mois"] = fig_mois_best

with st.expander("Voir tous les produits"):
    st.dataframe(best_seller_month)

if "Plage_Horaire" in df.columns:
    with st.expander("Afficher la répartition du CA par plage horaire", expanded = True):
        st.subheader("Répartition du CA par plage horaire")
        mode = st.radio("Style de graphique :", ["Empilé", "Côte à côte"], horizontal=True)
        barmode_value = "relative" if mode == "Empilé" else "group"
        ca_par_plage = df_mois.groupby(["Plage_Horaire", "PRODUCTCODE"])["Montant de la vente"].sum().reset_index()
        ca_par_plage = ca_par_plage.sort_values(by="Plage_Horaire", ascending=True)
        fig_produits_heure = px.bar(
        ca_par_plage,
        x="Plage_Horaire",
        y="Montant de la vente",
        color ="PRODUCTCODE",
        title = "Répartition du CA par plage horaire",
        labels={
            "Plage_Horaire": "Plage horaire",
            "Montant de la vente": "Chiffre d'affaires (€)"
        },
        barmode= barmode_value
    )
    st.plotly_chart(fig_produits_heure, use_container_width=True)
include_fig_plage = st.checkbox("Inclure dans le rapport PDF", key="fig_produit_heure")
if include_fig_plage:
    figures["Meilleures ventes de produits par heure"] = fig_produits_heure

def fig_to_bytes(fig):
    return fig.to_image(format="png", width=800, height=400)


métriques = {
    "Chiffre d'affaires total": f"{ca_total:,.0f} €",
    "Prix unitaire moyen": f"{prix_moyen:,.2f} €",
    "Quantités vendues": f"{quantite_totale:,.0f}",
    f"CA en {année_selectionnée}": f"{ca_annuel:,.0f} €",
    f"Pourcentage du CA total": f"{pourcentage_ca_total:,.1f}%",
    f"Quantité vendue en {année_selectionnée}": f"{quantité_annuelle:,.0f}",
    
}

try:
    pdf_bytes = generate_pdf(métriques, figures)
    st.download_button(
    label="Télécharger le rapport PDF",
    data=pdf_bytes,
    file_name=f"rapport_ventes_{année_selectionnée}.pdf",
    mime="application/pdf"
)
except Exception as e:
    st.error(f"Erreur lors de la génération du PDF : {e}")


