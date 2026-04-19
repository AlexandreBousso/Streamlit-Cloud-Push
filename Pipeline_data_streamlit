import pandas as pd
import os
import requests
import json
import logging
import numpy as np
import streamlit as st



# ==========================================
# 1. CONFIGURATION DU LOGGING (Le Journal)
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)

# ==========================================
# 2. GESTION DES CHEMINS (Le GPS)
# ==========================================

def load_config(path: str = "config.json") -> dict:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(BASE_DIR, path)  # on utilise path
    try:
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            config = json.load(f)
            logging.info(" Configuration chargée avec succès.")
            return config
    except FileNotFoundError as e:
        logging.error(f" Impossible de charger la config : {e}")
        exit(1)
    except Exception as e:
        logging.error(f"Erreur lors du chargement de la config: {e}")
        return(None)



def load_path (path):   
    if not os.path.exists(path):
        logging.error("Le chemin spécifié n'existe pas.")
        return None
    extension = os.path.splitext(path)[1].lower() #permet d'extraire l'extension du fichier et mettre en minuscule. 
    try:
        if extension ==".csv":
            return pd.read_csv(path, sep=",")
        elif extension in [".xls", ".xlsx"]:
            return pd.read_excel(path)
        else:
            logging.error("Format de fichier incompatible")
            return(None)
    except Exception as e:
        logging.error(f"Une erreur s'est produite lors du chargement du fichier : {e}")
        return None

def load_API(url, API_KEY):
    headers = {"Authorization":f'Bearer {API_KEY}'}

    response = requests.get(url, headers=headers, timeout=10) #On lance la requête avec GET
    if response.status_code == 200:
        data = response.json() #On convertit la réponse en format JSON
        return pd.DataFrame(data) #Pandas parse le JSON et le convertit en dataframe
    else:
        logging.error(f"Erreur lors de la requête API, Code d'erreur : {response.status_code}") 
        return None

def load_database(source, api_key=None, encoding=None): #Je rassemble mes fonctions load_path et load_API dans une seule fonction afin de pouvoir automatiser plus simplement mon pipeline
    if source.startswith(("http://", "https://")):
        if api_key is None:
            logging.error("Clé API requise")
            return(None)
        headers = {"Authorization":f'Bearer {api_key}'}
        try:
            response = requests.get(source, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json() 
                return pd.DataFrame(data)
            else:
                logging.error(f"Erreur lors de la requête API, Code d'erreur : {response.status_code}") 
                return None
        except Exception as e:
            logging.error(f"Une erreur s'est produite lors de la requête API: {e}")
            return None
    else:
        if not os.path.exists(source):
            logging.error(f"Le fichier '{source}' n'existe pas.")
            return None
        extension = os.path.splitext(source)[1].lower() #permet d'extraire l'extension du fichier et mettre en minuscule. 
        try:
            if extension ==".csv":
                return pd.read_csv(source, sep=",", encoding=encoding)
            elif extension in [".xls", ".xlsx"]:
                return pd.read_excel(source)
            else:
                logging.error("Format de fichier incompatible")
                return(None)
        except Exception as e:
            logging.error(f"Une erreur s'est produite lors du chargement du fichier : {e}")
            return None

# ==========================================
#3. FONCTIONS DE TRANSFORMATION
#==========================================
# Fonction Info
# Fonction check_missing
# Fonction drop_NAN 
#F Fonction replace_values
# Fonction replace_mapping
# Fonction transform_value 
# Fonction check_missing_after_mapping
# Fonction column_rename
#Fonction Filter rows
# Fonction convert_dtypes
# Fonction col_assign qui permet de créer une nouvelle colonne à partir du résultat de colonne1/colonne2


def df_info(df):
    print("Aperçu des données :")
    print(df.head())
    print("\n Informations sur les données :")
    print(df.info())
    return(df)


def check_missing(df):
    print("Analyse des valeurs manquantes :")
    missing = df.isnull().sum()
    summary = missing.reset_index()
    summary.columns = ["Colonne", "Nb manquants"]
    manquant= summary[summary['Nb manquants'] > 0]
    missing_percent = 100 * df.isnull().sum() / len(df)
    if manquant.empty:
        print("Aucune valeur manquante détectée.")
    else:
        print("Valeurs manquantes :")
        print(manquant)
        print(missing_percent)
    
    
    return(df)
   

def df_drop_NAN(df, active=False, subset=None): #Fonction qui permet de supprimer les lignes contenants des valeurs manquantes, subset=["colonne", "colonne2", etc], si subset est None (juste appeler df_drop_NAN(df) alors toutes les colonnes seront prises) 
    if active:
        df_cleaned = df.dropna(subset=subset)
        logging.info(f"Supression des lignes. Lignes restantes : {len(df_cleaned)}")
        return df_cleaned
    else:
        logging.info(" df_drop_ NAN : Aucune suppression effectuée.")
        return(df)

def str_replace_values(df, column, old_val,  new_val):  #Fonction qui permet de remplacer une valeur string dans une colonne spécifique
    df[column]= df[column].str.replace(pat = old_val, repl= new_val, regex=False)
    return df

def replace_mapping(df, column, mapping):  #Fonction qui permet de remplacer les valeurs d'une colonne à l'aide d'un dictionnaire de mapping, mapping = {"old_value1:"new value1, ... "old_valueN:"new_valueN"}
    df=df
    df[column] = df[column].map(mapping)
    return df


#Fonction combo de mapping et replace values en fonction des besoins dans un soucis de faire un pipeline automatisé
#keep_others=True pour utiliser .replace 
#keep_others=False pour utiliser .map
def transform_value(df, column, mapping, keep_others=True):
    df=df
    if keep_others:
        print(f"Remplacement partiel dans {column} avec .replace")
        df[column] = df[column].replace(mapping)
    else:
        print(f"Remplacement total dans {column} avec .map")
        df[column] = df[column].map(mapping)
    return df

def check_missing_after_mapping(df, column): #fonction qui vérifie s'il y'a des valeurs manquantes après le mapping, à utiliser juste après (replace_mapping)
    missing = df[column].isna().sum()
    if missing > 0:
        logging.warning(f" {missing} valeurs manquantes dans {column}")
    return(df)

#Fonction pour renommer les colonnes à l'aide d'un dictionnaire mapping 
def column_rename(df, mapping, keep_others=True): 
    df=df
    return df.rename(columns=mapping)

#Avec conditions un dictionnaire du type {column1:value1, etc}
def filter_rows(df, conditions):        
    df = df
    for column, value in conditions.items():
        df = df[df[column] == value]
    return df

#dtype map un dictionnaire tel que {"column1":dtype1, "column2"::dtype2 etc} avec dtype, int, float, str & datetime
def convert_dtypes(df, dtype_map): 
    df= df
    for column, dtype in dtype_map.items():
        try:
            if dtype == "datetime":
                df[column] = pd.to_datetime(df[column], errors='coerce')
            else:
                df[column]= df[column].astype(dtype)
            logging.info(f'Conversion de {column} en {dtype} réussie"')
        except Exception as e:
            logging.error(f"Erreur lors de la conversion de {column} en {dtype} : {e}")
    return df


#Fonction qui permet de créer une nouvelle colonne à partir du résultat d'une opération entre 2 autres columns avec replace(0,1) pour éviter les divisions par zéro
def col_assign(df, new_col_name, arg1, arg2):
    df = df
    df[new_col_name] = df[arg1] / df[arg2].replace(0, 1)  
    logging.info(f"Nouvelle colonne calculée : {new_col_name} ({arg1} / {arg2})")
    return df


#Fonction qui permet d'agréger les données en calculant la moyenne des values de "columns" après avoir filtré les donnés selons "conditions" de type conditions ={column1=value1, etc}
def aggregate_mean (df, columns, conditions):
    df= df
    for column, value in conditions.items():
        df = df[df[column] == value]
    return df.groupby(columns).mean().reset_index()

#Fonction qui permet d'aréger les donnés à l'aide d'un filtre "conditions" tel que conditions ={column1=value1, etc}} 
#grpby_columns : liste des colonnes sur lesquelles on veut grouper sous la forme ["colonne1, "colonne2," etc]
#agg_logic : dictionnaire tel que {"colonne1":"mean", "ventes":"sum", etc...}

def aggregate (df, grpby_columns, agg_logic, conditions=None):  
    df= df
    if conditions is not None:
        for column, value in conditions.items():
            df = df[df[column] == value]
    return df.groupby(grpby_columns).agg(agg_logic).reset_index()

def saving_file(df, path, format): #Fonction qui permet de save un data frame au format "csv" ou "excel"
    if format =="csv":
        df.to_csv(f"{path}.csv", index=False)
    elif format in ["xls", "xlsx"]:
        df.to_excel(f"{path}.xlsx", index=False)
    else:
        logging.info("Format de fichier incompatible, veuillez choisir entre 'csv' ou 'excel'")
    return df



#Extrait Année, Mois, Jour et heure de la colonne date choisie par l'utilisateur sur un format YYYY/MM/DD où DD/MM/YYYY etc...
def extract_date(df, date_column=None):
    if date_column is not None:
        temp_date = pd.to_datetime(df[date_column], errors="coerce")
        df["Année"] = temp_date.dt.year
        df["Mois"] = temp_date.dt.month
        df["Jour"] = temp_date.dt.day
        df["Heure"] = temp_date.dt.hour
        print("Colonnes après extraction :", df.columns.tolist())
        if df["Heure"].nunique()==1 and df["Heure"].iloc[0]==0:
            logging.info(f"Pas de mention d'heure dans {date_column}")
    return df


#Permet de mettre au même format les mois et période qu'importe le csv
def prepare_dates(df):
    if "Mois" in df.columns:
        df["Mois_num"] = df["Mois"].astype(int)
    else :
        raise KeyError("La colonne 'Mois' est introuvable.")
    mois_labels = {
        1: "Janvier", 2: "Février", 3: "Mars",
        4: "Avril", 5: "Mai", 6: "Juin",
         7: "Juillet", 8: "Août", 9: "Septembre",
        10: "Octobre", 11: "Novembre", 12: "Décembre"
    }

    df["Mois"] = df["Mois_num"].map(mois_labels)

    if "Année" in df.columns:
        df["Période"] = df["Année"].astype(str) + df["Mois_num"].astype(str).str.zfill(2)
    logging.info("Colonnes Mois_num et Période créées.")
    return df
# ==========================================
# 4. Execution du pipeline
# ==========================================

mois_labels = {
    1: "Janvier", 2: "Février", 3: "Mars",
    4: "Avril", 5: "Mai", 6: "Juin",
    7: "Juillet", 8: "Août", 9: "Septembre",
    10: "Octobre", 11: "Novembre", 12: "Décembre"
}


def run_pipeline(df, config):
    agg_logic = config.get("agg_logic", {})
    col_assign_conf = config.get("col_assign", {})
    conf_encoding= config.get("encoding", "utf-8")
    date_column = config.get("date_column", None)
    data_types = config.get("data_types", {})
    grpby = config.get("group_by", [])
    mapping_rename_col = config.get("mapping_rename", {})
    date_column = config.get("date_column", None)


    df_resultat= (df.pipe(extract_date, date_column=date_column)
    .pipe(df_info).pipe(check_missing)
    .pipe(convert_dtypes, dtype_map=data_types)
    .pipe(prepare_dates)
    .pipe(column_rename, mapping=mapping_rename_col, keep_others=True)
    .pipe(df_drop_NAN)
    .pipe(aggregate, grpby_columns=grpby, agg_logic=agg_logic)
    .pipe(col_assign, **col_assign_conf)
    .pipe(df_info))

    return df_resultat

def run_full_test():
    #config = load_config("config.json")
    if not config:
        logging.info("Aucune configuration trouvée, arrêt du pipeline.")
        return
    try :
        logging.info("Démarrage du pipeline ETL")   
        df = load_database(config["file_path"], 
                       encoding=config.get("encoding", "utf-8")).copy()
        return run_pipeline(df, config)
    except Exception as e:
        logging.critical(f" Erreur critique durant l'exécution : {e}")
if __name__=="__main__":
    run_full_test()
