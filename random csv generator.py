import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

def generate_bakery_data(rows=50, mode="complete"):
    CATALOGUE = {
    "Baguette": 1.10,
    "Croissant": 1.25,
    "Pain au Chocolat": 1.40,
    "Éclair": 2.80,
    "Chausson aux Pommes": 2.20,
    "Tarte aux fraises":3.50,
    "Mille-feuille":3.00,
    "Macaron":0.90,
    "Brioche":1.50,
    "Fougasse":2.00,
    "Eclair au chocolat":2.80,
    "Baguette tradi":1.20
}
    data = []
    
    start_date = datetime(2023, 1, 1)

    for _ in range(rows):
        # Génération d'une date aléatoire sur l'année 2023
        random_days = random.randint(0, 364)
        date_obj = start_date + timedelta(days=random_days)
        
        prod = random.choice(list(CATALOGUE.keys()))
        qty = random.randint(1, 20)
        price=CATALOGUE[prod]
        
        row = {
            "Produit": prod,
            "Quantité": qty,
            "Prix_Unitaire": price,
            "Total_Vente": round(qty * price, 2)
        }
        
        if mode == "complete":
            row["Date_Transaction"] = date_obj.strftime("%Y-%m-%d")
        else:
            row["Annee_Vente"] = date_obj.year
            row["Mois_Vente"] = date_obj.month
            row["Jour_Vente"] = date_obj.day
            
        data.append(row)
    
    return pd.DataFrame(data)

# --- GÉNÉRATION DES FICHIERS ---
suffixe = random.randint(1000, 9999)
nom_fichier = f"test_boulangerie_{suffixe}.csv"
df_complet = generate_bakery_data(rows=100, mode="complete")
df_complet.to_csv(nom_fichier, index=False, encoding="utf-8-sig")

df_separé = generate_bakery_data(rows=100, mode="separated")
df_separé.to_csv(f"test_boulangerie_colonnes_separees_{suffixe}.csv", index=False, encoding="utf-8-sig")

print("Fichiers CSV générés avec succès !")