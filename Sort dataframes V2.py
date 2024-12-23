# -*- coding: utf-8 -*-
"""
Created on Thu Dec  5 21:57:41 2024

@author: r.auappavou
"""

import pandas as pd

# Charger le fichier CSV
#file_path = 'D:/Documents de r.auappavou/Python Scripts/Quickcheck/quickcheck_measurements.csv'
file_path = 'D:/Documents de r.auappavou/Python Scripts/Quickcheck/quickcheck_measurements_hebdo.csv'

data = pd.read_csv(file_path)

# Renommer les colonnes pour correspondre aux attentes
data = data.rename(columns={
    'TASK_TUnit': 'Treatment Unit',
    'TASK_En': 'Energy',
    'MD_DateTime': 'Date',
    'WORK_Name': 'Work Name'  # S'assurer que 'WORK_Name' est renommé en 'Work Name'
})

# Convertir 'Energy' en entier ou décimal
data['Energy'] = pd.to_numeric(data['Energy'], errors='coerce').fillna(0).astype(int)

# Convertir 'Date' en format datetime, extraire la date et l'heure
data['Date'] = pd.to_datetime(data['Date'], errors='coerce')   # Conversion en datetime
data['Time'] = data['Date'].dt.time                           # Extraire l'heure
data['Date_Str'] = data['Date'].dt.strftime('%d/%m/%Y')       # Reformater la date en chaîne

# Obtenir la date du jour (au format `Date_Str`)
today = pd.Timestamp.now().strftime('%d/%m/%Y')

# Filtrer les données pour les unités de traitement "iX" et "Halcyon"
units_of_interest = ['iX', 'Halcyon']
filtered_data = data[data['Treatment Unit'].isin(units_of_interest)]

# Filtrer uniquement les données du jour
filtered_today = filtered_data[filtered_data['Date_Str'] == today]

# Trier les données du jour par unité de traitement et énergie
sorted_today = filtered_today.sort_values(by=['Treatment Unit', 'Energy', 'Date'])

# Séparer les DataFrames pour chaque unité de traitement
unit_dataframes_today = {unit: sorted_today[sorted_today['Treatment Unit'] == unit] for unit in units_of_interest}

# Colonnes communes à extraire pour toutes les unités
common_columns = ['Date', 'AV_SYMLR_Value', 'AV_SYMGT_Value', 'AV_CAX_Value', 'AV_FLAT_Value']

# Extraction et affichage des valeurs spécifiques pour chaque unité
for unit in units_of_interest:
    if unit in unit_dataframes_today:
        unit_data = unit_dataframes_today[unit]
        
        # Vérifier s'il y a des données pour l'unité
        if not unit_data.empty:
            # Extraire les colonnes communes
            selected_data = unit_data[common_columns]
            print(f"--- Données spécifiques pour {unit} ---")
            print(selected_data)
        else:
            print(f"Aucune donnée trouvée pour l'unité {unit} pour la date d'aujourd'hui.")
    else:
        print(f"Aucune donnée trouvée pour l'unité {unit} pour la date d'aujourd'hui.")

# Traitement spécial lorsque 'Work Name' est 'IX lundi hebdo'
work_name_filter = 'IX lundi hebdo'
if 'Work Name' not in data.columns:
    raise ValueError("La colonne 'Work Name' est manquante dans le fichier.")

# Filtrer les données avec 'Work Name' égal à 'IX lundi hebdo' pour la date du jour
hebdo_filtered_data = filtered_data[
    (filtered_data['Work Name'] == work_name_filter) & (filtered_data['Date_Str'] == today)
]

# Colonnes supplémentaires à extraire pour le cas particulier
hebdo_columns = [
    'AV_SYMLR_Value', 'AV_SYMGT_Value', 'AV_CAX_Value', 'AV_FLAT_Value',
    'Energy', 'AV_We_Value', 'TASK_Info', 'Date'
]

# Vérifier que toutes les colonnes demandées existent
missing_columns = [col for col in hebdo_columns if col not in data.columns]
if missing_columns:
    raise ValueError(f"Les colonnes suivantes sont manquantes : {missing_columns}")

# Extraire les colonnes pour le cas particulier
if not hebdo_filtered_data.empty:
    hebdo_selected_data = hebdo_filtered_data[hebdo_columns]
    print(f"\n--- Données spécifiques pour 'Work Name' = '{work_name_filter}' ---")
    print(hebdo_selected_data)
else:
    print(f"Aucune donnée trouvée pour 'Work Name' = '{work_name_filter}' pour la date d'aujourd'hui.")
