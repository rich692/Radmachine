

# -*- coding: utf-8 -*-
"""
Created on Wed Jan  8 14:23:05 2025

@author: r.auappavou
"""


import codecs
import datetime
import re
import socket
import logging
import pandas as pd
import tqdm


class QuickCheck:
    """A class to interact with PTW QuickCheck linac daily QA device.
    Common usage will look like:
        from pymedphys.experimental import QuickCheck
        qc = QuickCheck('QUICKCHECK-XXX')      QUICKCHECK-XXX (your QUICKCHECK device hostname or IP)
        qc.connect()
        qc.get_measurements()

            Receiving Quickcheck measurements
            100%|██████████| 108/108 [00:01<00:00, 58.57it/s]

        qc.measurements.to_csv(csv_path)       csv_path to save data as csv file

    ...

    Attributes
    ----------
    ip : str
        IP address or hostname of the device
    MSG : str
        Instruction to be sent to QuickCheck device e.g. MEASCNT, KEY, SER...
    measurements: pandas DataFrame
        DataFrame that contains all measurements retrieved from QuickCheck
    data: str
        Processed received data string

    """

    def __init__(self, ip):
        self.ip = ip
        self.port = 8123
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
        self.sock.settimeout(3)
        self.MSG = b""
        self.raw_MSG = ""
        self.measurements = pd.DataFrame()
        self.data = ""
        self.raw_data = b""
        self.connected = False

    def connect(self):
        """Opens connection to the device and prints its serial number if successful"""
        print("UDP target IP:", self.ip)
        print("UDP target port:", self.port)
        self.send_quickcheck("SER")
        if "SER" in self.data:
            self.connected = True
            print("Connected to Quickcheck")
            print(self.data)

    def close(self):
        """Closes connection to the device"""
        self.sock.close()
        del self.sock
        self.connected = False

    def _prepare_qcheck(self):
        """Appends characters \r\n to MSG"""
        self.MSG = (
            self.raw_MSG.encode()
            + codecs.decode("0d", "hex")
            + codecs.decode("0a", "hex")
        )

    def _socket_send(self):
        """Encapsulates socket sending of MSG and data reception, easier to mock in tests"""
        self.data = ""
        self.sock.sendto(self.MSG, (self.ip, self.port))
        self.raw_data, _ = self.sock.recvfrom(4096)

    def send_quickcheck(self, message):
        """Sends instructions to device

        Args:
            message: str
                Instruction to send to QuickCheck device. eg: SER, KEY, MEASCNT, MEASGET;INDEX-MEAS=xx

        """
        self.raw_MSG = message
        self._prepare_qcheck()
        max_retries = 3
        n_retry = 0

        while True:
            try:
                self._socket_send()
                data = self.raw_data.decode(encoding="utf-8")
                self.data = data.strip("\r\n")
                break
            except socket.timeout:
                if n_retry == max_retries:
                    print(
                        """
                          Connection Error  - Reached max retries
                          Quickcheck device unreachable, please check your settings"""
                    )
                    self.data = ""
                    break

                print("Connection Timeout")
                n_retry += 1
                print("Retrying connection {}/{}".format(n_retry, max_retries))

    def _parse_measurements(self):
        """Parses received data based on sent MSG"""
        data_split = self.data.split(";")
        m = {}  # Dictionary with measurements
        if data_split[0] == "MEASGET":
            #  MD section:__________________________________________________________
            MD = re.findall(r"MD=\[(.*?)\]", self.data)[0]
            m["MD_ID"] = int(re.findall(r"ID=(.*?);", MD)[0])
            meas_date = re.findall(r"Date=(.*?);", MD)[0]
            m["MD_Date"] = datetime.datetime.strptime(meas_date, "%Y-%m-%d").date()
            meas_time = re.findall(r"Time=(.*?)$", MD)[0]
            m["MD_Time"] = datetime.datetime.strptime(meas_time, "%H:%M:%S").time()
            m["MD_DateTime"] = datetime.datetime.combine(m["MD_Date"], m["MD_Time"])
            #  MV section:__________________________________________________________
            str_val = re.findall(r"MV=\[(.*?)\]", self.data)[0]
            regex_map = {
                "MV_CAX": "CAX=(.*?);",
                "MV_G10": "G10=(.*?);",
                "MV_L10": "L10=(.*?);",
                "MV_T10": "T10=(.*?);",
                "MV_R10": "R10=(.*?);",
                "MV_G20": "G20=(.*?);",
                "MV_L20": "L20=(.*?);",
                "MV_T20": "T20=(.*?);",
                "MV_R20": "R20=(.*?);",
                "MV_E1": "E1=(.*?);",
                "MV_E2": "E2=(.*?);",
                "MV_E3": "E3=(.*?);",
                "MV_E4": "E4=(.*?);",
                "MV_Temp": "Temp=(.*?);",
                "MV_Press": "Press=(.*?);",
                "MV_CAXRate": "CAXRate=(.*?);",
                "MV_ExpTime": "ExpTime=(.*?)$",
            }
            for key, pattern in regex_map.items():
                m[key] = float(re.findall(pattern, str_val)[0])

            #  AV section:__________________________________________________________
            AV = re.findall(r"AV=\[(.*?)\]\]", self.data)[0]
            AV = AV + "]"  # add last character ]
            for s in ("CAX", "FLAT", "SYMGT", "SYMLR", "BQF", "We"):
                str_val = re.findall(s + r"=\[(.*?)\]", AV)[0]
                m["AV_" + s + "_Min"] = float(re.findall("Min=(.*?);", str_val)[0])
                m["AV_" + s + "_Max"] = float(re.findall("Max=(.*?);", str_val)[0])
                m["AV_" + s + "_Target"] = float(
                    re.findall("Target=(.*?);", str_val)[0]
                )
                m["AV_" + s + "_Norm"] = float(re.findall("Norm=(.*?);", str_val)[0])
                m["AV_" + s + "_Value"] = float(re.findall("Value=(.*?);", str_val)[0])
                m["AV_" + s + "_Valid"] = int(re.findall("Valid=(.*?)$", str_val)[0])

            #  WORK section:__________________________________________________________
            str_val = re.findall(r"WORK=\[(.*?)\]", self.data)[0]

            m["WORK_ID"] = int(re.findall("ID=(.*?);", str_val)[0])
            m["WORK_Name"] = re.findall("Name=(.*?)$", str_val)[0]

            #  TASK section:__________________________________________________________
            str_val = re.findall(r"TASK=\[(.*?)\];MV", self.data)[0]
            m["TASK_ID"] = int(re.findall(r"ID=(.*?);", str_val)[0])
            m["TASK_TUnit"] = re.findall(r"TUnit=(.*?);", str_val)[0]
            m["TASK_En"] = int(re.findall(r"En=(.*?);", str_val)[0])
            m["TASK_Mod"] = re.findall(r"Mod=(.*?);", str_val)[0]
            m["TASK_Fs"] = re.findall(r"Fs=(.*?);", str_val)[0]
            m["TASK_SSD"] = int(re.findall(r"SDD=(.*?);", str_val)[0])
            m["TASK_Ga"] = int(re.findall(r"Ga=(.*?);", str_val)[0])
            m["TASK_We"] = int(re.findall(r"We=(.*?);", str_val)[0])
            m["TASK_MU"] = int(re.findall(r"MU=(.*?);", str_val)[0])
            m["TASK_My"] = float(re.findall(r"My=(.*?);", str_val)[0])
            m["TASK_Info"] = re.findall(r"Info=(.*?)$", str_val)[0]

            str_val = re.findall(r"Prot=\[(.*?)\];", str_val)[0]
            m["TASK_Prot_Name"] = re.findall(r"Name=(.*?);", str_val)[0]
            m["TASK_Prot_Flat"] = int(re.findall(r"Flat=(.*?);", str_val)[0])
            m["TASK_Prot_Sym"] = int(re.findall(r"Sym=(.*?)$", str_val)[0])
        elif data_split[0] == "MEASCNT":
            m[data_split[0]] = int(data_split[1:][0])
        elif data_split[0] in ("PTW", "SER", "KEY"):
            m[data_split[0]] = data_split[1:]
        return m

    def get_measurements(self):
        """Retrieves all the measurements in the QuickCheck device and stores them in self.measurements"""
        if not self.connected:
            raise ValueError("Quickcheck device not connected")
        self.send_quickcheck("MEASCNT")
        if "MEASCNT" not in self.data:
            self.send_quickcheck("MEASCNT")
        m = self._parse_measurements()
        if "MEASCNT" in m:
            n_meas = m["MEASCNT"]
            print("Receiving Quickcheck measurements")
            meas_list = []
            for m in tqdm.tqdm(range(n_meas)):
                control = False
                while not control:
                    self.send_quickcheck("MEASGET;INDEX-MEAS=" + "%d" % (m,))
                    control = self.raw_MSG in self.data

                meas = self._parse_measurements()
                meas_list.append(meas)
            self.measurements = pd.DataFrame(meas_list)

################################################################################################################################
#########################################################################################################################################

# Configuration des logs
def configure_logger():
    """Configure le logger pour le script"""
    log_level = logging.INFO
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)
    ch = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

# Fonction principale de récupération et sauvegarde des mesures
def retrieve_and_save_measurements(device_ip, csv_output_path, logger):
    """Connecte à QuickCheck, récupère les mesures et les retourne sans les afficher"""
    retrieved_measurements = None
    qc = None  # Initialize qc to avoid UnboundLocalError
    try:
        qc = QuickCheck(device_ip)
        logger.info("Connexion à la QuickCheck...")
        qc.connect()

        if qc.connected:
            logger.info("Connexion réussie. Récupération des mesures...")
            qc.get_measurements()
            
            if not qc.measurements.empty:
                logger.info(f"Récupération réussie : {len(qc.measurements)} lignes.")
                
                # Stocker les mesures dans une variable
                retrieved_measurements = qc.measurements
                
                # Sauvegarde des mesures
                qc.measurements.to_csv(csv_output_path, index=False)
                logger.info(f"Mesures sauvegardées dans {csv_output_path}")
            else:
                logger.warning("Aucune mesure récupérée.")
        else:
            logger.error("Impossible de se connecter au dispositif QuickCheck.")
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des mesures : {e}")
    finally:
        if qc and hasattr(qc, 'connected') and qc.connected:
            qc.close()
            logger.info("Connexion fermée.")
    return retrieved_measurements

# Traitement des données CSV
def process_csv(file_path, output_folder, logger):
    """Charge, traite et exporte les données triées par unité"""
    try:
        data = pd.read_csv(file_path)
        logger.info("Fichier CSV chargé avec succès.")

        # Renommer les colonnes
        data = data.rename(columns={
            'TASK_TUnit': 'Treatment Unit',
            'TASK_En': 'Energy',
            'MD_DateTime': 'Date',
            'TASK_Fs': 'Field Size',
            'TASK_SSD': 'SSD',
            'MV_CAX': 'CAX',
            'MV_Temp': 'Temperature',
            'MV_Press': 'Pressure'
        })

        # Validation des colonnes nécessaires
        required_columns = ['Treatment Unit', 'Energy', 'Date', 'Field Size', 'CAX', 'Temperature', 'Pressure']
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            logger.error(f"Colonnes manquantes : {missing_columns}")
            raise ValueError("Le fichier CSV doit contenir les colonnes nécessaires pour l'analyse.")

        # Conversion des colonnes
        data['Energy'] = pd.to_numeric(data['Energy'], errors='coerce').fillna(0).astype(int)
        data['Date'] = pd.to_datetime(data['Date'], errors='coerce')  # Conversion en datetime
        data['Time'] = data['Date'].dt.time                           # Extraire l'heure
        data['Date'] = data['Date'].dt.strftime('%d/%m/%Y')           # Reformater la date

        # Filtrer et trier les données
        units_of_interest = ['iX', 'Halcyon']
        filtered_data = data[data['Treatment Unit'].isin(units_of_interest)]
        sorted_data = filtered_data.sort_values(by=['Treatment Unit', 'Energy', 'Date'])

        # Exporter les données triées par unité
        for unit in units_of_interest:
            unit_data = sorted_data[sorted_data['Treatment Unit'] == unit]
            output_path = f"{output_folder}/{unit}_sorted_output.csv"
            unit_data.to_csv(output_path, index=False)
            logger.info(f"Données triées pour {unit} exportées dans : {output_path}")
    except Exception as e:
        logger.error(f"Erreur lors du traitement du fichier CSV : {e}")

# Extraction et structuration des données depuis QuickCheck
def parse_quickcheck_data(raw_data, logger):
    """Structure les données QuickCheck en DataFrame"""
    try:
        measurements = []
        for data in raw_data:
            parsed_data = QuickCheck._parse_measurements(data)
            if parsed_data:
                measurements.append(parsed_data)
        
        df = pd.DataFrame(measurements)
        logger.info(f"Mesures structurées en DataFrame : {len(df)} lignes.")
        return df
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse des données QuickCheck : {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    logger = configure_logger()

    # Configuration
    device_ip = "10.188.65.253"
    csv_output_path = "D:/Documents de r.auappavou/Python Scripts/Quickcheck/quickcheck_measurements_2301.csv"
    output_folder = "D:/Documents de r.auappavou/Python Scripts/Quickcheck"

    # Étape 1 : Récupérer les mesures sans les afficher
    measurements = retrieve_and_save_measurements(device_ip, csv_output_path, logger)

#####################################################################################################################################



import pandas as pd
from datetime import datetime

# Charger le fichier CSV
#file_path = 'D:/Documents de r.auappavou/Python Scripts/Quickcheck/quickcheck_measurements 0801.csv'  # Remplacez par le chemin du fichier si nécessaire
data = measurements

# Renommer les colonnes pour correspondre aux attentes
# TASK_TUnit -> Treatment Unit, TASK_En -> Energy, MD_DateTime -> Date
data = data.rename(columns={
    'TASK_TUnit': 'Treatment Unit',
    'TASK_En': 'Energy',
    'MD_DateTime': 'Date'
})

# Vérifiez les colonnes disponibles après renommage
print("Colonnes disponibles après renommage :", data.columns)

# Vérifiez si les colonnes nécessaires existent
required_columns = ['Treatment Unit', 'Energy', 'Date']
missing_columns = [col for col in required_columns if col not in data.columns]
if missing_columns:
    print(f"Colonnes manquantes : {missing_columns}")
    raise ValueError("Le fichier CSV doit contenir les colonnes nécessaires pour l'analyse.")

# Convertir 'Energy' en entier ou décimal
data['Energy'] = pd.to_numeric(data['Energy'], errors='coerce').fillna(0).astype(int)

# Convertir 'Date' en format datetime, extraire la date et l'heure
data['Date'] = pd.to_datetime(data['Date'], errors='coerce')  # Conversion en datetime
data['Time'] = data['Date'].dt.time                          # Extraire l'heure
data['Date'] = data['Date'].dt.strftime('%d/%m/%Y')          # Reformater la date

# Filtrer les données pour les unités de traitement "iX" et "Halcyon"
units_of_interest = ['iX', 'Halcyon']
filtered_data = data[data['Treatment Unit'].isin(units_of_interest)]

# Trier les données par unité de traitement, énergie et date
sorted_data = filtered_data.sort_values(by=['Treatment Unit', 'Energy', 'Date'])

# Séparer les DataFrames pour chaque unité de traitement
unit_dataframes = {unit: sorted_data[sorted_data['Treatment Unit'] == unit] for unit in units_of_interest}

# Exporter les résultats pour chaque unité
for unit, df in unit_dataframes.items():
    output_path = f'D:/Documents de r.auappavou/Python Scripts/Quickcheck/{unit}_sorted_output.csv'
    df.to_csv(output_path, index=False)
    print(f"Les données triées pour l'unité {unit} ont été exportées dans : {output_path}")

# Exemple : afficher les premières lignes des données pour 'iX'
if 'iX' in unit_dataframes:
    print("Aperçu des données triées pour l'unité iX :")
    print(unit_dataframes['iX'].head())

# Exemple : afficher les premières lignes des données pour 'Halcyon'
if 'Halcyon' in unit_dataframes:
    print("Aperçu des données triées pour l'unité Halcyon :")
    print(unit_dataframes['Halcyon'].head())



# Vérifier si certaines valeurs n'ont pas pu être converties
if data['Date'].isnull().any():
    print("Certaines dates n'ont pas pu être converties et ont été ignorées.")
    data = data.dropna(subset=['Date'])  # Supprimer les lignes avec des dates non valides

# Extraire uniquement la partie date
data['Date Only'] = pd.to_datetime(data['Date'], errors='coerce',dayfirst=True)
#data['Date Only'] = data['Date'].dt.date

# Obtenir les dates disponibles dans le fichier
available_dates = data['Date Only'].unique()
print("Dates disponibles dans le fichier :", available_dates)

# Définir la date d'analyse (par défaut : aujourd'hui)
today_date = datetime.now().date()

# Vérifier si la date actuelle est disponible dans les données
if today_date not in available_dates:
    print(f"La date actuelle ({today_date}) n'est pas disponible dans le fichier. Utilisation d'une date spécifique.")
    # Définir une date spécifique (par exemple, la première date disponible)
    specific_date = available_dates[0] if len(available_dates) > 0 else None
    if specific_date:
        print(f"Analyse des données pour la date spécifique : {specific_date}")
        today_date = specific_date
    else:
        raise ValueError("Aucune date valide n'est disponible dans le fichier.")

# Filtrer les données pour la date choisie
today_data = data[data['Date Only'] == today_date]

# Filtrer les unités d'intérêt
units_of_interest = ['iX', 'Halcyon']
filtered_data = today_data[today_data['Treatment Unit'].isin(units_of_interest)]

# Créer un dictionnaire pour les DataFrames des machines pour la date choisie
today_dataframes = {
    unit: filtered_data[filtered_data['Treatment Unit'] == unit]
    for unit in units_of_interest
}

# Afficher les DataFrames pour chaque unité
for unit, df in today_dataframes.items():
    print(f"DataFrame pour l'unité {unit} pour la date choisie ({today_date}):")
    print(df if not df.empty else "Aucune donnée disponible", "\n")

df_Halcyon_today = today_dataframes['Halcyon']
df_iX_today = today_dataframes['iX']


# Liste des colonnes nécessaires
columns_to_keep = ['AV_CAX_Value', 'AV_FLAT_Value', 'AV_SYMLR_Value', 'AV_SYMGT_Value', 'TASK_We', 'MD_Time']

# Créer des DataFrames réduits
df_iX_today_reduced = df_iX_today[columns_to_keep].copy()
df_Halcyon_today_reduced = df_Halcyon_today[columns_to_keep].copy()

# Afficher un aperçu des DataFrames réduits
print("DataFrame réduit pour iX :")
print(df_iX_today_reduced.head(), "\n")

print("DataFrame réduit pour Halcyon :")
print(df_Halcyon_today_reduced.head(), "\n")