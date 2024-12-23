# -*- coding: utf-8 -*-
"""
Created on Thu Dec  5 21:23:51 2024

@author: r.auappavou
"""

# -*- coding: utf-8 -*-
"""
Script unifié QuickCheck
Permet la connexion à la QuickCheck, la récupération des mesures, et l'export des données vers un fichier CSV.
"""

import codecs
import datetime
import logging
import pandas as pd
import re
import socket
import sys
from tqdm import tqdm

class QuickCheck:
    """Gestion de l'interaction avec la PTW QuickCheck pour la QA quotidienne."""

    def __init__(self, adresse_ip):
        self.adresse_ip = adresse_ip
        self.port = 8123
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
        self.sock.settimeout(3)
        self.MSG = b""
        self.message_brut = ""
        self.mesures = pd.DataFrame()
        self.donnees = ""
        self.donnees_brutes = b""
        self.est_connecte = False

    def connexion(self):
        """Se connecte au dispositif et vérifie la connexion."""
        print(f"IP cible UDP : {self.adresse_ip}")
        print(f"Port cible UDP : {self.port}")
        self.envoyer_message("SER")
        if "SER" in self.donnees:
            self.est_connecte = True
            print("Connexion réussie à la QuickCheck.")
            print(self.donnees)

    def fermer(self):
        """Ferme la connexion au dispositif."""
        self.sock.close()
        del self.sock
        self.est_connecte = False

    def preparer_message(self):
        """Prépare un message à envoyer (ajout des caractères \r\n)."""
        self.MSG = (
            self.message_brut.encode()
            + codecs.decode("0d", "hex")
            + codecs.decode("0a", "hex")
        )

    def envoyer_socket(self):
        """Envoie le message via socket et récupère les données."""
        self.donnees = ""
        self.sock.sendto(self.MSG, (self.adresse_ip, self.port))
        self.donnees_brutes, _ = self.sock.recvfrom(4096)

    def envoyer_message(self, message):
        """Envoie une instruction à la QuickCheck.

        Args:
            message (str): Instruction à envoyer (e.g., SER, MEASCNT, MEASGET;INDEX-MEAS=xx).
        """
        self.message_brut = message
        self.preparer_message()
        max_retentatives = 3
        n_retentative = 0

        while True:
            try:
                self.envoyer_socket()
                donnees_decodees = self.donnees_brutes.decode(encoding="utf-8")
                self.donnees = donnees_decodees.strip("\r\n")
                break
            except socket.timeout:
                if n_retentative == max_retentatives:
                    print("Erreur de connexion - Retentatives maximales atteintes.")
                    print("Dispositif QuickCheck inaccessible, veuillez vérifier les paramètres.")
                    self.donnees = ""
                    break

                print("Délai d'attente expiré. Nouvelle tentative...")
                n_retentative += 1
                print(f"Tentative {n_retentative}/{max_retentatives}.")

    def analyser_mesures(self):
        """Analyse et structure les données reçues."""
        donnees_split = self.donnees.split(";")
        mesures = {}

        if donnees_split[0] == "MEASGET":
            # Analyse des sections MD, MV, AV, WORK, TASK
            # (Simplifié ici pour la démonstration)
            mesures["Donnees"] = self.donnees
        elif donnees_split[0] == "MEASCNT":
            mesures[donnees_split[0]] = int(donnees_split[1])
        elif donnees_split[0] in ("PTW", "SER", "KEY"):
            mesures[donnees_split[0]] = donnees_split[1:]
        return mesures

    def recuperer_mesures(self):
        """Récupère toutes les mesures depuis le dispositif."""
        if not self.est_connecte:
            raise ValueError("La QuickCheck n'est pas connectée.")
        self.envoyer_message("MEASCNT")
        m = self.analyser_mesures()
        if "MEASCNT" in m:
            nombre_mesures = m["MEASCNT"]
            print("Récupération des mesures QuickCheck...")
            liste_mesures = []
            for i in tqdm(range(nombre_mesures)):
                controle = False
                while not controle:
                    self.envoyer_message(f"MEASGET;INDEX-MEAS={i}")
                    controle = self.message_brut in self.donnees

                mesure = self.analyser_mesures()
                liste_mesures.append(mesure)
            self.mesures = pd.DataFrame(liste_mesures)


def exporter_vers_csv(adresse_ip, chemin_csv):
    """Exporte les données QuickCheck vers un fichier CSV.

    Args:
        adresse_ip (str): Adresse IP du dispositif QuickCheck.
        chemin_csv (str): Chemin du fichier CSV de destination.
    """
    niveau_log = logging.INFO
    logger = logging.getLogger(__name__)
    logger.setLevel(niveau_log)

    sortie_console = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    sortie_console.setFormatter(formatter)
    sortie_console.setLevel(niveau_log)
    logger.addHandler(sortie_console)

    qc = QuickCheck(adresse_ip)
    qc.connexion()
    if qc.est_connecte:
        qc.recuperer_mesures()
        print(f"Sauvegarde des données dans {chemin_csv}")
        qc.fermer()
        qc.mesures.to_csv(chemin_csv, index=False)


if __name__ == "__main__":
    # Configuration pour un test rapide
    adresse_ip = "10.188.65.253"  # Remplacez par l'adresse IP de votre dispositif QuickCheck
    chemin_csv = "mesures_quickcheck.csv"
    exporter_vers_csv(adresse_ip, chemin_csv)
