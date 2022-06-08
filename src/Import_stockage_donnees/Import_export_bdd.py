# -*- coding: utf-8 -*-
'''
Created on 7 juin 2022

@author: martin.schoreisz
Module d'import et d'export des donnees stockees en Bdd
'''
import os
import pandas as pd
import Connexion_Transfert as ct
from datetime import datetime
from Import_stockage_donnees.Params import (conversionNumerique, colonnesFichierMesureBruit, converters, dicoMatosBruit_mesure)


def importIndexNiveauBruit(bdd='ech24'):
    """
    recuperation du prochain index dans la table des niveaux de bruit
    in : 
        bdd : string descriptif de la connexion bdd
    out : 
        indexMaxBruit : integer : prochaine valeur de l'index Bruit
    """
    with ct.ConnexionBdd(bdd) as c:
        indexMaxBruit = pd.read_sql("SELECT CASE WHEN max(id) IS NULL THEN 0 ELSE max(id) END +1 AS max_id FROM mesures_physiques.niveau_bruit",
                                    c.sqlAlchemyConn).iloc[0, 0]
    return indexMaxBruit


def transfertFichierMesure2Bdd(bdd, listAPasser, dossierSrc):
    """
    fonction de transfert des csv de mesure vers la Bdd
    in : 
        bdd : string de connexion à la bdd
        listAPasser : itérables de nom de fichier (sans le chemin) à ignorer
        dossierSrc : path du dossier contenant les fichiers csv
    """
    with ct.ConnexionBdd(bdd) as c:
        for r, d, files in os.walk(dossierSrc):
            for f in files:
                if f in listAPasser:
                    continue
                if f .endswith('.csv'):
                    cheminFichier = os.path.join(r, f)
                    timerAvantCalcul = datetime.now()
                    print(f"fichier {cheminFichier}, avant calcul {datetime.now()}")
                    fichier = FichierMesureBruitCsv(cheminFichier)
                    timerApresCalcul = datetime.now()
                    print(f"duree calcul : {timerApresCalcul-timerAvantCalcul}")
                    fichier.dfNiveauSpectre.to_sql(schema='mesures_physiques', name='niveau_bruit', con=c.sqlAlchemyConn, if_exists='append', index=False)
                    timerApresNiveaux = datetime.now()
                    print(f"duree transfert spectre {timerApresNiveaux-timerApresCalcul}")
    return

class FichierMesureBruitCsv(object):
    """
    Class permettant la manipulation des fichiers csv issus des mesures de bruit.
    Attributs : 
        nomFichier : string : raw string cu chemin complet du fichier
        skiprows : integer : nb de ligne a ne pas prendre en compte. Default = 49
    """
    def __init__(self, nomFichier, skiprows=49):
        self.nomFichier = nomFichier
        self.skiprows = skiprows
        self.sonometre = self.affecterIdSonometre()
        self.formaliserNiveauSpectre(self.lectureFichier(), importIndexNiveauBruit())
       
        
    def conversionDate(self, dateTexte):
        """
        convertir le format de date des fichiers acoustiques de mesure
        """
        return datetime.strptime(dateTexte, "%H:%M:%S:%f %d/%m/%Y")
    
    
    def lectureFichier(self):
        """
        lire le fichier et renvoyer une dataframe pandas
        """
        return pd.read_csv(self.nomFichier, skiprows=self.skiprows, names=colonnesFichierMesureBruit, sep=';',
                           parse_dates=[0], dayfirst=True, date_parser=self.conversionDate, converters=converters)
        
        
    def affecterIdSonometre(self):
        """
        fournir l'identifiant du sonometre a partir du dico ad hoc et du nom de fichier.
        si pas trouve, alors erreur
        """
        for k, v in dicoMatosBruit_mesure.items():
            if k in os.path.basename(self.nomFichier):
                return v
        else:
            raise KeyError(f"le fichier n'est pas rattaché à un sonomètre")
        
        
    def formaliserNiveauSpectre(self, dfBruteFichier, indexStart=0):
        """
        Convertir la df brutes en df des niveaux et du spectre, selon le formalisme Bdd et avec les index qui vont bien
        in : 
            dfBruteFichier : dataframe issu de lectureFichier
            indexStart : integer : modification de l'index pour démarer à un identifiant spécifié
        out : 
            self.dfNiveauBruit : dataframe des niveaux de bruit format bdd
            self.dfSpectre : dataframe du spectre relatif aux niveaux de bruit format bdd
        """
        dfBruteFichier.index += indexStart
        self.dfNiveauSpectre = dfBruteFichier.reset_index().rename(columns={'index': 'id'})
        self.dfNiveauSpectre['id_materiel'] = self.sonometre
        
