# -*- coding: utf-8 -*-
'''
Created on 7 juin 2022

@author: martin.schoreisz
Module d'import et d'export des donnees stockees en Bdd
'''
import os, re
import pandas as pd
import Connexion_Transfert as ct
from datetime import datetime
from Outils import checkParamValues
from Import_stockage_donnees.Params import (conversionNumerique, colonnesFichierMesureBruit, converters, dicoMatosBruit_mesure,
                                            mappingColonnesFixesRessenti, colonnesAjouteesRessenti, dicoAdresseAEpurerRessenti)


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


def transfertFichierMesureBruit2Bdd(dossierSrc, listAPasser=None , bdd='ech24'):
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
                if listAPasser and f in listAPasser:
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
        
class FichierCsvEnquete(object):
    """
    classe permettant la mise en forme du fihcier csv issu de Lime Survey avant trabsfert bdd
    """
    def __init__(self, fichier):
        self.fichier = fichier
        self.ouverture()
        self.dfExploitable, self.dfNonExploitable = self.creationDfExploitable()
        self.listerAdresseParticipants()
        self.declarationGeneFinale()
        
        
    def renommerColonne(self, df):
        """
        renommer toutes les colonnes necessaires
        in : 
            df : datafaframe a renommer
        """
        for k, v in mappingColonnesFixesRessenti.items():
            df.rename(columns={df.columns[k]: v}, inplace=True)
            
            
    def ouverture(self):
        self.dFBrute = pd.read_csv(self.fichier)
        self.renommerColonne(self.dFBrute)
        
        
    def simplifierColonneChoixMultipe(self, df, x,  typeCol):
        """
        ramener les colonnes à choix multiples dans une eule avec une liste de valeur
        in : 
            dfExploitable : df issu de la lecture des données de résultat
            x : serie dans le cadre d'un apply
            typeCol : string parmi qualif_bruit, localisation_gene, vehicule_source
        """
        checkParamValues(typeCol, colonnesAjouteesRessenti)
        for k, v in {'qualif_bruit': [i for i in range(67,86)], 'localisation_gene': [i for i in range(63,67)], 'vehicule_source': [i for i in range(59,63)],
                     'route_source': [48, 50, 52, 54, 56], 'source_bruit': [37, 39, 41, 43, 45], 'perturbation': [i for i in range(19,29)]}.items():
            if typeCol == k:
                resultat = [re.split(' \[', c.replace('[obligatoire]',''))[1][:-1] for c in df.iloc[:, v].columns
                            if not pd.isnull(x[c]) and x[c] != 'Non']
                break
        for k, v in {'route_source_comment': [49, 51, 53, 55, 57], 'source_bruit_comment': [38, 40, 42, 44, 46]}.items():
            if typeCol == k:
                resultat = [x[c] for c in df.iloc[:, v].columns if not pd.isnull(x[c]) and x[c] != 'Non']
        resultat = resultat if resultat else None
        return resultat
    
        
    def creationDfExploitable(self):
        """
        regrouper les valeurs prises par les listes de choix multiple dans une seule colonne relative à la question.
        completion des adresses mail manquantes issue des formulaires papier
        """
        # separer les lignes exploitables du reste
        dfNonExploitable =  self.dFBrute.loc[ self.dFBrute.iloc[:, 5: 87].isna().all(axis=1)].copy()
        dfExploitable =  self.dFBrute.loc[~ self.dFBrute.iloc[:, 5: 87].isna().all(axis=1)].copy()
        
        # regrouper les valeurs d'attributs
        dfExploitable['qualif_bruit'] = dfExploitable.apply(lambda x: self.simplifierColonneChoixMultipe(dfExploitable, x, 'qualif_bruit'), axis=1)
        dfExploitable['localisation_gene'] = dfExploitable.apply(lambda x: self.simplifierColonneChoixMultipe(dfExploitable, x, 'localisation_gene'), axis=1)
        dfExploitable['vehicule_source'] = dfExploitable.apply(lambda x: self.simplifierColonneChoixMultipe(dfExploitable, x, 'vehicule_source'), axis=1)
        dfExploitable['route_source'] = dfExploitable.apply(lambda x: self.simplifierColonneChoixMultipe(dfExploitable, x, 'route_source'), axis=1)
        dfExploitable['route_source_comment'] = dfExploitable.apply(lambda x: self.simplifierColonneChoixMultipe(dfExploitable, x, 'route_source_comment'), axis=1)
        dfExploitable['source_bruit'] = dfExploitable.apply(lambda x: self.simplifierColonneChoixMultipe(dfExploitable, x, 'source_bruit'), axis=1)
        dfExploitable['source_bruit_comment'] = dfExploitable.apply(lambda x: self.simplifierColonneChoixMultipe(dfExploitable, x, 'source_bruit_comment'), axis=1)
        dfExploitable['perturbation'] = dfExploitable.apply(lambda x: self.simplifierColonneChoixMultipe(dfExploitable, x, 'perturbation'), axis=1)
        
        # completer les adresses mails
        dfExploitable.loc[dfExploitable.mail.isna(), 'mail'] = dfExploitable.loc[dfExploitable.mail.isna()].nom+dfExploitable.loc[
            dfExploitable.mail.isna()].prenom+'@factice.com'
        return dfExploitable, dfNonExploitable
    
    
    def listerAdresseParticipants(self):
        # tables des adresses et des participants
        self.dfParticipantsAdresses = self.dfExploitable.loc[~self.dfExploitable.iloc[:, 6].isna()].iloc[:, [6, 87, 88, 89]].reset_index(drop=True).drop_duplicates(
            ['adresse', 'nom', 'prenom', 'mail']).reset_index().copy()
            
        # trouver / nettoyer les participants en double avec des adresses différentes
        self.dfParticipantsAdresses.loc[self.dfParticipantsAdresses.duplicated('mail', keep=False)].sort_values('mail')  # analyse visuelle pour déterminer les lignes à enlever et les mettre dans le parametre dicoAdresseAEpurer
        self.dfParticipantsAdresses.drop(self.dfParticipantsAdresses.loc[(self.dfParticipantsAdresses.adresse.isin(dicoAdresseAEpurerRessenti['adresse'])) &
                                                                         (self.dfParticipantsAdresses.nom.isin(dicoAdresseAEpurerRessenti['nom'])) &
                                                                         (self.dfParticipantsAdresses.prenom.isin(dicoAdresseAEpurerRessenti['prenom']))
                                                                         ].index, inplace=True)
        
        
    def listerParticipantsSansAdressePostale(self):
        """
        out : 
            participantsSansAdressePostale : liste d'adresse mail
        """
        return list(self.dfExploitable.loc[~self.dfExploitable.mail.isin(self.dfParticipantsAdresses.mail.tolist())].mail.unique())
    
    
    def declarationGeneFinale(self):
        """
        a partir de la dfExploitable, valider les adresses pour chaque déclaration
        """
        self.dfDeclarationGene = self.dfExploitable.merge(self.dfParticipantsAdresses, on='mail', how='left').rename(
            columns={'adresse_y': 'adresse', 'nom_x': 'nom', 'prenom_x': 'prenom'})[list(mappingColonnesFixesRessenti.values())+colonnesAjouteesRessenti]
        
    
    
    