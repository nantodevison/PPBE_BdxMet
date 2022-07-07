# -*- coding: utf-8 -*-
'''
Created on 7 juin 2022

@author: martin.schoreisz
Module d'import et d'export des donnees stockees en Bdd
'''
import os, re
import pandas as pd
import numpy as np
import altair as alt
import Connexion_Transfert as ct
from datetime import datetime
from math import pi
from Outils import checkParamValues, regrouperLigneDfValeurNonNulle
from Import_stockage_donnees.Params import (bdd, conversionNumerique, colonnesFichierMesureBruit, converters, dicoMatosBruit_mesure,
                                            mappingColonnesFixesRessenti, colonnesAjouteesRessenti, dicoAdresseAEpurerRessenti, 
                                            dossierExportChartsRessenti, colonnesMeteo)


def importIndexNiveauBruit():
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


def transfertFichierMesureBruit2Bdd(dossierSrc, listAPasser=None, bdd='ech24'):
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
        dfNiveauSpectre : df des donnees
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
    classe permettant la mise en forme du fihcier csv issu de Lime Survey.
    possibilite de transfert vers Bdd.
    attributs : 
        fichier : raw_string du chemin complet du fihcier
        dFBrute : dataframe issu de la lecture
        dfExploitable : issu de dFBrute. dFBrute modifiee pour exploitation et comprehension facilitee
        dfNonExploitable : issu de dFBrute. Partie ne pouvant etre utilisee car entierement en NaN
        dfParticipants : df des participants : nom / prenom / adresse / mail / ...
        dfDeclarations : df des declarations valides, mise en forme, avec une date de debut, et de fin
        dfDeclarations_KO : df des declarations invalides, mise en forme, sans date de debut et de fin
    methodes : 
        listerParticipantsSansAdressePostale() : retourne la liste des particpants sans adresse
        exportVersBdd() : creer un dico des df permettant la création de charts
    """
    def __init__(self, fichier):
        self.fichier = fichier
        self.ouverture()
        dfExploitable, dfNonExploitable = self.creationDfExploitable()
        dfParticipantsAdresses, dfDeclarationGene = self.participantsGeneV0(dfExploitable)
        self.creerDfParticipants(dfParticipantsAdresses, dfDeclarationGene) 
        self.creerDfDeclarations(dfDeclarationGene)
        
        
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
                     'route_source': [48, 50, 52, 54, 56], 'source_bruit': [37, 39, 41, 43, 45]}.items():
            if typeCol == k:
                resultat = [re.split(' \[', re.sub('(?i)\[obligatoire\]', '', c))[1][:-1] for c in df.iloc[:, v].columns
                            if not pd.isnull(x[c]) and x[c] != 'Non']
                break
        for k, v in {'route_source_coment': [49, 51, 53, 55, 57], 'source_bruit_coment': [38, 40, 42, 44, 46]}.items():
            if typeCol == k:
                resultat = ' ; '.join([x[c] for c in df.iloc[:, v].columns if not pd.isnull(x[c]) and x[c] != 'Non'])
        if typeCol == 'perturbation':
            resultat = {re.split(' \[', re.sub('(?i)\[obligatoire\]', '', c))[1][:-1]:x[c] for c in df.iloc[:, [i for i in range(19,29)]].
                                   columns if not pd.isnull(x[c]) and x[c] != 'Non'}
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
        dfExploitable['route_source_coment'] = dfExploitable.apply(lambda x: self.simplifierColonneChoixMultipe(dfExploitable, x, 'route_source_coment'), axis=1)
        dfExploitable['source_bruit'] = dfExploitable.apply(lambda x: self.simplifierColonneChoixMultipe(dfExploitable, x, 'source_bruit'), axis=1)
        dfExploitable['source_bruit_coment'] = dfExploitable.apply(lambda x: self.simplifierColonneChoixMultipe(dfExploitable, x, 'source_bruit_coment'), axis=1)
        dfExploitable['perturbation'] = dfExploitable.apply(lambda x: self.simplifierColonneChoixMultipe(dfExploitable, x, 'perturbation'), axis=1)
        dfExploitable['perturbation'] = dfExploitable.perturbation.apply(lambda x: ' ; '.join([f'{k}: {v}' for k, v in x.items()]) if not pd.isnull(x) else x)
        
        # flecher les questionnaires papier et completer les adresses mails
        dfExploitable = dfExploitable.assign(papier= dfExploitable.mail.apply(lambda x: True if pd.isnull(x) else False))
        dfExploitable.loc[dfExploitable.mail.isna(), 'mail'] = dfExploitable.loc[dfExploitable.mail.isna()].nom+dfExploitable.loc[
            dfExploitable.mail.isna()].prenom+'@factice.com'
            
        # mettre en forme les notes de gene et sensibiilte
        dfExploitable.replace({'0 - pas du tout sensible': 0, '10 - extrêmement sensible': 10}, inplace=True)
        dfExploitable.replace({'0 - pas du tout gêné': 0, '10 - extrêmement gêné': 10}, inplace=True)
        return dfExploitable, dfNonExploitable
    
    
    def participantsGeneV0(self, dfExploitable):
        """
        creer la dataframe des participants avec mail, adresse, nom, prenom et la premiere version de la dataframe des decla de gene
        out : 
            dfExploitable : dataframe issue de creationDfExploitable()
            dfParticipantsAdresses : dataframe des particpants avec seulement mail, adresse, nom, prenom
            dfDeclarationGene : premiere version des declarations de gene
        """
        # tables des adresses et des participants
        dfParticipantsAdresses = dfExploitable.loc[~dfExploitable.iloc[:, 6].isna()].iloc[:, [6, 87, 88, 89]].reset_index(drop=True).drop_duplicates(
            ['adresse', 'nom', 'prenom', 'mail']).reset_index().copy()
            
        # trouver / nettoyer les participants en double avec des adresses différentes
        dfParticipantsAdresses.loc[dfParticipantsAdresses.duplicated('mail', keep=False)].sort_values('mail')  # analyse visuelle pour déterminer les lignes à enlever et les mettre dans le parametre dicoAdresseAEpurer
        dfParticipantsAdresses.drop(dfParticipantsAdresses.loc[(dfParticipantsAdresses.adresse.isin(dicoAdresseAEpurerRessenti['adresse'])) &
                                                                         (dfParticipantsAdresses.nom.isin(dicoAdresseAEpurerRessenti['nom'])) &
                                                                         (dfParticipantsAdresses.prenom.isin(dicoAdresseAEpurerRessenti['prenom']))
                                                                         ].index, inplace=True)
        dfDeclarationGene = dfExploitable.merge(dfParticipantsAdresses, on='mail', how='left').rename(
            columns={'adresse_y': 'adresse', 'nom_x': 'nom', 'prenom_x': 'prenom'})[list(mappingColonnesFixesRessenti.values())+
                                                                                    colonnesAjouteesRessenti]
        return dfParticipantsAdresses, dfDeclarationGene
        
        
    def listerParticipantsSansAdressePostale(self):
        """
        out : 
            participantsSansAdressePostale : liste d'adresse mail
        """
        return list(self.dfExploitable.loc[~self.dfExploitable.mail.isin(self.dfParticipantsAdresses.mail.tolist())].mail.unique())
    

    def creerDfParticipants(self, dfParticipantsAdresses, dfDeclarationGene):
        """
        creer la dataframe resume des participants : 1 ligne par adresse mail
        in : 
            dfParticipantsAdresses : issue de listerAdresseParticipants()
            dfDeclarationGene : df issue de declarationGeneFinale   
        """
        dfParticipant = dfParticipantsAdresses.merge(
            dfDeclarationGene[[c for c in dfDeclarationGene.columns if c not in ('adresse', 'nom', 'prenom')]],
            on='mail', how='left', suffixes=('', '_y')).drop_duplicates(
                ['adresse', 'genre', 'age', 'emploi', 'sensibilite_bruit', 'periode_travail', 'periode_travail_autre', 'sensib_bruit_travail', 
                 'gene_long_terme',
                 'gene_long_terme_6_18', 'gene_long_terme_18_22','gene_long_terme_22_6', 'bati_type', 'bati_annee'])[[
                     'mail', 'adresse', 'genre', 'age', 'emploi', 'sensibilite_bruit', 'periode_travail', 'periode_travail_autre', 
                     'sensib_bruit_travail', 'gene_long_terme','gene_long_terme_6_18', 'gene_long_terme_18_22','gene_long_terme_22_6', 'perturbation',
                     'bati_type', 'bati_annee', 'papier']]
        dfParticipant = dfParticipant.loc[~dfParticipant.iloc[:, 2:].isna().all(axis=1)].copy()
        self.dfParticipants = pd.concat([dfParticipant.loc[~dfParticipant.duplicated('mail', keep=False)
                                                             ].replace({'0 - pas du tout sensible': 0,
                                                                        '10 - extrêmement sensible': 10}),
                                                             dfParticipant.loc[dfParticipant.duplicated('mail', keep=False)].groupby('mail').agg(
                                                                 lambda x : regrouperLigneDfValeurNonNulle(x)).reset_index().replace(
                                                                     {'0 - pas du tout sensible': 0, '10 - extrêmement sensible': 10})
                                                                 ]).reset_index(drop=True).reset_index().rename(columns={'index':'id'})
        self.dfParticipants['age'] = self.dfParticipants.age.apply(lambda x: int(x.replace(' ans','')) if not pd.isnull(x) else x)
        self.dfParticipants['bati_annee'] = self.dfParticipants['bati_annee'].apply(lambda x: int(x[:4]) if not pd.isnull(x) else x)
    

    def creerDfDeclarations(self, dfDeclarationGene):
        """
        creer la datafrme des declarations : 1 ligne par declaration
        """
        dfDeclarations = dfDeclarationGene.drop('id', axis=1, errors='ignore').merge(
            self.dfParticipants[['id', 'mail']], on='mail')[
                ['id','debut_gene', 'fin_gene', 'duree_gene', 'note_gene', 'source_bruit', 'source_bruit_coment', 'route_source', 
                 'route_source_coment', 'vehicule_source', 'localisation_gene', 'qualif_bruit', 'coment']
                ].rename(columns={'id': 'id_participant'})
        # combler les trous dans debut ou fin
        dfDeclarations.loc[((dfDeclarations.debut_gene.notna()) & (dfDeclarations.fin_gene.notna())) |
                                    ((dfDeclarations.debut_gene.notna()) & (dfDeclarations.duree_gene.notna())) |
                                    ((dfDeclarations.fin_gene.notna()) & (dfDeclarations.duree_gene.notna())), 'debut_gene'
                                   ] = dfDeclarations.loc[((dfDeclarations.debut_gene.notna()) & (dfDeclarations.fin_gene.notna())) |
                                    ((dfDeclarations.debut_gene.notna()) & (dfDeclarations.duree_gene.notna())) |
                                    ((dfDeclarations.fin_gene.notna()) & (dfDeclarations.duree_gene.notna()))
                                                                  ].debut_gene.apply(lambda x: pd.to_datetime(f"2022-{x[5:]}") if not pd.isnull(x) else x)
        dfDeclarations.loc[((dfDeclarations.debut_gene.notna()) & (dfDeclarations.fin_gene.notna())) |
                                    ((dfDeclarations.debut_gene.notna()) & (dfDeclarations.duree_gene.notna())) |
                                    ((dfDeclarations.fin_gene.notna()) & (dfDeclarations.duree_gene.notna())), 'fin_gene'
                                   ] = dfDeclarations.loc[((dfDeclarations.debut_gene.notna()) & (dfDeclarations.fin_gene.notna())) |
                                    ((dfDeclarations.debut_gene.notna()) & (dfDeclarations.duree_gene.notna())) |
                                    ((dfDeclarations.fin_gene.notna()) & (dfDeclarations.duree_gene.notna()))
                                                                  ].apply(lambda x: pd.to_datetime(f"2022-{x.fin_gene[5:]}") if not pd.isnull(x.fin_gene) 
                                                                              else x.debut_gene + pd.to_timedelta(x.duree_gene, 'minute'), axis=1)
        # corriger les erreurs de debut ete fin inverse
        debutFinInverse = dfDeclarations.loc[dfDeclarations.debut_gene > dfDeclarations.fin_gene].copy()
        dfDeclarations.loc[dfDeclarations.index.isin(debutFinInverse.index.tolist()), 'debut_gene'] = debutFinInverse.fin_gene
        dfDeclarations.loc[dfDeclarations.index.isin(debutFinInverse.index.tolist()), 'fin_gene'] = debutFinInverse.debut_gene
        # mettre à jour les duree
        dfDeclarations['duree_gene'] = pd.to_timedelta(dfDeclarations['fin_gene']-dfDeclarations['debut_gene'], 'hour')
        
        # virer les retours lignes intempestifs
        dfDeclarations.coment = dfDeclarations.coment.apply(lambda x: x.replace('\n', ' ; ') if not pd.isnull(x) else x)
        
        # extraire les declarations valides
        self.dfDeclarations_KO = dfDeclarations.loc[((dfDeclarations.debut_gene.isna()) & (dfDeclarations.fin_gene.isna())) |
                                                    ((dfDeclarations.debut_gene.isna()) & (dfDeclarations.duree_gene.isna())) |
                                                    ((dfDeclarations.fin_gene.isna()) & (dfDeclarations.duree_gene.isna()))].copy()
        self.dfDeclarations = dfDeclarations.loc[((dfDeclarations.debut_gene.notna()) & (dfDeclarations.fin_gene.notna())) |
                                                 ((dfDeclarations.debut_gene.notna()) & (dfDeclarations.duree_gene.notna())) |
                                                 ((dfDeclarations.fin_gene.notna()) & (dfDeclarations.duree_gene.notna()))].copy()
                                                 
                                                 
    def exportVersBdd(self, bdd='ech24'):
        """
        exporter les fichiers mis en forme vers la bdd
        """
        with ct.ConnexionBdd(bdd) as c:
            self.dfParticipants.drop('mail', axis=1).to_sql('participant', c.sqlAlchemyConn, schema='ressenti', index=False, if_exists='append')
            self.dfDeclarations.assign(duree_gene=self.dfDeclarations.duree_gene.apply(lambda x: str(x))
                                           ).to_sql('situ_gene', c.sqlAlchemyConn, schema='ressenti', index=False, if_exists='append')
            self.dfDeclarations_KO.assign(duree_gene=self.dfDeclarations.duree_gene.apply(lambda x: str(x))
                                              ).to_sql('situ_gene_ko', c.sqlAlchemyConn, schema='ressenti', index=False, if_exists='append')
        return
        

class ResultatsEnquete(object):
    """
    classe d'import des resultats de l'enquete depuis la Bdd, avec traitement
    """  
    def __init__(self, bdd='ech24'):
        self.bdd = bdd
        with ct.ConnexionBdd(self.bdd) as c:
            self.dfParticipants = pd.read_sql('select * from ressenti.participant', c.sqlAlchemyConn)
            self.declarations_ok = pd.read_sql('select * from ressenti.situ_gene', c.sqlAlchemyConn)
            self.declarations_ko = pd.read_sql('select * from ressenti.situ_gene_ko', c.sqlAlchemyConn)
        self.miseEnformeAttributsListe()
        # creation des donnees
        self.dicoCharts = self.creationDfAnalyseRetour()
        
    
    def miseEnformeAttributsListe(self):
        self.declarations_ok['route_source'] = self.declarations_ok['route_source'].apply(
            lambda x: re.sub('( \(.*\)|\"|\{|\})', '', x).split(',') if not pd.isnull(x) else x)
        self.declarations_ok['source_bruit'] = self.declarations_ok['source_bruit'].apply(
            lambda x: re.sub('( \(.*\)|\"|\{|\})', '', x).split(',') if not pd.isnull(x) else x)
        self.declarations_ok['vehicule_source'] = self.declarations_ok['vehicule_source'].apply(
            lambda x: re.sub('(\"|\{|\})', '', re.sub('(?i)\([a-z]+, [a-z]+, \.\.\.\)', '', x)).split(',') if not pd.isnull(x) else x)
        self.declarations_ok['localisation_gene'] = self.declarations_ok['localisation_gene'].apply(
            lambda x: re.sub('( \(.*\)|\"|\{|\})', '', x).split(',') if not pd.isnull(x) else x)
        self.declarations_ok['qualif_bruit'] = self.declarations_ok['qualif_bruit'].apply(
            lambda x: re.sub('( \(.*\)|\"|\{|\})', '', x).split(',') if not pd.isnull(x) else x)
    
    
    def creationDfAnalyseRetour(self):
        """
        creer des df et du dico qui vont permettre d'analyser les réponses fournies dans le fichier d'enquete
        """
        # préparation des données pour analyse rapide par camembert des informatios relatives aux participants et à l'exploitation des déclarations
        # donnees aploitables ou non 
        exploitable = pd.DataFrame({'test': 'exploitable', 'valeurs': ['exploitable', 'Inexploitable'], 'nb_valeurs': [len(self.declarations_ok),
                                                                                                                             len(self.declarations_ko)]})
        # genre
        genre = pd.concat([self.dfParticipants.loc[(self.dfParticipants.genre.notna())].groupby('genre').id.count().reset_index().assign(test='genre').rename(
                columns={'id': 'nb_valeurs', 'genre': 'valeurs'}), pd.DataFrame({'valeurs': 'inconnu',
                                                                                 'nb_valeurs': len(self.dfParticipants.loc[(self.dfParticipants.genre.isna())]),
                                                                                 'test': 'genre'}, index=[0])]).reset_index(drop=True)
        # emploi
        emploi = pd.concat([self.dfParticipants.loc[(self.dfParticipants.emploi.notna())].groupby('emploi').id.count().reset_index().assign(test='emploi').rename(
                columns={'id': 'nb_valeurs', 'emploi': 'valeurs'}), pd.DataFrame({'valeurs': 'inconnu',
                                                                                 'nb_valeurs': len(self.dfParticipants.loc[(self.dfParticipants.emploi.isna())]),
                                                                                 'test': 'emploi'}, index=[0])]).reset_index(drop=True)
        
        
        # periode de travail
        periode_travail = pd.concat([self.dfParticipants.loc[(self.dfParticipants.periode_travail.notna())].groupby('periode_travail').id.count().reset_index().rename(
            columns={'id': 'nb_valeurs', 'periode_travail': 'valeurs'}), pd.DataFrame({'valeurs': 'inconnu', 
                                                                      'nb_valeurs': len(self.dfParticipants.loc[(self.dfParticipants.periode_travail.isna())]), 
                                                                      'test': 'periode_travail'}, index=[0])]).reset_index(drop=True)
        periode_travail_autre = self.dfParticipants.periode_travail_autre.loc[self.dfParticipants.periode_travail_autre.notna()].replace(
            '(Alternance Jour/nuit 12h|alternance Jour/nuit en cycle de 12h)', 'alternance Jour/nuit en cycle de 12h', regex=True).replace(
            '(.*en journée et surtout en télétravail.*|.*je travaille à mon domicile.*)', 'domicile / télétravail', regex=True).replace(
            '(Retraitée |RETRTAITEE|Retraité)', 'Retraite', regex=True).value_counts().reset_index().rename(columns={
                'index': 'valeurs', 'periode_travail_autre': 'nb_valeurs',})
        periode_travail = pd.concat([periode_travail_autre, periode_travail]).reset_index(drop=True).drop(3).groupby('valeurs').nb_valeurs.sum(
            ).reset_index().assign(test='periode_travail')
        # bati_type
        bati_type = pd.concat([self.dfParticipants.loc[(self.dfParticipants.bati_type.notna())].groupby('bati_type').id.count().reset_index().rename(
            columns={'id': 'nb_valeurs', 'bati_type': 'valeurs'}).assign(test='bati_type'), pd.DataFrame({'valeurs': 'inconnu', 
                                                                      'nb_valeurs': len(self.dfParticipants.loc[(self.dfParticipants.bati_type.isna())]), 
                                                                      'test': 'bati_type'}, index=[0])]).reset_index(drop=True)
        # bati annee
        batAnneeValues = self.dfParticipants.loc[(self.dfParticipants.bati_annee.notna())].groupby('bati_annee').id.count().reset_index().rename(
            columns={'id': 'nb_valeurs', 'bati_annee': 'valeurs'}).assign(test='bati_annee')
        bati_annee = batAnneeValues.groupby(np.where(batAnneeValues.valeurs>1999, 'apres loi 1999', 'avant loi 1999')
                                                   ).nb_valeurs.sum().reset_index().rename(columns={'index': 'valeurs', 'periode_travail_autre': 'nb_valeurs'}
                                                                                          ).assign(test='bati_annee')
        # sensibilite au bruit
        sensibBruit = pd.concat([self.dfParticipants.loc[(self.dfParticipants.sensibilite_bruit.notna())].groupby(
            'sensibilite_bruit').id.count().reset_index().assign(test='sensibilite_bruit').rename(
                columns={'id': 'nb_valeurs', 'sensibilite_bruit': 'valeurs'}), 
            pd.DataFrame({'valeurs': 'inconnu','nb_valeurs': len(self.dfParticipants.loc[(self.dfParticipants.sensibilite_bruit.isna())]),
                          'test': 'sensibilite_bruit'}, index=[0])]).reset_index(drop=True)
        # gene long terme
        GeneLgTerme = pd.concat([self.dfParticipants.loc[(self.dfParticipants.gene_long_terme.notna())].groupby(
            'gene_long_terme').id.count().reset_index().assign(test='gene_long_terme').rename(
                columns={'id': 'nb_valeurs', 'gene_long_terme': 'valeurs'}),
            pd.DataFrame({'valeurs': 'inconnu','nb_valeurs': len(self.dfParticipants.loc[(self.dfParticipants.gene_long_terme.isna())]),
                          'test': 'gene_long_terme'}, index=[0])]).reset_index(drop=True)
        # perturbation
        #     perturbation existence
        perturbationNotNaN = self.dfParticipants.loc[self.dfParticipants.perturbation.notna()].copy()
        perturbationNotNaN['perturbation'] = perturbationNotNaN['perturbation'].apply(lambda x: {z.split(': ')[0]: z.split(': ')[1] for z in x.split(' ; ')})
        perturbationNaN = self.dfParticipants.loc[self.dfParticipants.perturbation.isna()]
        perturbation = pd.DataFrame({'valeurs': ['inconnue', 'connue'], 'nb_valeurs': [len(perturbationNaN), len(perturbationNotNaN)], 'test': 'presence_perturbation'})
        
        
        # analyse des données de déclarations
        # regrouper les temporalités selon les durées
        def classerDureeGene(duree):
            if duree <= pd.to_timedelta(30, 'minutes'):
                return 'inf 30 minutes'
            elif pd.to_timedelta(30, 'minutes') < duree <= pd.to_timedelta(2, 'hour'):
                return 'entre 30 minutes et 2 heures'
            elif pd.to_timedelta(1, 'hour') < duree <= pd.to_timedelta(8, 'hour'):
                return 'entre 2 et 8 heures'
            elif pd.to_timedelta(8, 'hour') < duree <= pd.to_timedelta(24, 'hour'):
                return 'entre 8 et 24 heures'
            elif pd.to_timedelta(24, 'hour') <= duree:
                return 'superieur ou egale a 24h'
        
        
        # temporalité
        temporalite = pd.DataFrame({'valeurs': ['inconnue', 'connue'],'nb_valeurs': [len(self.declarations_ko), len(self.declarations_ok)],
                                    'test': 'presence_temporalite'})
        # standardisation de la duree
        self.declarations_ok['duree_standardisee'] = self.declarations_ok.duree_gene.apply(lambda x: classerDureeGene(x))
        duree_standardisee = self.declarations_ok.duree_standardisee.value_counts().reset_index().rename(
            columns={'index': 'valeurs', 'duree_standardisee': 'nb_valeurs'}).assign(test='duree_standardisee')
        # analyse des notes de gene
        note_gene_valeurs = self.declarations_ok.note_gene.value_counts().reset_index().rename(
            columns={'index': 'valeurs', 'note_gene': 'nb_valeurs'}).assign(test='note_gene')
        # sources de bruit
        source_bruit = self.declarations_ok.explode('source_bruit')[['id_participant', 'source_bruit']].groupby('source_bruit', dropna=False).id_participant.count(
            ).reset_index().fillna('inconnu').rename(columns={'source_bruit': 'valeurs', 'id_participant': 'nb_valeurs'}).assign(test='source_bruit')
        # route source
        route_source = self.declarations_ok.explode('route_source')[['id_participant', 'route_source']].groupby('route_source', dropna=False).id_participant.count(
            ).reset_index().fillna('inconnu').rename(columns={'route_source': 'valeurs', 'id_participant': 'nb_valeurs'}).assign(test='route_source')
        # vehicules source
        vehicule_source = self.declarations_ok.explode('vehicule_source')[['id_participant', 'vehicule_source']].groupby(
            'vehicule_source', dropna=False).id_participant.count().reset_index().fillna('inconnu').rename(
                columns={'vehicule_source': 'valeurs', 'id_participant': 'nb_valeurs'}).assign(test='vehicule_source')
        # localisation
        localisation_gene = self.declarations_ok.explode('localisation_gene')[['id_participant', 'localisation_gene']].groupby(
            'localisation_gene', dropna=False).id_participant.count().reset_index().fillna('inconnu').rename(
                columns={'localisation_gene': 'valeurs', 'id_participant': 'nb_valeurs'}).assign(test='localisation_gene')
        for d in (exploitable, genre, emploi, periode_travail, bati_type, bati_annee, sensibBruit, GeneLgTerme,
                  temporalite, duree_standardisee, note_gene_valeurs, source_bruit, route_source, vehicule_source, perturbation, localisation_gene):
            d['pctag'] = d.apply(lambda x: f"{x.nb_valeurs} ({int(x.nb_valeurs / d.nb_valeurs.sum()*100)}%)", axis=1)
            d.replace('(é|è|ê)', 'e', regex=True, inplace=True)
            d.replace('ç', 'c', regex=True, inplace=True)
            d.replace('(ï|î)', 'i', regex=True, inplace=True)
            
        # cas speciaux des graphs autres que circuilaires
        # graph de relation entre la sensibilite et la gene et entre les perturbation et leurs occurences
        # sensibiite et gene
        dfNoteGenesensibilite = self.declarations_ok[['id_participant', 'note_gene']].merge(self.dfParticipants[['id', 'sensibilite_bruit']],
                                                                                           left_on='id_participant', right_on='id').drop('id', axis=1)
        # perturbation et occurence
        dfPerturbation = pd.concat([pd.DataFrame.from_dict(v, orient='index', columns=['frequence',]).reset_index() 
                                    for v in perturbationNotNaN['perturbation'].to_dict().values()]).rename(columns={'index': 'perturbation_type'}
                                                                                                            ).replace('(é|è|ê)', 'e', regex=True)               
        # preparation des graphs
        dicoCharts = {'sensibilite': {'data': sensibBruit, 
                                      'titre': 'Repartition des participants selon leur sensibilite au bruit',
                                      'angle': 45}, 
                      'gene': {'data': GeneLgTerme, 
                               'titre': ['Repartition des participants selon', 'la gene ressenti sur les 12 derniers mois'],
                               'angle': 45},
                      'genre': {'data': genre, 'titre': 'Repartition des participants selon leur genre', 'angle': 0}, 
                      'emploi': {'data': emploi, 'titre': 'Repartition des participants selon leur emploi', 'angle': 45},
                      'periode_travail': {'data': periode_travail, 'titre': 'Repartition des participants selon leur periode de travail', 'angle': 0},
                      'bati_type': {'data': bati_type, 'titre': 'Repartition des participants selon leur type de logement', 'angle': 0},
                      'bati_annee': {'data': bati_annee, 'titre': ['Repartition des logements des participants', 'selon leur annee de construction']
                                     , 'angle': 0},
                      'exploitable': {'data': exploitable, 'titre': 'Repartition des declarations de gene selon leur exploitabilite', 'angle': 0},
                      'temporalite': {'data': temporalite, 'titre': ['Repartition des declarations de gene selon', 'la presence des donnees de temporalite'], 'angle': 0},
                      'duree_standardisee': {'data': duree_standardisee, 'titre': ['Repartition des declarations selon', 'la duree de la gene'],
                                             'angle': 0},
                      'note_gene_valeurs': {'data': note_gene_valeurs, 'titre': ['Repartition des declarations selon', 'la note de gene declaree'],
                                            'angle': 80},
                      'source_bruit': {'data': source_bruit, 'titre': ['Repartition des declarations selon', 'la source de bruit'],'angle': 60}, 
                      'route_source': {'data': route_source, 'titre': ['Repartition des declarations selon', 'la route source du bruit'],'angle': 60},
                      'vehicule_source': {'data': vehicule_source, 'titre': ['Repartition des declarations selon', 'les vehicules source du bruit'],
                                          'angle': 0},
                      'perturbation': {'data': perturbation, 'titre': ['Repartition des participants selon', 'les declarations de perturbation'],
                                       'angle': 0},
                      'note_gene_senibilite': {'data': dfNoteGenesensibilite, 'titre': ["Nombre de declaration", "de gene selon la sensibilite"],
                                               'angle': 0},
                      'localisation_gene': {'data': localisation_gene, 'titre': ["Nombre de declaration de gene", "selon la localisation du ressenti"],
                                               'angle': 0},
                      'perturbation_occurence': {'data': dfPerturbation, 'titre': ["Declarations d'occurence de perturbations"],'angle': 0}}
        return dicoCharts


    def creationCharts(self, export=True):
        """
        création des charts issues de l'analyse des déclarations de gene. export des charts dans le dossier 
        specifie dans le moduel de parametre
        in : 
            dicoCharts : dictionnaire produit par creationDfAnalyseRetour()
        out :
            dicoCharts : ajout de la clé 'chart' au dictionnaire en entrée
        """
        for e in self.dicoCharts.keys():
            t = self.dicoCharts[e]
            if e not in ('note_gene_senibilite', 'perturbation_occurence'): #  Charts en forme de camembert
                base = alt.Chart(t['data'], title=t['titre']).encode(
                        theta=alt.Theta(field="nb_valeurs", type="quantitative", stack=True, sort='ascending'),
                        color=alt.Color(field="valeurs", type="nominal", scale=alt.Scale(scheme='category20'),
                                        sort='ascending'))
                pie = base.mark_arc(outerRadius=120, cornerRadius=50)
                text = base.mark_text(radius=155, size=15, angle=t['angle'], fontStyle='bold').encode(text="pctag:N")
                t['chart'] = (pie + text).configure_legend(labelLimit=0)
            elif e == 'note_gene_senibilite': # Charts spéciales 
                t['chart'] = alt.Chart(t['data'].replace({'0 - pas du tout gêné': 0, '10 - extrêmement gêné': 10}), title=t['titre']
                                            ).mark_circle(size=100).encode(
                x=alt.X('sensibilite_bruit:Q', title='sensibilite au bruit'),
                y=alt.Y('note_gene:Q', title='Note de gene'),
                size=alt.Size('count(note_gene):Q', title=['Nombre de', 'declaration'])).properties(width=500, height=300)
            elif e == 'perturbation_occurence':
                t['chart'] = alt.Chart(t['data'], title=t['titre']).mark_circle().encode(
                    x=alt.X('perturbation_type', title=None, axis=alt.Axis(labelAngle=60, labelFontSize=13, labelLimit=0)),
                    y=alt.Y('frequence', title=None, sort=['Tout le temps', 'Assez souvent', 'Occasionnellement', 'Jamais'], axis=alt.Axis(labelAngle=60, labelFontSize=15)), 
                    size=alt.Size('count(perturbation_type)', title=['Nombre de', 'declarations'])).properties(width=500, height=500)
            if export:
                t['chart'].save(os.path.join(dossierExportChartsRessenti, e+'.svg'))       
        return

class FichiersMeteo(object):
    """
    gestion du ou des fichiers bruts d'acquisition de la meteo
    attributs : 
        dossier : raw string du dossier contenant les fichiers meteo
        listFichiers : liste des fichiers meteo a analyser
        dfBrutes : df issue de la lecture des fichiers
    methodes : 
        ouvrirFichier()
        analyseDirVents(dfBrute)
        graphDiffDirVent(dfDiffDirVent, fichierSortie)
    """
    def __init__(self, dossier, listFichiers):
        self.dossier = dossier
        self.listFichiers = listFichiers
        self.importBdd()
        
    def ouvrirFichier(self):
        """
        lecture des fichiers bruts
        """
        self.dfBrutes = pd.concat([pd.read_csv(os.path.join(self.dossier, f), skiprows=2, usecols=[0, 1, 2, 4, 5, 6, 9, 10, 11, 13, 14, 15],
                                               names=colonnesMeteo,
                                               parse_dates=[1], dayfirst=True) for f in self.listFichiers])
        
    
    def importBdd(self):
        with ct.ConnexionBdd(bdd) as c: 
            self.dfBrutes = pd.read_sql('select * from mesures_physiques.meteo', c.sqlAlchemyConn)
        
        
    def analyseDirVents(self, dfBrute):
        """
        ajout des attributs nécéssaire à la caractérisation des différences entre les girouettes haute et basses
        in : 
            dfBrute : df issue de ouvrirFichier()
        out : 
            dfBruteCopy : dfBrute avec ajout des attributs dirVentBas_abs, dirVentHaut_abs, diff_dir_vent, diff_dir_vent_class
        """
        dfBruteCopy = dfBrute.copy()
        dfBruteCopy['dirVentBas_abs'] = abs((dfBruteCopy.dirVentBas*pi/180)-pi)*180/pi
        dfBruteCopy['dirVentHaut_abs'] = abs((dfBruteCopy.dirVentHaut*pi/180)-pi)*180/pi
        dfBruteCopy['diff_dir_vent'] = abs(dfBruteCopy['dirVentBas_abs'] - dfBruteCopy['dirVentHaut_abs'])
        dfBruteCopy['diff_dir_vent_class'] = pd.cut(dfBruteCopy['diff_dir_vent'], [-1,20,40, 60, 80, 100, 1000], labels=['0 a 20', '20 a 40', '40 a 60', '60 a 80', '80 a 100', 'sup 100'])
        return dfBruteCopy
    
    
    def graphDiffDirVent(self, dfDiffDirVent, fichierSortie):
        """
        creation de la chartde visualisation des différences entre girouette haute et basse
        in : 
            dfDiffDirVent : df passée par analyseDirVents()
        """
        alt.Chart(dfDiffDirVent, title='Ecart entre les directions de vent haut et bas').mark_point().encode(
            x=alt.X('dirVentBas', title='Direction du vent girouette basse'),
            y=alt.Y('dirVentHaut', title='Direction du vent girouette haute'),
            color=alt.Color('diff_dir_vent_class:N', title=["Valeur de l'ecart"])).properties(width=600, height=600).save(fichierSortie)
    