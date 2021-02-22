# -*- coding: utf-8 -*-
'''
Created on 22 f�vr. 2021

@author: Martin

Module d'analyse des emissions 
'''

""" ================================================
PARTIE 1 : analyse des donnes de test sur la Rocade
===================================================="""

import os
import pandas as pd

import Bruit.Emission as be
import Import_trafics as it
import Outils as O



def importFichierTraficDIRA(fichiersSynthese, dossierHoraire, annee, bdd, tableComptage, nomFichierHoraire):
    """
    ouvertur d'un fihcier DIRA en se basant sur la creation pour l'OTV
    in : 
        dossierHoraire : rawstring du chemin vers le fichier HOraire
        nomFichierHoraire : nom du fichier a utiliser 
        fichiersSynthese : sans importance
        annee, bdd, tableComptage : parametres lies au code OTV. cf module Import_trafics du projet OTV. permet de recuperer les id_dira existant, sinon ça foire
    out : 
        dfHoraireStack : df des donnees qui vont servir au calcul d'emission 'stackees', avec pour attribut l'heuere, le type de Veh, le nb de veh 
        dfHoraireEmission : df des données d'emissions calculees, par heure et par jour
    """
    #utilisation des codes OTV
    dira=it.Comptage_Dira(fichiersSynthese,
                          dossierHoraire,
                          annee, bdd, tableComptage)
    #Creer les données horaires format OTV, puis la passer pour un format de calcul
    dfHoraireOtv=dira.miseEnFormeFichier(nomFichierHoraire,6)
    dfHoraireOtv['typeJour']=dfHoraireOtv.jour.apply(lambda x : O.typeJour(x)) 
    dfHoraireOtv['sens']=dfHoraireOtv.voie.apply(lambda x : x.lower().replace(' ',''))
    dfHoraireOtv.set_index(['jour', 'type_veh','sens','typeJour'],inplace=True)
    dfHoraireStack=dfHoraireOtv[[c for c in dfHoraireOtv.columns if c[0]=='h']].stack().reset_index().rename(columns={'level_4':'heure',0:'nbVeh'})
    dfHoraireGroup=dfHoraireStack.groupby(['jour','heure','type_veh']).nbVeh.sum().reset_index()
    dfHoraireEmission=dfHoraireGroup.loc[dfHoraireGroup.type_veh=='VL'].merge(dfHoraireGroup.loc[dfHoraireGroup.type_veh=='PL'][['jour','heure','nbVeh']], on=['jour','heure'],suffixes=('VL','PL'))
    #mettre en forme les données
    dfHoraireEmission['emission']=dfHoraireEmission.apply(lambda x : be.Route(x.nbVehVL, x.nbVehPL, 90, 80).lwm, axis=1)
    dfHoraireEmission['dateHeure']=dfHoraireEmission.apply(lambda x : pd.to_datetime(f"{x.jour.strftime('%Y-%m-%d')} {x.heure.split('_')[0][1:]}:00:00"), axis=1)
    return dfHoraireStack, dfHoraireEmission

def importFichierVtsGroupe(dossier, fichierVts):
    """
    depuis un fichier de vitesse regroupe ne jours ouvré, samedi, dimanche, fournir une df utilisable pour un calcul de niveau
    in : 
       dossier : raw string du dossier contenant les vitesses
       fichierVts  : string : fichier vitesse
    out : 
       dFVtsFinale : df des vitesses par sens, type de jour et heure, en V85 et Vmoy
    """
    dfVts=pd.read_excel(os.path.join(dossier, fichierVts),sheet_name=None, skiprows=8, nrows=24)
    listDfVts=[]
    for i,s in ((0,'sensexter'),(1,'sensinter')) : 
        dFSensComplet=[dfVts[k] for k in dfVts.keys() if not 'Voie' in k and k[:2]!="xx"][i]
        listDfVts.append(pd.concat([dFSensComplet.iloc[:,[0,12,15]].rename(columns={'vitesse moyenne':'vMoy'}).assign(typeJour='JO', sens=s), 
               dFSensComplet.iloc[:,[0,28,31]].rename(columns={'vitesse moyenne.1':'vMoy','V85.1':'V85'}).assign(typeJour='Samedi', sens=s),
               dFSensComplet.iloc[:,[0,44,47]].rename(columns={'vitesse moyenne.2':'vMoy','V85.2':'V85'}).assign(typeJour='Dimanche', sens=s)]))
    dFVtsFinale=pd.concat(listDfVts)
    dFVtsFinale['sequence']=dFVtsFinale.sequence.apply(lambda x : 'h'+x.replace('-','_')[:-1])
    dFVtsFinale.rename(columns={'sequence':'heure'}, inplace=True)
    return dFVtsFinale

def emissionSensVts(dfHoraireStack,dFVtsFinale):
    """
    calcul de l'emission par sens, Type de jour et heure, en se basant sur les données de vitesse. L'emission est egale à la somme énergétique
    attention, sans données de vitesse propre aux PL on prend la vitesse PL = vitesseVL -5 
    in : 
        dfHoraireStack : df des donnees de debit horaire issue de importFichierTraficDIRA()
        dFVtsFinale : df des vitesses par sens, type de jour et heure, issue de importFichierVtsGroupe
    out : 
        dfEmissionSensHeureJour : jointure entre les debits et les vitesses, avec une donnees VL, PL et vitesse par heure, type de jour et sens 
    """
    dfHoraireStack.nbVeh.fillna(0, inplace=True)
    dfGroupTypeJour=dfHoraireStack.groupby(['typeJour','heure','type_veh', 'sens']).nbVeh.mean().reset_index()
    dFSensHeureJour=dfGroupTypeJour.loc[dfGroupTypeJour.type_veh=='VL'].merge(dfGroupTypeJour.loc[dfGroupTypeJour.type_veh=='PL'][['typeJour','heure','nbVeh','sens']], 
                                                            on=['typeJour','heure','sens'],suffixes=('VL','PL')).merge(dFVtsFinale, on=['typeJour','heure','sens']).drop('type_veh', axis=1)
    dFSensHeureJour['emission']=dFSensHeureJour.apply(lambda x : be.Route(x.nbVehVL, x.nbVehPL, x.vMoy,x.vMoy-5).lwm, axis=1)
    dfEmissionSensHeureJour=dFSensHeureJour.loc[dFSensHeureJour.sens=='sensexter'][['heure','typeJour','emission','nbVehVL','nbVehPL','vMoy']].merge(
        dFSensHeureJour.loc[dFSensHeureJour.sens=='sensinter'][['heure','typeJour','emission','nbVehVL','nbVehPL','vMoy']],
        on=['heure','typeJour'],suffixes=('Exter','Inter'))
    dfEmissionSensHeureJour['emission_tot']=dfEmissionSensHeureJour.apply(lambda x : be.sommeEnergetique(x.emissionExter,x.emissionInter), axis=1) 
    return dfEmissionSensHeureJour

def compEmission(dossier, tupleFichiers):
    """
    comparaison des donnees d'emission pour plusieurs fichiers de trafic associes a des fichiers de vitesses, en JO
    in : 
        dossier : dossier contenant les fichiers debit et vitesse
        tupleFichiers : tuple de tule de fichier de la forme (ficherTrafic, fichierVitesse, descriptifPeriode) 
    out : 
        dfEmissionToutePeriode : df des emissions moyennes par heure et période, 2 sens confondus
        dfVtsToutePeriode : df des vitesses moyennes par heure, par sens, et par période,
        dfTraficToutePeriode : df des trafics moyens par heure, par sens, et par période,
    """ 
    listDfJO,listDfVts,listDfTraf=[], [], []
    for fichier, vts, p in tupleFichiers : 
        dfHoraireStack=importFichierTraficDIRA('totoFake', dossier, '2019', 'local_otv_gti', 'na_2010_2019_p', fichier)[0]
        dFVtsFinale=importFichierVtsGroupe(dossier, vts)
        dfEmissionSensHeureJour=emissionSensVts(dfHoraireStack,dFVtsFinale)
        listDfVts.append(dFVtsFinale.loc[dFVtsFinale.typeJour=='JO'].assign(periode=p))
        listDfTraf.append(dfHoraireStack.loc[dfHoraireStack.typeJour=='JO'].groupby(['heure','sens']).nbVeh.sum().reset_index().assign(periode=p))
        listDfJO.append(dfEmissionSensHeureJour.loc[dfEmissionSensHeureJour.typeJour=='JO'].assign(periode=p))
    dfEmissionToutePeriode, dfVtsToutePeriode, dfTraficToutePeriode= pd.concat(listDfJO), pd.concat(listDfVts), pd.concat(listDfTraf)
    return dfEmissionToutePeriode, dfVtsToutePeriode, dfTraficToutePeriode



