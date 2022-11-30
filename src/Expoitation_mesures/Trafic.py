# -*- coding: utf-8 -*-
'''
Created on 7 oct. 2022

@author: martin.schoreisz
extraire et traiter les donnees de trafic
'''

import pandas as pd
import altair as alt
from datetime import datetime
from Import_stockage_donnees.Params import bdd, enum_period_agreg  # , startDateMesure, endDateMesure
from Connexions.Connexion_Transfert import ConnexionBdd
from Outils.Outils import checkParamValues, checkAttributsinDf


def recupDonneesTraficBase(id_instru_site, sens=None, voie=None, indicateur=None):
    """
    telechager les donnees trafic source
    in : 
        id_instru_site : integer parmi les valeurs de la bdd
        voie : list de string parmi les valeurs possible de la bdd
    """
    dicoParams = {'sens': sens, 'voie': voie, 'indicateur': indicateur}
    for k, v in dicoParams.items():
        if v and not (isinstance(v, list)):
            raise TypeError(f"{k} doit etre de type list")
    if not voie and not indicateur and not sens:
        rqt = f"""SELECT *
                  FROM mesures_physiques.trafic
                  WHERE id_instru_site = {id_instru_site}"""
    elif voie and indicateur and sens:
        rqt = f"""SELECT *
                  FROM mesures_physiques.trafic
                  WHERE id_instru_site = {id_instru_site} and sens = any(ARRAY{sens}) and voie = any(ARRAY{voie}) and indicateur = any(ARRAY{indicateur})"""
    elif sens and voie and not indicateur:
        rqt = f"""SELECT *
                  FROM mesures_physiques.trafic
                  WHERE id_instru_site = {id_instru_site} and sens = any(ARRAY{sens}) and voie = any(ARRAY{voie})"""
    elif sens and not voie and indicateur:
        rqt = f"""SELECT *
                  FROM mesures_physiques.trafic
                  WHERE id_instru_site = {id_instru_site} and sens = any(ARRAY{sens}) and indicateur = any(ARRAY{indicateur})"""
    elif sens and not voie and not indicateur:
        rqt = f"""SELECT *
                  FROM mesures_physiques.trafic
                  WHERE id_instru_site = {id_instru_site} and sens = any(ARRAY{sens})"""
    elif not sens and voie and not indicateur:
        rqt = f"""SELECT *
                  FROM mesures_physiques.trafic
                  WHERE id_instru_site = {id_instru_site} and voie = any(ARRAY{voie})"""
    elif not sens and voie and indicateur:
        rqt = f"""SELECT *
                  FROM mesures_physiques.trafic
                  WHERE id_instru_site = {id_instru_site} and voie = any(ARRAY{voie}) and indicateur = any(ARRAY{indicateur})"""
    elif not sens and not voie and indicateur:
        rqt = f"""SELECT *
                  FROM mesures_physiques.trafic
                  WHERE id_instru_site = {id_instru_site} and indicateur = any(ARRAY{indicateur})"""
    else:
        raise NotImplementedError(f"la combinaison id_instru_site, voie, sens, indicateur n'est pas implémentée")
    with ConnexionBdd(bdd) as c:
        df = pd.read_sql(rqt, c.sqlAlchemyConn)
    return df


def recupDonneesTraficVlPl(id_instru_site, periode_agreg):
    """
    telechager les donnees trafic avec decomposition VL / PL
    in : 
        id_instru_site : integer
        periode_agreg : periodes autorisées
    """
    checkParamValues(periode_agreg, enum_period_agreg)
    with ConnexionBdd(bdd) as c:
        rqt = f"""SELECT *
                  FROM mesures_physiques.vl_pl_6min
                  WHERE id_instru_site = {id_instru_site} and periode_agreg = '{periode_agreg}'"""
        df = pd.read_sql(rqt, c.sqlAlchemyConn)
    return df

def recupDonneesTraficVlPl2Sens(id_instru_site):
    """
    telechager les donnees trafic avec decomposition VL / PL
    in : 
        id_instru_site : integer
        periode_agreg : periodes autorisées
    """
    with ConnexionBdd(bdd) as c:
        rqt = f"""SELECT *
                  FROM mesures_physiques.vl_pl_6_min_2sens
                  WHERE id_instru_site = {id_instru_site}"""
        df = pd.read_sql(rqt, c.sqlAlchemyConn)
    return df
    
    