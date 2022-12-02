# -*- coding: utf-8 -*-
'''
Created on 7 oct. 2022

@author: martin.schoreisz
extraire et traiter les donnees de trafic
'''

import pandas as pd
from Connexions.Connexion_Transfert import ConnexionBdd
from Import_stockage_donnees.Params import bdd, enum_period_agreg, enum_indicateur, enum_instru_site  # , startDateMesure, endDateMesure
from Outils.Outils import checkParamValues



def recupDonneesTraficBase(id_instru_site, sens=None, voie=None, indicateur=None, periodeAgreg=None):
    """
    telechager les donnees trafic source
    in : 
        id_instru_site : integer parmi les valeurs de la bdd
        sens: listde string
        voie : list de string parmi les valeurs possible de la bdd
        indicateur : list de string
        periodeAgreg : list de string
    """
    dicoParams = {'sens': sens, 'voie': voie, 'indicateur': indicateur, 'periodeAgreg': periodeAgreg}
    for k, v in dicoParams.items():
        if v and not (isinstance(v, list)):
            raise TypeError(f"{k} doit etre de type list")
    if periodeAgreg and any([e for e in periodeAgreg if e not in enum_period_agreg]):
        raise ValueError(f"une des valeur de periode n'est pas presente dans la bdd")
    if indicateur and any([e for e in indicateur if e not in enum_indicateur]):
        raise ValueError(f"une des valeur de indicateur n'est pas presente dans la bdd")                            

    if not voie and not indicateur and not sens and not periodeAgreg:
        rqt = f"""SELECT *
                  FROM mesures_physiques.trafic
                  WHERE id_instru_site = {id_instru_site}"""
    elif voie and indicateur and sens and periodeAgreg:
        rqt = f"""SELECT *
                  FROM mesures_physiques.trafic
                  WHERE id_instru_site = {id_instru_site} and sens = any(ARRAY{sens}) and voie = any(ARRAY{voie}) 
                      and indicateur = any(ARRAY{indicateur}) and periode_agreg = any(ARRAY{periodeAgreg})"""
    elif sens and voie and not indicateur and not periodeAgreg:
        rqt = f"""SELECT *
                  FROM mesures_physiques.trafic
                  WHERE id_instru_site = {id_instru_site} and sens = any(ARRAY{sens}) and voie = any(ARRAY{voie}) """
    elif sens and voie and not indicateur and periodeAgreg:
        rqt = f"""SELECT *
                  FROM mesures_physiques.trafic
                  WHERE id_instru_site = {id_instru_site} and sens = any(ARRAY{sens}) and voie = any(ARRAY{voie}) and periode_agreg = any(ARRAY{periodeAgreg})"""
    elif sens and voie and indicateur and not periodeAgreg:
        rqt = f"""SELECT *
                  FROM mesures_physiques.trafic
                  WHERE id_instru_site = {id_instru_site} and sens = any(ARRAY{sens}) and voie = any(ARRAY{voie}) and indicateur = any(ARRAY{indicateur})"""
    elif sens and not voie and indicateur and periodeAgreg:
        rqt = f"""SELECT *
                  FROM mesures_physiques.trafic
                  WHERE id_instru_site = {id_instru_site} and sens = any(ARRAY{sens}) and indicateur = any(ARRAY{indicateur}) and periode_agreg = any(ARRAY{periodeAgreg})"""
    elif sens and not voie and not indicateur and not periodeAgreg:
        rqt = f"""SELECT *
                  FROM mesures_physiques.trafic
                  WHERE id_instru_site = {id_instru_site} and sens = any(ARRAY{sens})"""
    elif sens and not voie and not indicateur and periodeAgreg:
        rqt = f"""SELECT *
                  FROM mesures_physiques.trafic
                  WHERE id_instru_site = {id_instru_site} and sens = any(ARRAY{sens}) and periode_agreg = any(ARRAY{periodeAgreg})"""
    elif not sens and voie and not indicateur and not periodeAgreg:
        rqt = f"""SELECT *
                  FROM mesures_physiques.trafic
                  WHERE id_instru_site = {id_instru_site} and voie = any(ARRAY{voie})"""
    elif not sens and voie and not indicateur  and periodeAgreg:
        rqt = f"""SELECT *
                  FROM mesures_physiques.trafic
                  WHERE id_instru_site = {id_instru_site} and voie = any(ARRAY{voie}) and periode_agreg = any(ARRAY{periodeAgreg})"""
    elif not sens and voie and indicateur and not periodeAgreg:
        rqt = f"""SELECT *
                  FROM mesures_physiques.trafic
                  WHERE id_instru_site = {id_instru_site} and voie = any(ARRAY{voie}) and indicateur = any(ARRAY{indicateur})"""
    elif not sens and voie and indicateur and periodeAgreg:
        rqt = f"""SELECT *
                  FROM mesures_physiques.trafic
                  WHERE id_instru_site = {id_instru_site} and voie = any(ARRAY{voie}) and indicateur = any(ARRAY{indicateur}) and periode_agreg = any(ARRAY{periodeAgreg})"""                  
    elif not sens and not voie and indicateur and not periodeAgreg:
        rqt = f"""SELECT *
                  FROM mesures_physiques.trafic
                  WHERE id_instru_site = {id_instru_site} and indicateur = any(ARRAY{indicateur})"""
    elif not sens and not voie and indicateur and periodeAgreg:
        rqt = f"""SELECT *
                  FROM mesures_physiques.trafic
                  WHERE id_instru_site = {id_instru_site} and indicateur = any(ARRAY{indicateur}) and periode_agreg = any(ARRAY{periodeAgreg})"""                  
    else:
        raise NotImplementedError(f"la combinaison id_instru_site, voie, sens, indicateur n'est pas implémentée")
    with ConnexionBdd(bdd) as c:
        df = pd.read_sql(rqt, c.sqlAlchemyConn)
    return df


def recupPourcentagePL(id_instru_site, sens=None, voie=None):
    """
    telechager les donnees de pourcentage de PL
    in : 
        id_instru_site : integer parmi les valeurs de la bdd
        sens: listde string
        voie : list de string parmi les valeurs possible de la bdd
        periodeAgreg : list de string
    """
    dicoParams = {'sens': sens, 'voie': voie}
    for k, v in dicoParams.items():
        if v and not (isinstance(v, list)):
            raise TypeError(f"{k} doit etre de type list")
    if not voie and not sens:
        rqt = f"""SELECT *
                  FROM mesures_physiques.vm_pc_pl
                  WHERE id_instru_site = {id_instru_site}"""
    elif voie and sens:
        rqt = f"""SELECT *
                  FROM mesures_physiques.vm_pc_pl
                  WHERE id_instru_site = {id_instru_site} and sens = any(ARRAY{sens}) and voie = any(ARRAY{voie})"""
    elif sens and not voie:
        rqt = f"""SELECT *
                  FROM mesures_physiques.vm_pc_pl
                  WHERE id_instru_site = {id_instru_site} and sens = any(ARRAY{sens})"""
    elif not sens and voie:
        rqt = f"""SELECT *
                  FROM mesures_physiques.vm_pc_pl
                  WHERE id_instru_site = {id_instru_site} and voie = any(ARRAY{voie})"""              
    else:
        raise NotImplementedError(f"la combinaison id_instru_site, voie, sens, indicateur n'est pas implémentée")
    with ConnexionBdd(bdd) as c:
        df = pd.read_sql(rqt, c.sqlAlchemyConn)
    return df

def recupVlPl6min(id_instru_site, sens=None, voie=None):
    """
    Calculer les volumes  par catégorie de véhicuels par pas de 6 minutes. dans ce cas la periode agreg et l'indicateur sont figés
    in : 
        id_instru_site : integer
        periode_agreg : periodes autorisées
    """
    # recup des donnees 6 min ute et 1 h
    dfTraficParSens6min = recupDonneesTraficBase(id_instru_site, sens=sens, voie=voie, indicateur=['TV'], periodeAgreg=['6 min'])
    dfPcPLParSens1h = recupPourcentagePL(id_instru_site, voie=voie)
    # jointure et calcul des attributs
    dfMerge6min1h = (dfTraficParSens6min.assign(jour=dfTraficParSens6min.date_heure.dt.dayofyear,
                                                heure=dfTraficParSens6min.date_heure.dt.hour)
                     .merge(dfPcPLParSens1h
                                .assign(jour=dfPcPLParSens1h.date_heure.dt.dayofyear, heure=dfPcPLParSens1h.date_heure.dt.hour)
                                .drop(['id_instru_site', 'date_heure'], axis=1, errors='ignore')
                            , on=['jour', 'heure', 'sens', 'voie']))
    dfNbVLPL6min = (dfMerge6min1h.assign(VL=dfMerge6min1h.valeur * (1 - (dfMerge6min1h.pc_pl/100)), 
                                          PL=dfMerge6min1h.valeur * dfMerge6min1h.pc_pl/100)
                                  .melt(id_vars=['date_heure', 'id_instru_site', 'jour', 'voie', 'sens'], value_vars=['VL', 'PL'], var_name='indicateur', value_name='valeurs')
                                  .rename(columns={'valeurs': 'valeur'}))
    return dfNbVLPL6min

def recupVlPl6min2Sens(id_instru_site):
    """
    somme des 2 sens. dans ce cas pas de décomposition par voie
    si lla données source présente de la section courante + autre chose il faur la filter avant
    in : 
        id_instru_site : integer
        voie : list de string parmi les valeurs possible de la bdd
    """
    checkParamValues(id_instru_site, enum_instru_site)
    dfNbVLPL6min = recupVlPl6min(id_instru_site)
    if (dfNbVLPL6min.voie == 'section courante').any() and (dfNbVLPL6min.voie != 'section courante').any():
        dfNbVLPL6min = dfNbVLPL6min.loc[dfNbVLPL6min.voie != 'section courante']
    if id_instru_site in (6, 7, 8):
        dfNbVLPL6min2sens = dfNbVLPL6min.groupby(['date_heure', 'id_instru_site', 'indicateur']).valeur.sum().reset_index().assign(voie='section courante')
    elif id_instru_site in (9, ):
        dfNbVLPL6min2sens = dfNbVLPL6min.groupby(['date_heure', 'id_instru_site', 'indicateur', 'voie']).valeur.sum().reset_index()
    else:
        raise NotImplementedError(f"le site {id_instru_site} n'est pas implémenté dans la code")
    return dfNbVLPL6min2sens
    
    