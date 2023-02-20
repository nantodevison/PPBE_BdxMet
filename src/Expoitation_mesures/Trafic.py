# -*- coding: utf-8 -*-
"""
Created on 7 oct. 2022

@author: martin.schoreisz
extraire et traiter les donnees de trafic
"""

import pandas as pd
import altair as alt
import numpy as np
from Connexions.Connexion_Transfert import ConnexionBdd
from Import_stockage_donnees.Params import (
    bdd,
    enum_period_agreg,
    enum_indicateur,
    enum_instru_site,
    enum_trafic_sens,
    enum_trafic_voie,
    dicoJourSensVoieKO,
)  # , startDateMesure, endDateMesure
from Outils.Outils import checkParamValues, checkAttributsinDf, dateTexteDepuisDayOfYear, checkValuesInAttribut
from Bruit.Niveaux import sommeEnergetique
from Bruit.Emission import Route


def recupDonneesTraficBase(
    id_instru_site, sens=None, voie=None, indicateur=None, periodeAgreg=None
):
    """
    telechager les donnees trafic source
    in :
        id_instru_site : integer parmi les valeurs de la bdd
        sens: listde string
        voie : list de string parmi les valeurs possible de la bdd
        indicateur : list de string
        periodeAgreg : list de string
    """
    dicoParams = {
        "sens": sens,
        "voie": voie,
        "indicateur": indicateur,
        "periodeAgreg": periodeAgreg,
    }
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
    elif not sens and voie and not indicateur and periodeAgreg:
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
        raise NotImplementedError(
            f"la combinaison id_instru_site, voie, sens, indicateur n'est pas implémentée"
        )
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
    """
    dicoParams = {"sens": sens, "voie": voie}
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
        raise NotImplementedError(
            f"la combinaison id_instru_site, voie, sens n'est pas implémentée"
        )
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
    # pour la rocade
    if id_instru_site in (7,8):
        # recup des donnees 6 minute et 1 h
        dfTraficParSens6min = recupDonneesTraficBase(
            id_instru_site, sens=sens, voie=voie, indicateur=["TV"], periodeAgreg=["6 min"]
        )
        dfPcPLParSens1h = recupPourcentagePL(id_instru_site, voie=voie)
        # jointure et calcul des attributs
        dfMerge6min1h = dfTraficParSens6min.assign(
            jour=dfTraficParSens6min.date_heure.dt.dayofyear,
            heure=dfTraficParSens6min.date_heure.dt.hour,
        ).merge(
            dfPcPLParSens1h.assign(
                jour=dfPcPLParSens1h.date_heure.dt.dayofyear,
                heure=dfPcPLParSens1h.date_heure.dt.hour,
            ).drop(["id_instru_site", "date_heure"], axis=1, errors="ignore"),
            on=["jour", "heure", "sens", "voie"],
        )
        dfNbVLPL6min = (
            dfMerge6min1h.assign(
                VL=dfMerge6min1h.valeur * (1 - (dfMerge6min1h.pc_pl / 100)),
                PL=dfMerge6min1h.valeur * dfMerge6min1h.pc_pl / 100,
            )
            .melt(
                id_vars=["date_heure", "id_instru_site", "jour", "voie", "sens"],
                value_vars=["VL", "PL"],
                var_name="indicateur",
                value_name="valeurs",
            )
            .rename(columns={"valeurs": "valeur"})
        )
    # pour la D936
    if id_instru_site == 6:
        dfNbVLPL6min = recupDonneesTraficBase(id_instru_site, sens=sens, voie=voie, indicateur=['VL', 'PL'], periodeAgreg='6 min')
        
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
    if (dfNbVLPL6min.voie == "section courante").any() and (
        dfNbVLPL6min.voie != "section courante"
    ).any():
        dfNbVLPL6min = dfNbVLPL6min.loc[dfNbVLPL6min.voie != "section courante"]
    if id_instru_site in (6, 7, 8):
        dfNbVLPL6min2sens = (
            dfNbVLPL6min.groupby(["date_heure", "id_instru_site", "indicateur"])
            .valeur.sum()
            .reset_index()
            .assign(voie="section courante")
        )
    elif id_instru_site in (9,):
        dfNbVLPL6min2sens = (
            dfNbVLPL6min.groupby(["date_heure", "id_instru_site", "indicateur", "voie"])
            .valeur.sum()
            .reset_index()
        )
    else:
        raise NotImplementedError(
            f"le site {id_instru_site} n'est pas implémenté dans la code"
        )
    return dfNbVLPL6min2sens


def recupVitesseTV6min(id_instru_site, sens=None, voie=None):
    """
    telechager les donnees de vitesse tout véhicule sur la période 6 minutes
    in :
        id_instru_site : integer parmi les valeurs de la bdd
        sens: listde string
        voie : list de string parmi les valeurs possible de la bdd
    """
    dicoParams = {"sens": sens, "voie": voie}
    for k, v in dicoParams.items():
        if v and not (isinstance(v, list)):
            raise TypeError(f"{k} doit etre de type list")
    if not voie and not sens:
        rqt = f"""SELECT *
                  FROM mesures_physiques.trafic
                  WHERE id_instru_site = {id_instru_site} and indicateur = 'Vmoy_TV' and periode_agreg = '6 min'"""
    elif voie and sens:
        rqt = f"""SELECT *
                  FROM mesures_physiques.trafic
                  WHERE id_instru_site = {id_instru_site} and sens = any(ARRAY{sens}) and voie = any(ARRAY{voie})  and indicateur = 'Vmoy_TV'
                       and periode_agreg = '6 min'"""
    elif sens and not voie:
        rqt = f"""SELECT *
                  FROM mesures_physiques.trafic
                  WHERE id_instru_site = {id_instru_site} and sens = any(ARRAY{sens}) and indicateur = 'Vmoy_TV' and periode_agreg = '6 min'"""
    elif not sens and voie:
        rqt = f"""SELECT *
                  FROM mesures_physiques.trafic
                  WHERE id_instru_site = {id_instru_site} and voie = any(ARRAY{voie}) and indicateur = 'Vmoy_TV' and periode_agreg = '6 min'"""
    else:
        raise NotImplementedError(
            f"la combinaison id_instru_site, voie, sensn'est pas implémentée"
        )
    with ConnexionBdd(bdd) as c:
        df = pd.read_sql(rqt, c.sqlAlchemyConn)
    return df


def testTraficInDicoJourKO(id_instru_site, sens, voie, date, dicoJourSensVoieKO):
    """
    tester la présence d'uien date dans le dico des jours KO
    """
    try:
        listeDateKo = dicoJourSensVoieKO[id_instru_site][sens][voie]
        if date in listeDateKo:
            return True
        else:
            return False
    except KeyError:
        return False


def affecterVitesseInDico(
    id_instru_site, sens, date_heure, dfVts6min, dicoJourSensVoieKO
):
    """
    regrader si un moment est identifié comme faisant partie des données à pb
     in :
        id_instru_site : integer : parmi les valeurs possible de la Bdd table mesures_physiques.instrumentation site
        sens : varchar : parmi les valeur possibem de la bdd, table mesures_physiques.enum_trafic_sens
        date : string de date au format YYYY-MM-DD
        voie : varchar : parmi les valeur possibem de la bdd, table mesures_physiques.enum_trafic_voie
        dfDateSimple : datafarme des moment de mesure par pas de 6 minutes
        dicoJourSensVoieKO : dico issu de la vérif manuelle des donnée de trafic. de forme {id_instru_site: {sens: {voie: listeDesJoursKO}}
        dfVts6min : la df sur laquelle on applique la fonction
    """
    date = date_heure.strftime("%Y-%m-%d")
    if id_instru_site == 7:  # dans ce cas là on dispose de données par voie et le dico est rempli avec des jours bizarres
        vitesseVoieMediane = dfVts6min.loc[
            (dfVts6min.id_instru_site == id_instru_site)
            & (dfVts6min.sens == sens)
            & (dfVts6min.voie == "voie médiane")
            & (dfVts6min.date_heure == date_heure),
            "valeur",
        ].values[0]
        vitesseVoieLente = dfVts6min.loc[
            (dfVts6min.id_instru_site == id_instru_site)
            & (dfVts6min.sens == sens)
            & (dfVts6min.voie == "voie lente")
            & (dfVts6min.date_heure == date_heure),
            "valeur",
        ].values[0]
        vitesseVoieRapide = dfVts6min.loc[
            (dfVts6min.id_instru_site == id_instru_site)
            & (dfVts6min.sens == sens)
            & (dfVts6min.voie == "voie rapide")
            & (dfVts6min.date_heure == date_heure),
            "valeur",
        ].values[0]
        # vitesse PL
        voie = "voie lente"
        if (
            testTraficInDicoJourKO(id_instru_site, sens, voie, date, dicoJourSensVoieKO)
            or vitesseVoieLente == 0
        ):
            if (
                testTraficInDicoJourKO(
                    id_instru_site, sens, "voie médiane", date, dicoJourSensVoieKO
                )
                or vitesseVoieMediane == 0
            ):
                if (
                    testTraficInDicoJourKO(
                        id_instru_site, sens, "voie rapide", date, dicoJourSensVoieKO
                    )
                    or vitesseVoieRapide == 0
                ):
                    return None
                else:
                    vitessePL = vitesseVoieRapide
            else:
                vitessePL = vitesseVoieMediane
        else:
            vitessePL = vitesseVoieLente
        # vitesse VL
        voie = "voie médiane"
        if (
            testTraficInDicoJourKO(id_instru_site, sens, voie, date, dicoJourSensVoieKO)
            or vitesseVoieMediane == 0
        ):
            if (
                testTraficInDicoJourKO(
                    id_instru_site, sens, "voie lente", date, dicoJourSensVoieKO
                )
                or vitesseVoieLente == 0
            ):
                if (
                    testTraficInDicoJourKO(
                        id_instru_site, sens, "voie rapide", date, dicoJourSensVoieKO
                    )
                    or vitesseVoieRapide == 0
                ):
                    return None
                else:
                    vitesseVL = vitesseVoieRapide
            else:
                vitesseVL = vitesseVoieLente
        else:
            vitesseVL = vitesseVoieMediane
    elif id_instru_site == 8:  # dans ce cas là on ne dispose que de données par section courante, et les données de vitesse sont cohérentes
        vitesseVL = dfVts6min.loc[
            (dfVts6min.id_instru_site == id_instru_site)
            & (dfVts6min.sens == sens)
            & (dfVts6min.voie == "section courante")
            & (dfVts6min.date_heure == date_heure),
            "valeur",
        ].values[0]
        vitessePL = dfVts6min.loc[
            (dfVts6min.id_instru_site == id_instru_site)
            & (dfVts6min.sens == sens)
            & (dfVts6min.voie == "section courante")
            & (dfVts6min.date_heure == date_heure),
            "valeur",
        ].values[0]*0.95 
    return vitessePL, vitesseVL


def recupVitesseVlPl6minParSens(dfVts6min):
    """
    sortir la df des vitesses par 6 minutes pour chaque catégorie de véhicule
    in :
        dfVts6min : issu de recupVitesseTV6min()
    """
    checkAttributsinDf(dfVts6min, ["date_heure", "id_instru_site", "sens"])
    dfDateSimple = dfVts6min.drop_duplicates(
        ["date_heure", "id_instru_site", "sens"]
    ).drop(
        ["indicateur", "valeur", "periode_agreg", "testVitesse"],
        axis=1,
        errors="ignore",
    )
    dfDateSimple[["vitessePL", "vitesseVL"]] = dfDateSimple.apply(
        lambda x: pd.Series(
            affecterVitesseInDico(
                x.id_instru_site, x.sens, x.date_heure, dfVts6min, dicoJourSensVoieKO
            ),
            index=["vitessePL", "vitesseVL"],
            dtype="float64",
        ),
        axis=1,
    )
    return dfDateSimple


def recupTraficEtVitesse6MinParSens(id_instru_site, voie):
    """
    enchainer les fonctions précédentes pour obtenir une df des vitesses VL et PL et volumes
    VL et PL par sens
    in :
        id_instru_site : integer : parmi les valeurs possible de la Bdd table mesures_physiques.instrumentation site
        voie : list de string des 
    out :
        df6MinParSens : dataframe avec pour attributs date_heure', 'id_instru_site', 'sens', 'indicateur', 'valeur',
                        'heure_minute', 'jour', 'jour_sort'
    """
    if id_instru_site in (7, 8):
        dfNbVLPL6min = recupVlPl6min(id_instru_site, voie=voie)
        dfVts6min = recupVitesseVlPl6minParSens(recupVitesseTV6min(id_instru_site))
        dfVts6min = dfVts6min.loc[dfVts6min.voie.isin(voie)]
        df6MinParSens = pd.concat(
            [
                dfVts6min.melt(
                    id_vars=["date_heure", "id_instru_site", "sens", "voie"],
                    value_vars=["vitessePL", "vitesseVL"],
                    value_name="valeur",
                    var_name="indicateur",
                ),
                dfNbVLPL6min.groupby(["id_instru_site", "date_heure", "sens", "indicateur", "voie"])
                .valeur.sum()
                .reset_index(),
            ]
        )
    elif id_instru_site == 6:
        df6MinParSens = recupDonneesTraficBase(id_instru_site, voie=['section courante',], 
                                               indicateur=['Vmoy_VL', 'Vmoy_PL', 'VL', 'PL'], 
                                               periodeAgreg=['6 min']).replace({'Vmoy_VL': "vitesseVL", 'Vmoy_PL': "vitessePL"})
    df6MinParSens = df6MinParSens.assign(
        heure_minute=df6MinParSens.date_heure.apply(lambda x: f"{x.strftime('%H:%M')}"),
        jour=df6MinParSens.date_heure.apply(lambda x: x.strftime("%A %d %B %Y")),
        jour_sort=df6MinParSens.date_heure.dt.dayofyear,
    )
    return df6MinParSens


def calculStatTVParSens(df):
    """
    à partird'une df de traficsur plusieurs jours comprenant les attributs heure_minute, sens et valeur
    assigner les attributs de moyenne, mediane, ecart type et variation de 30 pourcent
    in : 
        df : dataframe de trafic sur plusieurs jour
    """
    checkAttributsinDf(df, ['heure_minute', 'sens', 'valeur'])
    dfStats6MinParSens = (df
                      .groupby(['heure_minute', 'sens'])
                      .valeur.agg([np.mean, np.median, np.std])
                      .reset_index())
    dfStats6MinParSens = dfStats6MinParSens.assign(mean_moins_std=dfStats6MinParSens['mean']-dfStats6MinParSens['std'],
                                               mean_plus_std=dfStats6MinParSens['mean']+dfStats6MinParSens['std'],
                                               mean_moins_30pc=dfStats6MinParSens['mean']*0.7,
                                               mean_plus_30pc=dfStats6MinParSens['mean']*1.3)
    return dfStats6MinParSens


def calculStatVitesseParSens(df):
    """
    à partird'une df de traficsur plusieurs jours comprenant les attributs heure_minute, sens et valeur
    assigner les attributs de moyenne, mediane, ecart type et variation de 30 pourcent
    in : 
        df : dataframe de trafic sur plusieurs jour
    """
    checkAttributsinDf(df, ['heure_minute', 'sens', 'valeur', 'indicateur'])
    dfStats6MinParSens = (df
                      .groupby(['heure_minute', 'sens', 'indicateur'])
                      .valeur.agg([np.mean, np.median, np.std])
                      .reset_index())
    dfStats6MinParSens = dfStats6MinParSens.assign(mean_moins_std=dfStats6MinParSens['mean']-dfStats6MinParSens['std'],
                                               mean_plus_std=dfStats6MinParSens['mean']+dfStats6MinParSens['std'],
                                               mean_plus_10kmh=dfStats6MinParSens['mean']+10,
                                               mean_moins_10kmh=dfStats6MinParSens['mean']-10)
    return dfStats6MinParSens


def calculEmissionTheorique(df, sens1, decliviteVoieSens1, decliviteVoieSens2, ageRvtSens1, ageRvtSens2, revetement, allure):
    """
    calcul de l'émission d'une voie. la df doit etre classée par sens avec un attribut 'sens'
    in : 
        df : df avec des attribut 'sens, vitesseVL, vitessePL, VL, PL
        sens1 : string : valeur d'attrinut sens pour un des deux sens
        decliviteVoieSens1 : integer : declivité de la voie pour le sens 1 
        decliviteVoieSens2 : integer : declivité de la voie pour le sens 2
        ageRvtSens1 : integer : age du revt pour le sens 1
        ageRvtSens2 : integer : age du revt pour le sens 2
        revetement : string pari r1, r2, r3 concerne les deux sens
        allure : string parmi 's', 'a', 'd', concerne les deux sens    
    """
    checkAttributsinDf(df, ['indicateur', 'sens'])
    checkValuesInAttribut(df, 'indicateur', 'vitesseVL','vitessePL', 'VL', 'PL')
    df6MinParSens = df.copy()
    dfEmissionTheorique = df6MinParSens.pivot(index=['date_heure', 'sens', 'heure_minute', 'jour', 'jour_sort'], 
                                              columns='indicateur', values='valeur').reset_index()
    dfEmissionTheorique['declivite'] = dfEmissionTheorique.apply(lambda x: decliviteVoieSens1 if x.sens == sens1 else decliviteVoieSens2, axis=1)
    dfEmissionTheorique['age_rvt'] = dfEmissionTheorique.apply(lambda x: ageRvtSens1 if x.sens == sens1 else ageRvtSens2, axis=1)
    dfEmissionTheorique['emission_bruit'] = dfEmissionTheorique.apply(
        lambda x : Route(x.VL, x.PL, x.vitesseVL, x.vitessePL, revetement, x.age_rvt, allure, x.declivite, ignoreErreurVts=True).lwm, axis=1)
    return dfEmissionTheorique


def graphTraficVitesseEmission2SensSepares(
    dfChartEmission, titre, largeur=1000, hauteur=300, domaintraficMax=600, domainVitesseMax=130, jour=None, domainBruit=(55,90),
    listSens=('sens inter', 'sens exter'), emission_bruit=True
):
    """
    Creer un graph des volume des vl et pl et vitesses vl et pl, à partir des donnée 6 minutes
    in :
        dfChartEmission : df issue de recupTraficEtVitesse6MinParSens(), avec ajout du niveau d'émission de bruit théorique
        largeur : integer : largeur de la Chart
        domaintraficMax : integer : valeur max de l'axe Y des trafics
        domainVitesseMax : integer : valeur max de l'axe Y des vitesses
        domainBruit : tuple de 2 integer : valeur max et min du niveaudémission théorique
        jour : integer ou None : nombre décrivant le jour de l'année. Si None, alors le graph propose un slider
        emission_bruit : boolean qui traduit si l'emission bruit doit etre ajoutee au graph ou non
    """
    checkAttributsinDf(dfChartEmission, ['sens', 'jour_sort', 'indicateur', 'date_heure', 'valeur' ])
    checkValuesInAttribut(dfChartEmission, 'sens', *listSens)
    baseInt = (
        alt.Chart(
            dfChartEmission.loc[(dfChartEmission.jour_sort.notna())],
            title=[
                f"Volume, vitesse de trafics et émission sonore ; {listSens[0]}",
                titre,
            ],
        )
        .encode(
            x=alt.X("hoursminutes(date_heure):T", title="Heure"),
        )
        .transform_filter(f"datum.sens == '{listSens[0]}'")
    )
    nbVeh2SensInt = (
        baseInt.mark_bar(size=4, opacity=0.75)
        .encode(
            y=alt.Y(
                "valeur",
                title="Nombre de véhicules",
                scale=alt.Scale(domainMax=domaintraficMax),
            ),
            color=alt.Color("indicateur", title="Indicateur")
        )
        .transform_filter(
            alt.FieldOneOfPredicate(field="indicateur", oneOf=["VL", "PL"])
        )
    )
    vts2SensInt = (
        baseInt.mark_line()
        .encode(
            y=alt.Y(
                "valeur",
                title="Vitesse moyenne",
                scale=alt.Scale(domainMax=domainVitesseMax),
                axis=alt.Axis(titleAlign='right', titleAnchor='start', labelOverlap=True, labelOffset=-3)
            ),
            size=alt.Size(
                "indicateur",
                scale=alt.Scale(rangeMin=2),
                sort="descending",
                legend=None,
            ),
            color=alt.Color("indicateur", title="Indicateur")
        )
        .transform_filter(
            alt.FieldOneOfPredicate(
                field="indicateur", oneOf=["vitesseVL", "vitessePL"]
            )
        )
    )   
    baseExt = (
        alt.Chart(
            dfChartEmission.loc[(dfChartEmission.jour_sort.notna())],
            title=[
                f"Volume, vitesse de trafics et émission sonore ; {listSens[1]}",
                titre,
            ],
        )
        .encode(
            x=alt.X("hoursminutes(date_heure):T", title="Heure"),
        )
        .transform_filter(f"datum.sens == '{listSens[1]}'")
    )
    nbVeh2SensExt = (
        baseExt.mark_bar(size=4, opacity=0.75)
        .encode(
            y=alt.Y(
                "valeur",
                title="Nombre de véhicules",
                scale=alt.Scale(domainMax=domaintraficMax),
            ),
            color=alt.Color("indicateur", title="Indicateur")
        )
        .transform_filter(
            alt.FieldOneOfPredicate(field="indicateur", oneOf=["VL", "PL"])
        )
    )
    vts2SensExt = (
        baseExt.mark_line()
        .encode(
            y=alt.Y(
                "valeur",
                title="Vitesse moyenne",
                scale=alt.Scale(domainMax=domainVitesseMax),
                axis=alt.Axis(titleAlign='right', titleAnchor='start', labelOverlap=True, labelOffset=-3)
            ),
            size=alt.Size(
                "indicateur",
                scale=alt.Scale(rangeMin=2),
                sort="descending",
                legend=None,
            ),
            color=alt.Color("indicateur", title="Indicateur")
        )
        .transform_filter(
            alt.FieldOneOfPredicate(
                field="indicateur", oneOf=["vitesseVL", "vitessePL"]
            )
        )
    )
    if emission_bruit:
        checkValuesInAttribut(dfChartEmission, 'indicateur', 'emission_bruit')
        BruitExt = baseExt.mark_line(color='red', size=4).encode(
            y=alt.Y(
                    "valeur",
                    title="Émission de bruit",
                    scale=alt.Scale(domain=domainBruit),
                    axis=alt.Axis(labelColor='red', tickColor='red', titleColor='Red', titleAlign='left', titleAnchor='end', labelOverlap=True, labelOffset=3)
                )).transform_filter(f"datum.indicateur == 'emission_bruit'")
        BruitInt = baseInt.mark_line(color='red', size=4).encode(
        y=alt.Y(
                "valeur",
                title="Émission de bruit",
                scale=alt.Scale(domain=domainBruit),
                axis=alt.Axis(labelColor='red', tickColor='red', titleColor='Red', titleAlign='left', titleAnchor='end', labelOverlap=True, labelOffset=3)
            )).transform_filter(f"datum.indicateur == 'emission_bruit'")
        chartExt = (
            (nbVeh2SensExt + vts2SensExt + BruitExt)
            .resolve_scale(y="independent", color='independent')
            .properties(width=largeur, height=hauteur)
        )
        chartInt = (
            (nbVeh2SensInt + vts2SensInt + BruitInt)
            .resolve_scale(y="independent", color='independent')
            .properties(width=largeur, height=hauteur)
        ) 
    else:
        chartExt = (
            (nbVeh2SensExt + vts2SensExt)
            .resolve_scale(y="independent", color='independent')
            .properties(width=largeur, height=hauteur)
        )
        chartInt = (
            (nbVeh2SensInt + vts2SensInt)
            .resolve_scale(y="independent", color='independent')
            .properties(width=largeur, height=hauteur)
        )
    if jour:
        titre = alt.TitleParams(f"Trafics et émission de bruit le {dateTexteDepuisDayOfYear(jour, 2022)}")
        return alt.vconcat(
            chartExt.transform_filter(f"datum.jour_sort == {jour}"),
            chartInt.transform_filter(f"datum.jour_sort == {jour}"),
        ).properties(title=titre)
    else:
        slider = alt.binding_range(min=80, max=109, step=1)
        select_day = alt.selection_single(
            name="jour_sort", fields=["jour_sort"], bind=slider, init={"jour_sort": 80}
        )
        return alt.vconcat(
            chartExt.add_selection(select_day).transform_filter(select_day),
            chartInt.add_selection(select_day).transform_filter(select_day),
        )
        
        
def graphTVTousJours(df, titre, largeur=900, hauteur=200, domaintraficMax=650):
    """
    création d'un graph sur 2 sens (superposition "l'un au dessus de l'autre" avec 1 ligne par jour.
    le graph est cliquable pour masquer / afficher des éléments
    in : 
        df : df des données 6min de base. doit contenuir les attributs heure_minute, valeur, sens et jour
        titre : string ou list de string : titre du graph
        largeur : integer  : largeur du graph
        hauteur : integer : hauteur de chaque composant du graph
        domaintraficMax : integer : val max de l'axe Y
    """   
    checkAttributsinDf(df, ['heure_minute', 'valeur', 'sens', 'jour'])
    # préparation pour rendre le graph interactif
    selection = alt.selection_multi(fields=['jour'])
    color = alt.condition(selection,
                          alt.Color('jour:N', legend=None),
                          alt.value('lightgray'))
    opacity = alt.condition(selection, alt.value(1.0), alt.value(0.2))
    # paramètre et graph
    titreTraficTv = alt.TitleParams(titre,
                                anchor='middle', fontSize=16)
    chartTraficTV = alt.Chart(df, width=largeur, height=hauteur).mark_line().encode(
        x=alt.X('heure_minute', axis=alt.Axis(labelOverlap=True)),
        y=alt.Y('valeur', title='Nombre de véhicules (tous types)', scale=alt.Scale(domainMax=domaintraficMax)),
        color=color,
        opacity=opacity).facet(row='sens:N', title=titreTraficTv)
    legend = alt.Chart(df).mark_point().encode(
        y=alt.Y('jour:N', axis=alt.Axis(orient='right')),
        color=color
    ).add_selection(
        selection
    )
    chartTousJoursOuvres = (chartTraficTV | legend)   
    return chartTousJoursOuvres
    

def graphStatTVTousJours(df, titre, largeur=1000, listSens=('sens inter', 'sens exter')):
    """
    graph représentant tous les jousr de la df par uneligne, avec en plus la moyenne, 
    une zone pour l'écart type et la variation à 30%
    in : 
        df : dataframe de trafic sur plusieurs jour
        titre : string ou list de string : titre du graph
        largeur : integer : largeur du graph
        listSens : list ou tuple destring : les valeur que peut prendre l'attriut sens
    """
    checkAttributsinDf(df, ['heure_minute', 'sens', 'valeur', 'jour'])
    checkValuesInAttribut(df, 'sens', *listSens)
    dfStats6MinParSens = calculStatTVParSens(df)
    baseChartStat = alt.Chart(dfStats6MinParSens, width=largeur).encode(
                         x=alt.X('heure_minute', axis=alt.Axis(labelOverlap=True)))
    chartMeanStat = baseChartStat.mark_line(color='black').encode(
                         y=alt.Y('mean')) # .transform_filter("datum.sens == 'sens inter'")
    chartStdStat = alt.Chart(pd.concat([dfStats6MinParSens[['heure_minute', 'sens', 'mean_moins_std', 'mean_plus_std']]
                                  .rename(columns={'mean_moins_std': 'moins', 'mean_plus_std': 'plus'}).assign(variation='1 écart type'),
                              dfStats6MinParSens[['heure_minute', 'sens', 'mean_moins_30pc', 'mean_plus_30pc']]
                                  .rename(columns={'mean_moins_30pc': 'moins', 'mean_plus_30pc': 'plus'}).assign(variation='30 pourcent')]),
                              width=largeur).mark_area(opacity=0.2).encode(
                        x=alt.X('heure_minute', axis=alt.Axis(labelOverlap=True)),
                        y='moins',
                        y2='plus',
                        color=alt.Color('variation', scale=alt.Scale(range=['green', 'red']), sort=alt.Sort(['30 pourcent']))) # .transform_filter("datum.sens == 'sens inter'")
    
    chartStat = alt.vconcat()
    for s in listSens:
        titreTrafic = titre + f" ; {s}"
        titreTraficTv = alt.TitleParams(titreTrafic,
                                        subtitle=f"Moyenne, évolution selon un écart type ou 30 %", subtitleFontSize=14,
                                        anchor='middle', fontSize=16)
        chartStat &= (alt.Chart(df, width=largeur, title=titreTraficTv).mark_line().encode(
                    x=alt.X('heure_minute', axis=alt.Axis(labelOverlap=True)),
                    y=alt.Y('valeur', title='Nombre de véhicules (tous types)'),
                    color=alt.Color('jour', scale=alt.Scale(range=['#B3B3B3',]), legend=None)).transform_filter(f"datum.sens == '{s}'") 
        + chartMeanStat.transform_filter(f"datum.sens == '{s}'") + chartStdStat.transform_filter(f"datum.sens == '{s}'")).resolve_scale(color='independent')
    return chartStat


def graphVitessesTousJours(df, titre, largeur=500, hauteur=150, domainVitesseMax=110):
    """
    création d'un graph sur 2 sens (superposition "l'un au dessus de l'autre" avec 1 ligne par jour.
    le graph est cliquable pour masquer / afficher des éléments
    Ne concerne que les vitesses
    in : 
        df : df des données 6min de base. doit contenuir les attributs heure_minute, valeur, sens et jour
        titre : string ou list de string : titre du graph
        largeur : integer  : largeur du graph
        hauteur : integer : hauteur de chaque composant du graph
        domaintraficMax : integer : val max de l'axe Y
    """   
    checkAttributsinDf(df, ['heure_minute', 'indicateur', 'valeur', 'sens', 'jour'])
    # préparation pour rendre le graph interactif
    selection = alt.selection_multi(fields=['jour', 'indicateur'])
    color = alt.condition(selection,
                          alt.Color('jour:N', legend=None),
                          alt.value('lightgray'))
    opacity = alt.condition(selection, alt.value(1.0), alt.value(0.2))
    # paramètre et graph
    titreVitesses = alt.TitleParams(titre,
                                anchor='middle', fontSize=16)
    chartVitesses = alt.Chart(df, width=largeur, height=hauteur).mark_line().encode(
        x=alt.X('heure_minute', axis=alt.Axis(labelOverlap=True)),
        y=alt.Y('valeur', title='Vitesse de véhicules', scale=alt.Scale(domainMax=domainVitesseMax)),
        color=color,
        opacity=opacity).facet(row='sens:N', column='indicateur:N', title=titreVitesses)
    legend = alt.Chart(df).mark_rect().encode(
        x='indicateur',
        y=alt.Y('jour:N', axis=alt.Axis(orient='right')),
        color=color
    ).add_selection(
        selection
    )
    chartTousJoursOuvres = (chartVitesses | legend)   
    return chartTousJoursOuvres


def graphStatVitesseTousJours(df, titre, largeur=1000, listSens=('sens inter', 'sens exter')):
    """
    graph représentant tous les jousr de la df par uneligne, avec en plus la moyenne, 
    une zone pour l'écart type et la variation à 30%
    in : 
        df : dataframe de trafic sur plusieurs jour
        titre : string ou list de string : titre du graph
        largeur : integer : largeur du graph
        listSens : list ou tuple destring : les valeur que peut prendre l'attriut sens
    """
    checkAttributsinDf(df, ['heure_minute', 'indicateur', 'valeur', 'sens', 'jour'])
    dfStats6MinParSens = calculStatVitesseParSens(df)
    baseChartStat = alt.Chart(dfStats6MinParSens, width=largeur).encode(
                         x=alt.X('heure_minute', axis=alt.Axis(labelOverlap=True)))
    chartMeanStat = baseChartStat.mark_line(color='black').encode(
                         y=alt.Y('mean')) # .transform_filter("datum.sens == 'sens inter'")
    chartStdStat = alt.Chart(pd.concat([dfStats6MinParSens[['heure_minute', 'sens', 'indicateur', 'mean_moins_std', 'mean_plus_std']]
                                  .rename(columns={'mean_moins_std': 'moins', 'mean_plus_std': 'plus'}).assign(variation='1 écart type'),
                              dfStats6MinParSens[['heure_minute', 'sens', 'indicateur', 'mean_plus_10kmh', 'mean_moins_10kmh']]
                                  .rename(columns={'mean_moins_10kmh': 'moins', 'mean_plus_10kmh': 'plus'}).assign(variation='10 km.h')]),
                              width=largeur).mark_area(opacity=0.2).encode(
                        x=alt.X('heure_minute', axis=alt.Axis(labelOverlap=True)),
                        y='moins',
                        y2='plus',
                        color=alt.Color('variation', scale=alt.Scale(range=['green', 'red']), sort=alt.Sort(['10 km.h', '1 écart type']))) # .transform_filter("datum.sens == 'sens inter'")
    
    listChartStatH = []
    listChartStatV = []
    for s in ('sens exter', 'sens inter'):
        for i in dfStats6MinParSens.indicateur.unique():
            titreTraficTv = alt.TitleParams(f"{i} ; {s}",
                                            subtitleFontSize=14,
                                            anchor='middle', fontSize=10, fontWeight='normal')
            chart = (alt.Chart(df, width=largeur, title=titreTraficTv).mark_line().encode(
                        x=alt.X('heure_minute', axis=alt.Axis(labelOverlap=True)),
                        y=alt.Y('valeur', title='Vitesse des véhicules'),
                        color=alt.Color('jour', scale=alt.Scale(range=['#B3B3B3',]), legend=None)).transform_filter(f"datum.sens == '{s}' & datum.indicateur == '{i}'") 
            + chartMeanStat.transform_filter(f"datum.sens == '{s}' & datum.indicateur == '{i}'") 
                          + chartStdStat.transform_filter(f"datum.sens == '{s}' & datum.indicateur == '{i}'")).resolve_scale(color='independent')
            listChartStatH.append(chart)
        listChartStatV.append(listChartStatH)
        listChartStatH=[]
    return alt.vconcat(*[alt.hconcat(*c) for c in listChartStatV]).properties(
        title=alt.TitleParams(titre, anchor='middle', fontSize=16))
    
    
def graphEmissionBruitSensSepares(dfChartEmission, largeur=600, hauteur=300, domainBruit=(60, 90), jour=None):
    """
    graph des émissions calculés selon les donnée de trafic et de voierie
    in : 
        dfChartEmission : df avec les attributs 'date_heure', 'heure_minute', 'jour', 'jour_sort', 'indicateur' et la valeur 'emission_bruit dans l'atribut indicateur
        largeur : integer : largeur du graph
        domainBruit : tuple de 2 integer : valeur max et min du niveaudémission théorique
    """  
    # verifs
    checkAttributsinDf(dfChartEmission, ['date_heure', 'heure_minute', 'indicateur', 'valeur', 'sens', 'jour'])
    checkValuesInAttribut(dfChartEmission, 'indicateur', 'emission_bruit')
    # calcul des données 2 sens 
    dfChartEmission2Sens = dfChartEmission.loc[dfChartEmission.indicateur == 'emission_bruit'].groupby(
        ['date_heure', 'heure_minute', 'jour', 'jour_sort', 'indicateur']).valeur.apply(
        lambda x : sommeEnergetique(*x)).reset_index()
    # Chart générales
    chartEmission = (alt.Chart(pd.concat([dfChartEmission.loc[dfChartEmission.indicateur == 'emission_bruit'], 
                                          dfChartEmission2Sens.assign(sens='2 sens')]),
                               width=largeur, height=hauteur)
                         .mark_line()
                         .encode(
                             x=alt.X("hoursminutes(date_heure):T", title="Heure", axis=alt.Axis(labelOverlap=True, labelAngle=45, titleFontSize=12)),
                             y=alt.Y('valeur', scale=alt.Scale(domain=domainBruit), axis=alt.Axis(titleFontSize=12), title="Niveau de bruit (dB)"),
                             color=alt.Color('sens', legend=alt.Legend(labelFontSize=12, titleFontSize=12)),
                             strokeDash='sens'))
    #gestion du cas selon le jour fourni ou non
    if jour:
        titre = alt.TitleParams(f"Niveau d'émission sonore par sens et cumulé le {dateTexteDepuisDayOfYear(jour, 2022)}", fontSize=14)
        return chartEmission.transform_filter(f"datum.jour_sort == '{jour}'").properties(title=titre)
    else:
        titre = alt.TitleParams("Niveau d'émission sonore par sens et cumulé", fontSize=14)
        slider = alt.binding_range(min=80, max=109, step=1)
        select_day = alt.selection_single(
            name="jour_sort", fields=["jour_sort"], bind=slider, init={"jour_sort": 80}
        )
        chartEmission = chartEmission.add_selection(select_day).transform_filter(select_day).properties(title=titre)
    return chartEmission
    
    
def graphEmissionBruitTousJours(dfChartEmission, titre, largeur=1000, hauteur=350, domainBruit=(70, 90)):
    """
    graph de l'émission de bruit théorique double sens à partir du fux de trafic 6 minutes par sens
    in : 
        dfChartEmission : df avec les attributs 'date_heure', 'heure_minute', 'jour', 'jour_sort', 'indicateur' et la valeur 'emission_bruit dans l'atribut indicateur
        titre : string ou list de string : titre du graph
        largeur : integer  : largeur du graph
        hauteur : integer : hauteur de chaque composant du graph
        domainBruit : tuple de 2 integer : valeur max et min du niveaudémission théorique
    """
    checkAttributsinDf(dfChartEmission, ['heure_minute', 'jour','valeur'])
    selection = alt.selection_multi(fields=['jour'])
    color = alt.condition(selection,
                          alt.Color('jour:N', legend=None),
                          alt.value('lightgray'))
    opacity = alt.condition(selection, alt.value(1.0), alt.value(0.2))
    # paramètre et graph
    titreBruit = alt.TitleParams(titre,
                                 anchor='middle', fontSize=16)
    chartBruit = alt.Chart(dfChartEmission, width=largeur, height=hauteur,title=titreBruit).mark_line().encode(
        x=alt.X('heure_minute', axis=alt.Axis(labelOverlap=True, titleFontSize=12)),
        y=alt.Y('valeur', title="Niveau d'émision (dB)", scale=alt.Scale(domain=domainBruit), axis=alt.Axis(labelOverlap=True, titleFontSize=12)),
        color=color,
        opacity=opacity)
    legend = alt.Chart(dfChartEmission).mark_point().encode(
        y=alt.Y('jour:N', axis=alt.Axis(orient='right')),
        color=color
    ).add_selection(
        selection
    )
    return (chartBruit | legend)


def graphDensiteBruitSelonSelection(dfChartEmission, titre, largeur=900, hauteur=200, domainBruit=(70, 90), errorBandType=('stdev'),
                                    cumulative=False):
    """
    graph interactif permettant de voir la répartition des émissions sur une période et de selectionner
    une ous période pour en voir la ditribution
    in : 
        dfChartEmission : df avec les attributs 'date_heure', 'heure_minute', 'jour', 'jour_sort', 'indicateur' et la valeur 'emission_bruit dans l'atribut indicateur
        titre : string ou list de string : titre du graph
        largeur : integer  : largeur du graph
        hauteur : integer : hauteur de chaque composant du graph
        domainBruit : tuple de 2 integer : valeur max et min du niveaudémission théorique
        errorBandType : type de bande d'erreur, cf https://vega.github.io/vega-lite/docs/errorband.html#mark-config
        cumulative : boolean, densité cumulative ou non
    """
    # verif
    checkParamValues(errorBandType, ['stdev', 'ci', 'stderr', 'iqr'])
    checkAttributsinDf(dfChartEmission, ['date_heure', 'jour','valeur'])
    # creer la selection dans la chart
    brush = alt.selection_interval(encodings=['x'], empty='all')
    # chart des niveaux d'émission
    titreBruit = alt.TitleParams(titre, anchor='middle', fontSize=16)
    baseBruit = alt.Chart(dfChartEmission, width=largeur, height=hauteur, title=titreBruit).mark_line(opacity=0.3).encode(
        x=alt.X('hoursminutes(date_heure):T', axis=alt.Axis(labelOverlap=True, titleFontSize=12), title='Heure'),
        y=alt.Y('valeur', title="Niveau d'émision (dB)", scale=alt.Scale(domain=domainBruit), axis=alt.Axis(labelOverlap=True, titleFontSize=12)),
        color=alt.Color('jour:N', legend=alt.Legend(symbolLimit=50, symbolOpacity=1)))
    # gérer le cas spécifique de la selection sur lignes
    background = baseBruit.add_selection(brush).add_selection(brush)
    selected = baseBruit.transform_filter(brush).mark_line().encode(color='jour:N')
    # chart de densite
    titreDensite = alt.TitleParams("Dispersion des niveaux de bruit", subtitle="intervalle à +/- 1 écart type")
    chartDensity = alt.Chart(dfChartEmission, width=largeur, height=hauteur, title=titreDensite).transform_filter(brush).transform_density(
        'valeur',
        as_=['valeur', 'density'], cumulative=cumulative).mark_area().encode(
        x=alt.X('valeur:Q', title="Niveaux de bruit (dB)", scale=alt.Scale(domain=domainBruit)),
        y=alt.Y('density:Q', title='Densité'))
    # chart de bade d'erreur
    chartErrorBand = (alt.Chart(dfChartEmission, width=largeur, height=hauteur, title="Dispersion des niveaux de bruit")
                      .transform_filter(brush)
                      .mark_errorband(extent=errorBandType, color='black', borders=True).encode(
                      x=alt.X('valeur:Q', title="Niveaux de bruit (dB)", scale=alt.Scale(domain=domainBruit))))
    # resultat
    return (background + selected) & (chartDensity + chartErrorBand)
    
    