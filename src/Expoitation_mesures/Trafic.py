# -*- coding: utf-8 -*-
'''
Created on 7 oct. 2022

@author: martin.schoreisz
extraire et traiter les donnees de trafic
'''

import pandas as pd
import altair as alt
from Connexions.Connexion_Transfert import ConnexionBdd
from Import_stockage_donnees.Params import (bdd, enum_period_agreg, enum_indicateur, enum_instru_site, enum_trafic_sens,
                                            enum_trafic_voie, dicoJourSensVoieKO)  # , startDateMesure, endDateMesure
from Outils.Outils import checkParamValues, checkAttributsinDf, dateTexteDepuisDayOfYear



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
        raise NotImplementedError(f"la combinaison id_instru_site, voie, sens n'est pas implémentée")
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
    
    
def recupVitesseTV6min(id_instru_site, sens=None, voie=None):
    """
    telechager les donnees de vitesse tout véhicule sur la période 6 minutes
    in : 
        id_instru_site : integer parmi les valeurs de la bdd
        sens: listde string
        voie : list de string parmi les valeurs possible de la bdd
    """
    dicoParams = {'sens': sens, 'voie': voie}
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
        raise NotImplementedError(f"la combinaison id_instru_site, voie, sensn'est pas implémentée")
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
    
    
def affecterVitesseInDico(id_instru_site, sens, date_heure, dfVts6min, dicoJourSensVoieKO):
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
    date = date_heure.strftime('%Y-%m-%d')
    vitesseVoieMediane = dfVts6min.loc[(dfVts6min.id_instru_site == id_instru_site) & (dfVts6min.sens == sens) &
                                        (dfVts6min.voie == 'voie médiane') & (dfVts6min.date_heure == date_heure), 'valeur'].values[0]
    vitesseVoieLente = dfVts6min.loc[(dfVts6min.id_instru_site == id_instru_site) & (dfVts6min.sens == sens) &
                                     (dfVts6min.voie == 'voie lente') & (dfVts6min.date_heure == date_heure), 'valeur'].values[0]
    vitesseVoieRapide = dfVts6min.loc[(dfVts6min.id_instru_site == id_instru_site) & (dfVts6min.sens == sens) &
                                     (dfVts6min.voie == 'voie rapide') & (dfVts6min.date_heure == date_heure), 'valeur'].values[0]
    # vitesse PL
    voie = 'voie lente'
    if testTraficInDicoJourKO(id_instru_site, sens, voie, date, dicoJourSensVoieKO) or vitesseVoieLente == 0:
        if testTraficInDicoJourKO(id_instru_site, sens, 'voie médiane', date, dicoJourSensVoieKO) or vitesseVoieMediane == 0:
            if testTraficInDicoJourKO(id_instru_site, sens, 'voie rapide', date, dicoJourSensVoieKO) or vitesseVoieRapide == 0:
                return None
            else:
                vitessePL = vitesseVoieRapide
        else:
            vitessePL = vitesseVoieMediane
    else:
        vitessePL = vitesseVoieLente
    # vitesse VL
    voie = 'voie médiane'
    if testTraficInDicoJourKO(id_instru_site, sens, voie, date, dicoJourSensVoieKO) or vitesseVoieMediane == 0:
        if testTraficInDicoJourKO(id_instru_site, sens, 'voie lente', date, dicoJourSensVoieKO) or vitesseVoieLente == 0:
            if testTraficInDicoJourKO(id_instru_site, sens, 'voie rapide', date, dicoJourSensVoieKO) or vitesseVoieRapide == 0:
                return None
            else:
                vitesseVL = vitesseVoieRapide
        else:
            vitesseVL = vitesseVoieLente
    else:
        vitesseVL = vitesseVoieMediane

    return vitessePL, vitesseVL


def recupVitesseVlPl6minParSens(dfVts6min):
    """
    sortir la df des vitesses par 6 minutes pour chaque catégorie de véhicule
    in : 
        dfVts6min : issu de recupVitesseTV6min()
    """
    checkAttributsinDf(dfVts6min, ['date_heure', 'id_instru_site', 'sens'])
    dfDateSimple = dfVts6min.drop_duplicates(['date_heure', 'id_instru_site', 'sens']).drop(
        ['indicateur', 'valeur', 'periode_agreg', 'testVitesse', 'voie'], axis=1, errors='ignore')
    dfDateSimple[['vitessePL', 'vitesseVL']] = dfDateSimple.apply(
        lambda x: pd.Series(affecterVitesseInDico(x.id_instru_site, x.sens, x.date_heure, dfVts6min, dicoJourSensVoieKO),
                            index=['vitessePL', 'vitesseVL'], dtype="float64"), axis=1)
    return dfDateSimple
    
    
def recupTraficEtVitesse6MinParSens(id_instru_site):
    """
    enchainer les fonctions précédentes pour obtenir une df des vitesses VL et PL et volumes 
    VL et PL par sens
    in : 
        id_instru_site : integer : parmi les valeurs possible de la Bdd table mesures_physiques.instrumentation site
    out : 
        df6MinParSens : dataframe avec pour attributs date_heure', 'id_instru_site', 'sens', 'indicateur', 'valeur', 
                        'heure_minute', 'jour', 'jour_sort'
    """
    dfNbVLPL6min = recupVlPl6min(id_instru_site)
    dfVts6min = recupVitesseVlPl6minParSens(recupVitesseTV6min(id_instru_site))
    df6MinParSens = pd.concat([dfVts6min.melt(id_vars=['date_heure', 'id_instru_site', 'sens'], value_vars=['vitessePL', 'vitesseVL'], value_name='valeur', 
                                              var_name='indicateur'),
                               dfNbVLPL6min.groupby(['id_instru_site', 'date_heure', 'sens', 'indicateur']).valeur.sum().reset_index()])
    df6MinParSens = df6MinParSens.assign(heure_minute=df6MinParSens.date_heure.apply(lambda x: f"{x.strftime('%H:%M')}"),
                                                         jour=df6MinParSens.date_heure.apply(lambda x: x.strftime('%A %d %B %Y')),
                                                         jour_sort=df6MinParSens.date_heure.dt.dayofyear)
    return df6MinParSens
    
    
def graphTraficVitesse2SensSepares(df6MinParSens, largeur=1000, domaintraficMax=1200, domainVitesseMax=130, jour=None):
    """
    Creer un graph des volume des vl et pl et vitesses vl et pl, à partir des donnée 6 minutes
    in : 
        df6MinParSens : df issue de recupTraficEtVitesse6MinParSens()
        largeur : integer : largeur de la Chart
        domaintraficMax : integer : valeur max de l'axe Y des trafics
        domainVitesseMax : integer : valeur max de l'axe Y des vitesses
        jour : integer ou None : nombre décrivant le jour de l'année. Si None, alors le graph propose un slider
    """
    baseExt = alt.Chart(df6MinParSens.loc[(df6MinParSens.jour_sort.notna())], title=['Volume et vitesse de trafics ; sens exter', 'Section courante rocade Sud']).encode(
        x=alt.X('hoursminutes(date_heure):T', title='Heure'),
        color=alt.Color('indicateur', title='Indicateur')).transform_filter("datum.sens == 'sens exter'")
    nbVeh2SensExt = baseExt.mark_bar(size=4, opacity=0.75).encode(
        y=alt.Y('valeur', title='Nombre de véhicules', scale=alt.Scale(domainMax=domaintraficMax))).transform_filter(alt.FieldOneOfPredicate(field='indicateur', oneOf=['VL', 'PL']))
    vts2SensExt = baseExt.mark_line().encode(
        y=alt.Y('valeur', title= 'Vitesse moyenne', scale=alt.Scale(domainMax=domainVitesseMax)),
        size=alt.Size('indicateur', scale=alt.Scale(rangeMin=2), sort='descending', legend=None)).transform_filter(alt.FieldOneOfPredicate(field='indicateur', oneOf=['vitesseVL', 'vitessePL']))
    chartExt = (nbVeh2SensExt + vts2SensExt).resolve_scale(y='independent').properties(width=largeur)
    baseInt = alt.Chart(df6MinParSens.loc[(df6MinParSens.jour_sort.notna())], title=['Volume et vitesse de trafics ; sens inter', 'Section courante rocade Sud']).encode(
        x=alt.X('hoursminutes(date_heure):T', title='Heure'),
        color=alt.Color('indicateur', title='Indicateur')).transform_filter("datum.sens == 'sens inter'")
    nbVeh2SensInt = baseInt.mark_bar(size=4, opacity=0.75).encode(
        y=alt.Y('valeur', title='Nombre de véhicules', scale=alt.Scale(domainMax=domaintraficMax))).transform_filter(alt.FieldOneOfPredicate(field='indicateur', oneOf=['VL', 'PL']))
    vts2SensInt = baseInt.mark_line().encode(
        y=alt.Y('valeur', title= 'Vitesse moyenne', scale=alt.Scale(domainMax=domainVitesseMax)),
        size=alt.Size('indicateur', scale=alt.Scale(rangeMin=2), sort='descending', legend=None)).transform_filter(alt.FieldOneOfPredicate(field='indicateur', oneOf=['vitesseVL', 'vitessePL']))
    chartInt = (nbVeh2SensInt + vts2SensInt).resolve_scale(y='independent').properties(width=largeur)
    if jour :
        titre = alt.TitleParams(f"Trafics le {dateTexteDepuisDayOfYear(jour, 2022)}")
        return alt.vconcat(chartExt.transform_filter(f"datum.jour_sort == {jour}"), 
                           chartInt.transform_filter(f"datum.jour_sort == {jour}")).properties(title=titre)
    else : 
        slider = alt.binding_range(min=80, max=109, step=1)
        select_day = alt.selection_single(name="jour_sort", fields=['jour_sort'],
                                   bind=slider, init={'jour_sort': 80})
        return alt.vconcat(chartExt.add_selection(select_day).transform_filter(select_day), 
                           chartInt.add_selection(select_day).transform_filter(select_day))
                           
