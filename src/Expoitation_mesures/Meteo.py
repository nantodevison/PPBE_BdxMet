# -*- coding: utf-8 -*-
'''
Created on 30 sept. 2022

@author: martin.schoreisz
exploitation des donnees meteo
'''

import pandas as pd
import altair as alt
from datetime import datetime
from Import_stockage_donnees.Params import bdd  # , startDateMesure, endDateMesure
from Connexion_Transfert import ConnexionBdd
from Outils import checkAttributsinDf
from Bruit.Meteo import correctionVitesseVentMeteoFrance


def recupDonneesMeteoBase():
    """
    telechager les donnees meteo
    """
    with ConnexionBdd(bdd) as c:
        rqt = """SELECT id, date_heure, vts_vent_haut, vit_vent_bas, temp_haut, temp_bas, 
                        dir_vent_haut, dir_vent_bas, hygro_haut, hygro_bas, rayonnement, pluie 
                  FROM mesures_physiques.meteo m"""
        df = pd.read_sql(rqt, c.sqlAlchemyConn)
    return df


def recupDonneesMeteofranceBase():
    """
    telechager les donnees meteo
    """
    with ConnexionBdd(bdd) as c:
        rqt = """SELECT station, date_3heures, dir_vent_moy_10min, vit_vent_moy_10min, temperature_k,
                        hygrometrie, nebulosite, nebulosite_nuage_inferieur, pluie_derniere_heure, 
                        pluie_3_dernieres_heures, pluie_6_dernieres_heures, pluie_12_dernieres_heures, 
                        pluie_24_dernieres_heures, id
                  FROM mesures_physiques.meteo_france;"""
        df = pd.read_sql(rqt, c.sqlAlchemyConn)
    df['vitesseHauteurPerso'] = df.vit_vent_moy_10min.apply(lambda x: correctionVitesseVentMeteoFrance(x, 3))
    return df


def creerGraphMeteo(dfAnglesOrienteRecepteurs, jour=None, hauteur=150, largeur=600, domainPluie=(0, 2), rayonnementMax=850, 
                    tempMax=26, couleurRayonnement='orange', couleurPluie='red', configure=True):
    """
    créer le graph qui permet la visualisation des données météo sur 2 charts en vertical concat.
    in : 
        dfAnglesOrienteRecepteurs: dataframe issue du module météo de Outil.Bruit, classe DonneesMeteo
        jour : integer : dayOfYear du jour considéré. si None alors on met une sélection sur le jour et pas de titre
        hauteur : integer, hauteur de chaque chart
        largeur : integer, largeur des charts
        domainPluie : tuple de 2 integer en min max des valeurs prise par pluie dans la dfAnglesOrienteRecepteurs
        rayonnementMax : integer :valeur pax du rayonnement dans la dfAnglesOrienteRecepteurs
        couleurRayonnement : string, couleur du trait représentant le rayaonnement
        couleurPluie : string, couleur du point représentant la pluie
        configure : booleen : ajoute ou non la config des axes, legende, titre
    """
    checkAttributsinDf(dfAnglesOrienteRecepteurs, ['vts_vent_haut', 'date_heure', 'dir_vent_haut', 'rayonnement', 'pluie'])
    base = (alt.Chart(dfAnglesOrienteRecepteurs
                  .assign(vitesseVentKmH=dfAnglesOrienteRecepteurs.vts_vent_haut*3.6,
                          jour=dfAnglesOrienteRecepteurs.date_heure.dt.dayofyear,
                          heure_minute=dfAnglesOrienteRecepteurs.date_heure.apply(lambda x: f"{x.strftime('%H:%M')}"))))
    vent = (base.mark_point(shape="arrow", opacity=1).encode(
        x=alt.X('heure_minute', title='Heure', axis=alt.Axis(labels=False)),
        y=alt.Y("vitesseVentKmH:Q", scale=alt.Scale(domain=(-5,30)), title='Vitesse du vent (km/h)'),
        size=alt.Size('vitesseVentKmH:Q', scale=alt.Scale(rangeMax=4000, rangeMin=-10, domainMin=-1, domainMax=30), legend=None),
        angle=alt.Angle("dir_vent_haut:Q", scale=alt.Scale(domain=[0, 360], range=[180, 540])),
        color=alt.Color("vitesseVentKmH:Q", scale=alt.Scale(scheme='turbo', domainMin=-1, domainMax=30), legend=None))
            .properties(height=hauteur, width=largeur))
    temperature = (base.mark_line(color='Grey').encode(
                    x=alt.X('heure_minute', title='Heure', axis=alt.Axis(labelOverlap=True)),
                    y=alt.Y('temp_haut', title='Temperature (°C)', scale=alt.Scale(domainMax=tempMax)))
                   .properties(height=hauteur, width=largeur))
    rayonnement = (base.mark_line(color=couleurRayonnement).encode(
                   x=alt.X('heure_minute', title='Heure'),
                   y=alt.Y('rayonnement', title='Rayonnement (W/m²)',
                           scale=alt.Scale(domainMax=rayonnementMax),
                           axis=alt.Axis(titleColor=couleurRayonnement)))
                   .properties(height=hauteur, width=largeur))
    pluie = (base.mark_point(color=couleurPluie, size=60, shape='diamond', filled=True).encode(
        x=alt.X('heure_minute', title='Heure', axis=alt.Axis(labelOverlap=True)),
        y=alt.Y('pluie', title='Pluie (mm)',
                scale=alt.Scale(domain=domainPluie),
                axis=alt.Axis(titleColor=couleurPluie))).transform_filter("datum.pluie > 0")
             .properties(height=hauteur, width=largeur))
    if not jour:
        slider = alt.binding_range(min=80, max=109, step=1)
        select_day = alt.selection_single(name="jour", fields=['jour'],
                                   bind=slider, init={'jour': 80})
        meteoGlobal = (((vent + pluie).resolve_scale(y='independent') & (temperature + rayonnement).resolve_scale(y='independent'))
                       .add_selection(select_day)
                       .transform_filter(select_day))
    else:
        titre = f"Conditions météorologiques le {datetime.strptime('2022' + '-' + str(jour).rjust(3 , '0'), '%Y-%j').strftime('%A %d %B %Y')}"
        meteoGlobal = (((vent + pluie).resolve_scale(y='independent') & 
                        (temperature + rayonnement).resolve_scale(y='independent'))
                        .transform_filter(f'datum.jour == {jour}')
                        .properties(title=titre))
    if configure:
        meteoGlobal =   (meteoGlobal.configure_title(align='center', anchor='middle', fontSize=20)
                             .configure_legend(titleFontSize=13, labelFontSize=12)
                             .configure_axis(labelFontSize=13, titleFontSize=12))
    return meteoGlobal


def concatMeteoFranceEtSite(dfAnglesOrienteRecepteurs, dfMeteoFranceBx):
    """
    fusionner les deux sources de données avant les graphs
    """
    checkAttributsinDf(dfAnglesOrienteRecepteurs, ['date_heure', 'ConditionPropagation', 'dir_vent_haut',
                                                   'vts_vent_haut', 'temp_haut_k', 'grad_moy'])
    checkAttributsinDf(dfMeteoFranceBx, ['date_heure', 'ConditionPropagation', 'dir_vent_moy_10min',
                                         'vitesseHauteurPerso', 'temperature_k'])
    dfMeteoCompRiverain = (pd.concat(
    [dfAnglesOrienteRecepteurs[['date_heure', 'ConditionPropagation', 'dir_vent_haut',
                                'vts_vent_haut', 'temp_haut_k', 'grad_moy']].assign(source='Mesures sur site'),
     dfMeteoFranceBx[['date_heure', 'ConditionPropagation', 'dir_vent_moy_10min',
                      'vitesseHauteurPerso', 'temperature_k']].rename(columns={'dir_vent_moy_10min': 'dir_vent_haut',
                                                                              'vitesseHauteurPerso': 'vts_vent_haut',
                                                                              'temperature_k': 'temp_haut_k'}).assign(
                                                                                  source='MétéoFrance Mérignac')]))
    return dfMeteoCompRiverain


def creerGraphCompConditionsPropa(dfAnglesOrienteRecepteurs, dfMeteoFranceBx, largeur=1500, hauteur=400):
    """
    creer un graph de comparaison des donnees issues de meteoFrance et du matos sur site
    in: 
        dfAnglesOrienteRecepteurs: dataframe issue du module météo de Outil.Bruit, classe DonneesMeteo   
        dfMeteoFranceBx : dataframe issu de MétéoFrance, cf Outil.Bruit recupDonneesMeteofranceBase()
    """
    dfMeteoCompRiverain=concatMeteoFranceEtSite(dfAnglesOrienteRecepteurs, dfMeteoFranceBx
                                                ).replace({'ConditionPropagation': {'favorable': 'defavorable', 'defavorable': 'favorable'}})

    return (alt.Chart(dfMeteoCompRiverain, width=largeur, height=hauteur, title=['Évolution des conditions de popagation du son ; Point de vue riverain'])
             .mark_line(strokeWidth=2)
             .encode(
                 x=alt.X('date_heure:T', title='Jour'),
                 y=alt.Y('ConditionPropagation:N', title='Conditions de propagations', sort=['favorable', 'homogene', 'defavorable']),
                 order=alt.EncodingSortField('date_3heures:T'),
                 color='source:N').configure_title(align='center', anchor='middle', fontSize=20)
                             .configure_legend(titleFontSize=13, labelFontSize=12)
                             .configure_axis(labelFontSize=13, titleFontSize=12))
    

def creerGraphCompVentSiteMeteoFrance(dfMeteoCompRiverain, jour=None, largeur=800, hauteur=150):
    """
    graph des conditions aréodynamiques de propagation, concaténer verticalement avec les données de site et celles 
    de la station Météo de Mérignac
    in :
        dfMeteoCompRiverain : df concatener des donnée des deux sources
    """
    checkAttributsinDf(dfMeteoCompRiverain, ['vts_vent_haut', 'date_heure', 'dir_vent_haut'])
    dfMeteoCompRiverain = dfMeteoCompRiverain.assign(vitesseVentKmH=dfMeteoCompRiverain.vts_vent_haut*3.6,
                                                jour=dfMeteoCompRiverain.date_heure.dt.dayofyear,
                                                heure_minute=dfMeteoCompRiverain.date_heure.apply(lambda x: f"{x.strftime('%H:%M')}"))
    base = (alt.Chart(dfMeteoCompRiverain)
        .mark_point(shape="arrow", opacity=1)
        .encode(x=alt.X('heure_minute', title='Heure'),
                y=alt.Y("vitesseVentKmH:Q"),
                size=alt.Size('vitesseVentKmH:Q', scale=alt.Scale(rangeMax=4000, rangeMin=-10, domainMin=-1, domainMax=30), legend=None),
                angle=alt.Angle("dir_vent_haut:Q", scale=alt.Scale(domain=[0, 360], range=[180, 540])),
                color=alt.Color("vitesseVentKmH:Q", scale=alt.Scale(scheme='turbo', domainMin=-1, domainMax=30), legend=None))
        .properties(height=hauteur, width=largeur))
    if not jour:
        slider = alt.binding_range(min=80, max=109, step=1)
        select_day = alt.selection_single(name="jour", fields=['jour'],
                                          bind=slider, init={'jour': 80})
        chart = ((base.transform_filter(f"(datum.source == 'Mesures sur site')")
                     .properties(title='force et vitesse du vent sur site') & 
                 base.transform_filter(f"(datum.source == 'MétéoFrance Mérignac')")
                     .properties(title='force et vitesse du vent relevés par Météo France (Mérignac)'))
                     .add_selection(select_day)
                     .transform_filter(select_day)
                     .configure_title(align='center', anchor='middle', fontSize=20)
                     .configure_legend(titleFontSize=13, labelFontSize=12)
                     .configure_axis(labelFontSize=13, titleFontSize=12))
    else:
        titre = [f"Conditions aérodynamiques le {datetime.strptime('2022' + '-' + str(jour).rjust(3 , '0'), '%Y-%j').strftime('%A %d %B %Y')}"]
        chart = ((base.transform_filter(f"(datum.source == 'Mesures sur site') & (datum.jour == {jour})")
                     .properties(title='force et vitesse du vent sur site') & 
                 base.transform_filter(f"(datum.source == 'MétéoFrance Mérignac') & (datum.jour == {jour})")
                     .properties(title='force et vitesse du vent relevés par Météo France (Mérignac)'))
                     .properties(title=titre)
                     .configure_title(align='center', anchor='middle', fontSize=20)
                     .configure_legend(titleFontSize=13, labelFontSize=12)
                     .configure_axis(labelFontSize=13, titleFontSize=12))
    return chart
    
    
    