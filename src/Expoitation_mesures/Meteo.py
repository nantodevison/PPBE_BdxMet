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
    base = (alt.Chart(dfAnglesOrienteRecepteurs
                  .assign(vitesseVentKmH=dfAnglesOrienteRecepteurs.vts_vent_haut*3.6,
                          homogeneHaut=0.015,
                          homogeneBas=-0.015,
                          jour=dfAnglesOrienteRecepteurs.date_heure.dt.dayofyear,
                          heure_minute=dfAnglesOrienteRecepteurs.date_heure.apply(lambda x: f"{x.strftime('%H:%M')}")))
        .encode(x=alt.X('heure_minute', title='Heure')))
    vent = (base.mark_point(shape="arrow", opacity=1).encode(
        y=alt.Y("vitesseVentKmH:Q", scale=alt.Scale(domain=(-5,30))),
        size=alt.Size('vitesseVentKmH:Q', scale=alt.Scale(rangeMax=4000, rangeMin=-10, domainMin=-1, domainMax=30), legend=None),
        angle=alt.Angle("dir_vent_haut:Q", scale=alt.Scale(domain=[0, 360], range=[180, 540])),
        color=alt.Color("vitesseVentKmH:Q", scale=alt.Scale(scheme='turbo', domainMin=-1, domainMax=30), legend=None))
            .properties(height=hauteur, width=largeur))
    temperature = (base.mark_line(color='Grey').encode(y=alt.Y('temp_haut', title='Temperature', scale=alt.Scale(domainMax=tempMax)))
                   .properties(height=hauteur, width=largeur))
    rayonnement = (base.mark_line(color=couleurRayonnement).encode(
                   y=alt.Y('rayonnement',
                           scale=alt.Scale(domainMax=rayonnementMax),
                           axis=alt.Axis(titleColor=couleurRayonnement)))
                   .properties(height=hauteur, width=largeur))
    pluie = (base.mark_point(color=couleurPluie, size=60, shape='diamond', filled=True).encode(
        y=alt.Y('pluie', 
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
    
    
    
    
    