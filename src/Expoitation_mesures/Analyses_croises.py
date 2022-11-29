# -*- coding: utf-8 -*-
'''
Created on 20 oct. 2022

@author: martin.schoreisz
analyses croises des donnees acoustiques, meteo, trafic
'''


import altair as alt
from datetime import datetime
import pandas as pd
from Expoitation_mesures.Meteo import creerGraphMeteo
from Outils import checkAttributsinDf


def graphCroiseBruitMeteo1Jour(dfBruitMeteo, jour, domainBruit, rangeMin, rangeMax, domainMax,
                               largeur, hauteur, configure=True):
    """
    faire un graph croise sur un jour entre les données bruit et les données météo
    in : 
        dfBruitMeteo : df contenant les donnée de météo et de bruit
        jour : integer décrivant le dayofyear,
        dfAnglesOrienteRecepteurs : dataframe issue du module météo de Outil.Bruit, classe DonneesMeteo
        domainBruit : tuple de deux integer : limites hautes et basses des graphs coté bruit
        rangeMin : integer : range mini de la scale de force de propagataion
        rangeMax : integer : range maxi de la scale de force de propagataion
        domainMax : integer : maximum du domaine de valeur de la force de propagation
        largeur : integer : larguer de la Chart
        hauteur : hauteur de chaque Chart
        configure : booleen : ajoute ou non la config des axes, legende, titre
    """
    checkAttributsinDf(dfBruitMeteo, ['ConditionPropagation', 'forcePropagation_num', 'heure_minute', 'leq_a',
                                      'vts_vent_haut', 'date_heure', 'dir_vent_haut', 'rayonnement', 'pluie'])
    titre = ["Niveau de bruit selon les conditions de propagation ;", f"point de vue riverains ; {datetime.strptime('2022' + '-' + str(jour).rjust(3 , '0'), '%Y-%j').strftime('%A %d %B %Y')} ; Rue Jules Ladoumègue"]
    ids = dfBruitMeteo[(dfBruitMeteo['ConditionPropagation'] != dfBruitMeteo['ConditionPropagation'].shift(-1))].index.to_list()[0:-1] # remove the last index
    ar = []
    txtConversionLegende = "if(datum.value >=0 & datum.value<0.2, 'faible', if(datum.value >= 0.2 & datum.value<0.4, 'moyenne', if(datum.value >= 0.4, 'forte', 'NC')))"
    for i in ids:
        temp = dfBruitMeteo.loc[[i, i+1]]  # adjacent indexes where the change happens
        temp['ConditionPropagation'] = dfBruitMeteo.loc[i]['ConditionPropagation']  # change the tag of the last tag to be equal the previous
        ar.append(temp)
    dfJoigningLines = pd.concat(ar)
    # graph des lignes de jointure
    chartjoiningLines = alt.Chart(dfJoigningLines).mark_trail().encode(
        x=alt.X('heure_minute:N', axis=alt.Axis(title='Heure', labels=False)),
        y=alt.Y('leq_a:Q',
                scale=alt.Scale(domain=domainBruit),
                impute=alt.ImputeParams(value=None),
                axis=alt.Axis(title='Niveau de bruit (Leq,a)')),
        size=alt.Size('forcePropagation_num:Q',
                      scale=alt.Scale(rangeMin=rangeMin, rangeMax=rangeMax, domainMax=domainMax),
                      legend=alt.Legend(title=['Force de', 'propagation'], values=[0, 0.2, 0.4], labelExpr=txtConversionLegende)),
        color=alt.Color('ConditionPropagation:N', scale=alt.Scale(domain=['defavorable', 'homogene', 'favorable'],
                        range=['red', 'blue', 'green']), legend=alt.Legend(title=['Conditions de', 'propagation']))
        ).transform_filter(f"datum.jour_sort == {jour}")
    # graphs des lignes de base
    chartSourceLines = alt.Chart(dfBruitMeteo, title=titre).mark_trail().encode(
        x=alt.X('heure_minute:N', axis=alt.Axis(title='Heure')),
        y=alt.Y('leq_a:Q', 
                impute=alt.ImputeParams(value=None),
                scale=alt.Scale(domain=domainBruit),
                axis=alt.Axis(title='Niveau de bruit (Leq,a)')),
        size=alt.Size('forcePropagation_num:Q',
                      scale=alt.Scale(rangeMin=rangeMin, rangeMax=rangeMax, domainMax=domainMax),
                      legend=alt.Legend(title=['Force de', 'propagation'], values=[0, 0.2, 0.4], labelExpr=txtConversionLegende)),  # , 
        color=alt.Color('ConditionPropagation:N', scale=alt.Scale(domain=['defavorable', 'homogene', 'favorable'],
                        range=['red', 'blue', 'green']),
                        legend=alt.Legend(title=['Conditions de', 'propagation']))).transform_filter(f"datum.jour_sort == {jour}")
    chartMeteo = creerGraphMeteo(dfBruitMeteo, jour=jour, configure=False, largeur=largeur)
    if not configure:
        chart = (alt.vconcat(((chartSourceLines + chartjoiningLines)
                              .properties(width=largeur, height=hauteur)), chartMeteo)
                              .resolve_scale(size='independent'))
    else:
        chart = (alt.vconcat(((chartSourceLines + chartjoiningLines)
                              .properties(width=largeur, height=hauteur)), chartMeteo)
                              .resolve_scale(size='independent')
                              .configure_title(align='center', anchor='middle', fontSize=16)
                              .configure_legend(titleFontSize=13, labelFontSize=12)
                              .configure_axis(labelFontSize=13, titleFontSize=12))
    return chart


def graphCroiseBruitMeteoPlusieursJour(dfBruitMeteo, listJours, domainBruit, rangeMin, rangeMax, domainMax, 
                                       largeur, hauteur, configure=True):
    """
    faire un graph croise sur plsueiurs jours entre les données bruit et les données météo. Affichage de X lignes sur 2 colonnes
    in : 
        dfBruitMeteo : df contenant les donnée de météo et de bruit
        listJours : list of integer décrivant le dayofyear,
        domainBruit : tuple de deux integer : limites hautes et basses des graphs coté bruit
        rangeMin : integer : range mini de la scale de force de propagataion
        rangeMax : integer : range maxi de la scale de force de propagataion
        domainMax : integer : maximum du domaine de valeur de la force de propagation
        largeur : integer : larguer de la Chart
        hauteur : hauteur de chaque Chart
        configure : booleen : ajoute ou non la config des axes, legende, titre
    """
    listChartBruitPropa = []
    for d in listJours:
        listChartBruitPropa.append(graphCroiseBruitMeteo1Jour(dfBruitMeteo, d, domainBruit, rangeMin, rangeMax, 
                                                              domainMax, largeur, hauteur, configure=False))
    return alt.vconcat(*[alt.hconcat(listChartBruitPropa[e], listChartBruitPropa[e+1]) for e in range(0, len(listChartBruitPropa), 2)]).configure_title(
                align='center', anchor='middle', fontSize=16).configure_legend(titleFontSize=13, labelFontSize=12).configure_axis(labelFontSize=13, titleFontSize=12)


