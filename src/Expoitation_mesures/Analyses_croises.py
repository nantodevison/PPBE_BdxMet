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
from Outils.Outils import checkAttributsinDf, checkValuesInAttribut
from Expoitation_mesures.Trafic import graphTraficVitesseEmission2SensSepares
from Bruit.Niveaux import sommeEnergetique, niveau2Pression


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


def graphCroiseBruitTrafic6min(df6MinParSens, df6MinBruit, largeur=1000, domaintraficMax=1200, domainVitesseMax=130, jour=None, domainBruit=(45, 68)):
    """
    concatener verticalement les graph de trafic avec les graphsde niveaux de bruit, par pas de 6 minutes
    in : 
        paramètres de Trafic.graphTraficVitesse2SensSepares()
        df6MinBruit : issue de Acoustique.recupDonneesAgregees() avec ajout des colonnes heure_minute, jour, jou_rsort (dayofyear)
        domainBruit : tuple d'integer représe tant les liimites lmin et max en Y du graph bruit
    """
    chartBruit = (alt.Chart(df6MinBruit.loc[(df6MinBruit.jour_sort.notna())], title=['Niveaux de bruit 2 sens confondus', 'Rue Jules Ladoumègue'])
                  .mark_line()
                  .encode(
                      x=alt.X('hoursminutes(date_heure):T', title='Heure'),
                      y=alt.Y('valeur', title='Niveaux de bruit', scale=alt.Scale(domain=(45, 68))))).properties(width=largeur)
    if jour : 
        chartBruit = chartBruit.transform_filter(f"datum.jour_sort == {jour}")
    else:
        slider = alt.binding_range(min=80, max=109, step=1)
        select_day = alt.selection_single(name="jour_sort", fields=['jour_sort'],
                                   bind=slider, init={'jour_sort': 80})
        chartBruit = chartBruit.add_selection(select_day).transform_filter(select_day)
    return alt.vconcat(graphTraficVitesseEmission2SensSepares(df6MinParSens, domaintraficMax=domaintraficMax, domainVitesseMax=domainVitesseMax, 
                                                      largeur=largeur, jour=jour),
                                                      chartBruit)
    
    
def graphCroiseBruitMeteoTrafic(dfBruitTraficMeteo, dfMeteo, sousTitre, jours, largeur=700, hauteur=300,
                                domainBruit = (45,90), domainGradSon = (-0.6, 0.6), domainTemp = (0,25), domainMaxRayonnement = 850):
    """
    graph comprenant 4 parties avec les niveaux mesures et émis, le gradeint de célérité du son, le vent et les paramètres de chaleur, 
    avec une variation sde couleur sur le jour étudié et une variation de type de ligne sur l'indicateur
    in : 
        dfBruitTraficMeteo : conteint les donnée dans l'attributs indicateur et valeur. comprend aussi la desrciption des jour en texte et nombre (jour_sort)
        dfMeteo : df issue de la partie météo avec en plus les références au jour comme la précédente df
        sousTitre : string décrivant le cas étudié
        jours : list ou tuple d'integer décriavnt le jour de l'annee
    """
    # verif
    checkAttributsinDf(dfBruitTraficMeteo, ['indicateur', "valeur", "date_heure", "jour", "jour_sort"])
    checkAttributsinDf(dfMeteo, ['vitesseVentKmH', "dir_vent_haut", "date_heure", "jour", "jour_sort"])
    checkValuesInAttribut(dfBruitTraficMeteo, 'indicateur', 'Mesure de bruit', 'Émission de la route', 'Force de propagation du son', 
                          'Température')
    # params
    filtreJour = alt.FieldOneOfPredicate('jour_sort', jours)
    filtreIndicBruit = alt.FieldOneOfPredicate('indicateur', ['Mesure de bruit', 'Émission de la route'])
    filtreIndicGradient = alt.FieldOneOfPredicate('indicateur', ['Force de propagation du son'])
    titreGlobal=alt.TitleParams("Relations entre niveau d'émission, niveau en façade et météo", fontSize=16, align='center', anchor='middle',
                            subtitle=sousTitre)
    # charts
    base = alt.Chart(dfBruitTraficMeteo).mark_line(size=3).encode(
        x=alt.X('hoursminutes(date_heure):T', axis=alt.Axis(labelOverlap=True, titleFontSize=12), title=None),
        color=alt.Color('jour:N', legend=alt.Legend(labelFontSize=12, titleFontSize=12), title='Jour'),
        strokeDash=alt.StrokeDash('indicateur:N', legend=alt.Legend(labelFontSize=12, titleFontSize=12), title='Indicateur',
                                  sort=alt.Sort(['Mesure de bruit', 'Émission de la route', 'Force de propagation du son']))).transform_filter(filtreJour).properties(width=largeur, height=hauteur)
    chartCompBruitTrafic = base.encode(
        y=alt.Y('valeur:Q', scale=alt.Scale(domain=domainBruit),
                axis=alt.Axis(labelOverlap=True, titleFontSize=12),
                title="Niveau de bruit (dB)")).transform_filter(filtreIndicBruit).properties(width=largeur, height=hauteur)
    chartCompGradientSon = base.encode(
        y=alt.Y('valeur:Q', scale=alt.Scale(domain=domainGradSon), 
                 axis=alt.Axis(labelOverlap=True, titleFontSize=12),
                 title=["Gradient de célérité du son", "défavorable au riverain si >= 0.015"])).transform_filter(filtreIndicGradient).properties(width=largeur, height=hauteur/1.5)
    chartMeteo = alt.Chart(dfMeteo).transform_filter(filtreJour).mark_point(shape="arrow", opacity=0.9, filled=True).encode(
            x=alt.X('hoursminutes(date_heure):T', title='Heure', axis=alt.Axis(labels=True, labelOverlap=True, titleFontSize=12)),
            y=alt.Y("vitesseVentKmH:Q", scale=alt.Scale(domain=(-1,20)), title='Vitesse du vent (km/h)', axis=alt.Axis(labelOverlap=True, titleFontSize=12)),
            size=alt.Size('vitesseVentKmH:Q', scale=alt.Scale(rangeMax=4000, rangeMin=-10, domainMin=-1, domainMax=30), legend=None),
            angle=alt.Angle("dir_vent_haut:Q", scale=alt.Scale(domain=[0, 360], range=[180, 540])),
            color=alt.Color("jour:N"),
            stroke=alt.value('black'),
            strokeWidth=alt.value(0.5)).properties(width=largeur, height=hauteur/1.2)
    chartTemp = base.encode(
        y=alt.Y('valeur:Q', scale=alt.Scale(domain=domainTemp), title=["Température (°C)"], axis=alt.Axis(labelOverlap=True, titleFontSize=12))
                ).transform_filter("datum.indicateur == 'Température'").properties(width=largeur, height=hauteur/2)
    chartRayon = base.encode(
        y=alt.Y('valeur:Q', scale=alt.Scale(domainMax=domainMaxRayonnement), title=["Rayonnement (W/m)"], axis=alt.Axis(labelOverlap=True, titleFontSize=12))
                ).transform_filter("datum.indicateur == 'rayonnement'").properties(width=largeur, height=hauteur/2)
    chartChaleur = (chartTemp + chartRayon).resolve_scale(y='independent')
    return (chartCompBruitTrafic & chartCompGradientSon & chartMeteo & chartChaleur).resolve_scale(y='independent').properties(title=titreGlobal)
    
    
    
def fusionnerDfSources(dfEmissionTheorique, dfMeteo, df6MinBruit):
    """
    creer les df pourchaque thematique et les concatener
    """ 
    # mis en forme pour graph de la partie Emission sonore
    dfChartEmission = dfEmissionTheorique.melt(id_vars=['date_heure', 'sens', 'heure_minute', 'jour', 'jour_sort'], 
                                               value_vars=['PL', 'VL', 'vitessePL', 'vitesseVL', 'emission_bruit'],
                                               var_name='indicateur', value_name='valeur')
    # emission par sens et ajout de la'ttribut pour filtre des catégorie de jours
    dfChartEmission = dfChartEmission.assign(jour_semaine=dfChartEmission.date_heure.dt.dayofweek)
    dfChartEmission2Sens = dfChartEmission.loc[dfChartEmission.indicateur == 'emission_bruit'].groupby(
                ['date_heure', 'heure_minute', 'jour', 'jour_sort','jour_semaine', 'indicateur']).valeur.apply(
                lambda x : sommeEnergetique(*x)).reset_index()
    # partie bruit
    df6MinBruit = (df6MinBruit.assign(indicateur='bruit',
                                      jour_semaine=dfChartEmission.date_heure.dt.dayofweek,
                                      heure_minute=df6MinBruit.date_heure.apply(lambda x: f"{x.strftime('%H:%M')}"),
                                      jour=df6MinBruit.date_heure.apply(lambda x: x.strftime('%A %d %B %Y')),
                                      jour_sort=df6MinBruit.date_heure.dt.dayofyear).drop(
        ['id', 'periode_agreg', 'pression'], axis=1, errors='ignore'))
    # partie météo
    dfMeteoChart = dfMeteo.melt(id_vars='date_heure', value_vars=['grad_moy', 'ConditionPropagation', 'forcePropagation_num', 
                                                                  'dir_vent_haut', 'vts_vent_haut', 'rayonnement', 'temp_haut'], var_name='indicateur', value_name='valeur')
    dfMeteoChart = dfMeteoChart.assign(jour_semaine=dfMeteoChart.date_heure.dt.dayofweek,
                                       heure_minute=dfMeteoChart.date_heure.apply(lambda x: f"{x.strftime('%H:%M')}"),
                                       jour=dfMeteoChart.date_heure.apply(lambda x: x.strftime('%A %d %B %Y')),
                                       jour_sort=dfMeteoChart.date_heure.dt.dayofyear)
    dfMeteo = dfMeteo.assign(vitesseVentKmH=dfMeteo.vts_vent_haut*3.6,
                             jour_sort=dfMeteo.date_heure.dt.dayofyear,
                             jour=dfMeteo.date_heure.apply(lambda x: x.strftime('%A %d %B %Y')))

    dfBruitTraficMeteo = pd.concat([df6MinBruit, dfChartEmission2Sens, dfMeteoChart])
    dfBruitTraficMeteo.indicateur.replace(
        {'bruit': 'Mesure de bruit', 'emission_bruit': 'Émission de la route', 'grad_moy': 'Force de propagation du son',
        'temp_haut': 'Température'}, inplace=True)
    return dfBruitTraficMeteo, dfMeteo
    
    
def creerCorrelation(dfBruitTraficMeteo):
    """
    creer la df des correlations entre bruit mesure, meteo et emission sonore.
    suppose d'avoir une df regroupant les données cf fusionnerDfSources()
    """  
    checkAttributsinDf(dfBruitTraficMeteo, ['indicateur'])  
    checkValuesInAttribut(dfBruitTraficMeteo, 'indicateur', 'Mesure de bruit', 'Émission de la route', 'Force de propagation du son')
    dfCorrelation = dfBruitTraficMeteo.loc[(dfBruitTraficMeteo.indicateur.isin(('Mesure de bruit', 'Émission de la route', 'Force de propagation du son')))
                      ].pivot(index=['date_heure'], columns='indicateur', values='valeur').reset_index()
    dfCorrelation['Mesure de bruit'] = dfCorrelation['Mesure de bruit'].astype(float)
    dfCorrelation['Émission de la route'] = dfCorrelation['Émission de la route'].astype(float)
    dfCorrelation['Force de propagation du son'] = dfCorrelation['Force de propagation du son'].astype(float)
    dfCorrelation['bruit_pression'] = dfCorrelation['Mesure de bruit'].apply(lambda x: niveau2Pression(x))
    dfCorrelation['bruit_emission_pression'] = dfCorrelation['Émission de la route'].apply(lambda x: niveau2Pression(x))
    dfCorrelation['secondes'] = dfCorrelation.date_heure.apply(lambda x: (float(x.strftime('%H'))*3600)+(float(x.strftime('%M'))*60))
    return dfCorrelation
    