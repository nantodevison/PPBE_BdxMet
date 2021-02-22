# -*- coding: utf-8 -*-
'''
Created on 22 f�vr. 2021

@author: Martin
module pour la creation des graphs lies a l'emission
'''

import altair as alt

def horaireParJourVtsForfait(dfHoraireEmission):
    """
    emission horaire selon le jour de la semaine (1 ligne par jour), avec vitesse forfaitaire
    in : 
       dfHoraireEmission : donnees d'emission issue de Analyse_emission.importFichierTraficDIRA 
    """
    return alt.Chart(dfHoraireEmission, title='Emission par jour a vitesse forfaitaire').mark_line().encode(
            x='hours(dateHeure):T',
            y=alt.Y('emission:Q',scale=alt.Scale(zero=False)),
            color=alt.Color('day(dateHeure):T', scale=alt.Scale(scheme='tableau10'),sort=[0,1,2]),)
    
def vtsJourTypeSens(dFVtsFinale, sens, typeJour):
    """
    graph des vitesses horaires selon le type de jour, et le sens
    in : 
        dFVtsFinale : df issue de Analyse_emission.importFichierVtsGroupe, 
        sens : sens a considerer dans le trafic : sensexter ou sensinter
        typeJour : type de jour a considerer :JO ou Samedi ou Dimanche
    """
    return alt.Chart(dFVtsFinale.loc[(dFVtsFinale.sens==sens)&(dFVtsFinale.typeJour==typeJour)].set_index(['heure','typeJour','sens']).stack().reset_index().rename(columns={'level_3':'typVts', 0:'vts'})).mark_line().encode(
            x=alt.X('heure', sort=alt.Sort(['h0_1,h1_2'])),
            y=alt.Y('vts', scale=alt.Scale(zero=False)),
            color='typVts',
            shape='sens')
    
def compSensJO(dFVtsFinale, typeJour='JO'):
    """
    comparer les émissions par selon un même type de jour
    in : 
        dFVtsFinale : df issue de Analyse_emission.importFichierVtsGroupe
    """
    return vtsJourTypeSens(dFVtsFinale, 'sensexter', typeJour) + vtsJourTypeSens(dFVtsFinale, 'sensinter', typeJour)
    
def emissionTypeJour(dfEmissionSensHeureJour):
    """
    renvoyer un graph avec l'emission globale selon le type de jour issu de la sommme energetique des emissions de chaque sens
    in : 
        dfEmissionSensHeureJour : df issue de Analyse_emission.emissionSensVts()
    """
    return alt.Chart(dfEmissionSensHeureJour.assign(heure=dfEmissionSensHeureJour.heure.apply(lambda x : int(x.split('_')[0][1:])))).mark_line().encode(
            x=alt.X('heure', sort='ascending', scale=alt.Scale(domain=(0,24))),
            y=alt.Y('emission_tot:Q',scale=alt.Scale(zero=False)),
            color='typeJour')
    
def compEmission(dfCompEmission):
    """
    comparer les emissions de plusieurs période
    in : 
        dfCompEmission : df des emissions pour les JO suivant les periodes, issue de Analyse_emission.compEmission()
    """
    return alt.Chart(dfCompEmission).mark_line().encode(
            x = alt.X('heure', sort=alt.Sort([f'h{i}_{i+1}'for i in range(24)])), 
            y = alt.Y('emission_tot', scale=alt.Scale(zero=False)),
            color=alt.Color('periode')).properties(width=600)

def compEmissionVts(dfVtsToutePeriode):
    """
    graphde compraison horaire des vitesses moyennes JO pour chaque période
    in : 
        dfVtsToutePeriode : df des vitesses pour les JO, par sens, suivant les periodes, issue de Analyse_emission.compEmission()
    """
    return alt.vconcat(alt.Chart(dfVtsToutePeriode.loc[dfVtsToutePeriode.sens=='sensinter'], title='Comparaison des vitesses sens interieur').mark_line().encode(
            x = alt.X('heure', sort=alt.Sort([f'h{i}_{i+1}'for i in range(24)])),
            y = alt.Y('vMoy', scale=alt.Scale(zero=False)),
            color='periode'),alt.Chart(dfVtsToutePeriode.loc[dfVtsToutePeriode.sens=='sensexter'], title='Comparaison des vitesses sens exterieur').mark_line().encode(
            x = alt.X('heure', sort=alt.Sort([f'h{i}_{i+1}'for i in range(24)])),
            y = alt.Y('vMoy', scale=alt.Scale(zero=False)),
            color='periode')) 
    
def compEmissionTraf(dfTraficToutePeriode):
    """
    graphde compraison horaire des trafics moyen JO pour chaque période
    in : 
        dfTraficToutePeriode : df des trafics pour les JO, par sens, suivant les periodes, issue de Analyse_emission.compEmission()
    """
    return alt.vconcat(alt.Chart(dfTraficToutePeriode.loc[dfTraficToutePeriode.sens=='sensinter'], title='Comparaison des trafics sens interieur').mark_line().encode(
        x = alt.X('heure', sort=alt.Sort([f'h{i}_{i+1}'for i in range(24)])),
        y = alt.Y('nbVeh', scale=alt.Scale(zero=False)),
        color='periode'),alt.Chart(dfTraficToutePeriode.loc[dfTraficToutePeriode.sens=='sensexter'], title='Comparaison des trafics sens exterieur').mark_line().encode(
        x = alt.X('heure', sort=alt.Sort([f'h{i}_{i+1}'for i in range(24)])),
        y = alt.Y('nbVeh', scale=alt.Scale(zero=False)),
        color='periode'))       
            
def compParamsEmission(chartVts, ChartTraf):
    """
    agreger les charts issues de compEmissionVts et compEmissionTraf
    in : 
        chartVts, ChartTraf : altair charts, cf compEmissionVts()  et compEmissionTraf()
    """
    return alt.hconcat(chartVts,ChartTraf)    
        
        
        
        