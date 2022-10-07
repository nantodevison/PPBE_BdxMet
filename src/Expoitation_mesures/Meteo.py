# -*- coding: utf-8 -*-
'''
Created on 30 sept. 2022

@author: martin.schoreisz
exploitation des donnees meteo
'''

import pandas as pd
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