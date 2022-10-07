'''
Created on 7 oct. 2022

@author: martin.schoreisz
extraire et traiter les données de trafic
'''

import pandas as pd
from Import_stockage_donnees.Params import bdd  # , startDateMesure, endDateMesure
from Connexion_Transfert import ConnexionBdd


def recupDonneesTraficBase(id_instru_site):
    """
    telechager les donnees meteo
    """
    with ConnexionBdd(bdd) as c:
        rqt = f"""SELECT *
                  FROM mesures_physiques.trafic
                  WHERE id_instru_site = {id_instru_site}"""
        df = pd.read_sql(rqt, c.sqlAlchemyConn)
    return df