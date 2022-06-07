# -*- coding: utf-8 -*-
'''
Created on 7 juin 2022

@author: martin.schoreisz
Module d'import et d'export des donnees stockees en Bdd
'''
import pandas as pd
import Connexion_Transfert as ct


def importIndexNiveauBruit(bdd='ech24'):
    """
    recuperation du prochain index dans la table des niveaux de bruit
    in : 
        bdd : string descriptif de la connexion bdd
    out : 
        indexMaxBruit : integer : prochaine valeur de l'index Bruit
    """
    with ct.ConnexionBdd(bdd) as c:
        indexMaxBruit = pd.read_sql("SELECT CASE WHEN max(id) IS NULL THEN 0 ELSE max(id) END +1 AS max_id FROM mesures_physiques.niveau_bruit",
                                    c.sqlAlchemyConn).iloc[0, 0]
    return indexMaxBruit
