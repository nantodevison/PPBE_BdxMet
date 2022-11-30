# -*- coding: utf-8 -*-
'''
Created on 7 juin 2022

@author: martin.schoreisz
Module de parametres d'import_export des donnees
'''

import os
import pandas as pd
from Connexions.Connexion_Transfert import ConnexionBdd


def conversionNumerique(numTexte):
    """
    conversion des donnees texte dans les fichiers acoustiques
    """
    try:
        numNumeric = float(numTexte.replace(',','.'))
    except ValueError:
        numNumeric = 0
    return numNumeric
    
    
bdd = 'ech24'
startDateMesure = '2022-03-21'
endDateMesure = '2022-04-20'
with ConnexionBdd(bdd) as c:
    enum_period_agreg = pd.read_sql("select code from agreg_bruit.enum_periode_agreg", c.sqlAlchemyConn).code.tolist()
dicoInstruSite = {1: 'Ronsard', 3: 'Noyers', 4: 'Ladoumègue', 2: 'Villon'}

##############################################
# MESURES DE BUIT
##############################################
colonnesFichierMesureBruit = ['date_heure', 'leq_a', 'leq_lin', 'crete_c', 'fast_inst_a', 'fast_max_a', 'fast_min_a', 'to_6',
                              'to_8', 'to_10', 'to_12', 'to_16','to_20', 'to_25', 'to_31', 'to_40', 'to_50', 'to_63', 'to_80', 'to_100',
                              'to_125', 'to_160', 'to_200', 'to_250', 'to_315', 'to_400', 'to_500', 'to_630', 'to_800', 'to_1000',
                              'to_1250', 'to_1600','to_2000', 'to_2500', 'to_3150', 'to_4000', 'to_5000', 'to_6300', 'to_8000',
                              'to_10000', 'to_12500', 'to_16000', 'to_20000']
converters = {'leq_a': conversionNumerique, 'leq_lin': conversionNumerique, 'crete_c': conversionNumerique, 'fast_inst_a': conversionNumerique, 
              'fast_max_a': conversionNumerique, 'fast_min_a': conversionNumerique, 'to_6': conversionNumerique, 'to_8': conversionNumerique,
              'to_10': conversionNumerique, 'to_12': conversionNumerique, 'to_16': conversionNumerique, 'to_20': conversionNumerique,
              'to_25': conversionNumerique, 'to_31': conversionNumerique, 'to_40': conversionNumerique, 'to_50': conversionNumerique, 'to_63': conversionNumerique,
              'to_80': conversionNumerique, 'to_100': conversionNumerique, 'to_125': conversionNumerique, 'to_160': conversionNumerique, 'to_200': conversionNumerique,
              'to_250': conversionNumerique, 'to_315': conversionNumerique, 'to_400': conversionNumerique, 'to_500': conversionNumerique, 'to_630': conversionNumerique,
              'to_800': conversionNumerique, 'to_1000': conversionNumerique, 'to_1250': conversionNumerique, 'to_1600': conversionNumerique,
              'to_2000': conversionNumerique, 'to_2500': conversionNumerique, 'to_3150': conversionNumerique, 'to_4000': conversionNumerique,
              'to_5000': conversionNumerique, 'to_6300': conversionNumerique, 'to_8000': conversionNumerique, 'to_10000': conversionNumerique,
              'to_12500': conversionNumerique, 'to_16000': conversionNumerique, 'to_20000': conversionNumerique}
dicoMatosBruit_mesure = {'sono1': 1, 'sono2': 2, 'sono3': 3, 'sono4': 1}
dossierBoxCsv = 'https://cerema.app.box.com/folder/164712899904'


##############################################
# COLLECTE DU RESSENTI
##############################################
colonnesFixesRessenti = [0, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18, 30, 31, 32, 33, 34, 35, 86]
mappingColonnesFixesRessenti = {0: 'id', 6: 'adresse', 7: 'genre', 8: 'age', 9: 'emploi', 10: 'sensibilite_bruit', 11: 'periode_travail', 12: 'periode_travail_autre',
                                13: 'sensib_bruit_travail', 15: 'gene_long_terme', 16: 'gene_long_terme_6_18', 17: 'gene_long_terme_18_22', 18: 'gene_long_terme_22_6',
                                30: 'bati_type', 31: 'bati_annee', 32: 'debut_gene', 33: 'fin_gene', 34: 'duree_gene', 35: 'note_gene', 86: 'coment', 87: 'nom',
                                88: 'prenom', 89: 'mail'}
colonnesAjouteesRessenti = ['qualif_bruit', 'localisation_gene', 'vehicule_source', 'route_source', 'route_source_coment', 'source_bruit', 'source_bruit_coment',
                            'perturbation', 'papier']
dicoAdresseAEpurerRessenti = {'adresse':['33270', '21 rue P. de Coubertin Lot st LEU Floirac'],
                              'nom':['MOHAMMED', 'Martine', 'Françoise'],
                              'prenom': ['HAMMOUDI', 'SINAMAN', 'Ospital']}
dossierExportChartsRessenti = r'C:\Users\martin.schoreisz\Box\Cerema\3E-Prod_Env_Risq\E6-Nuisances\E61-Acoustique\60-Etudes_Locales\2022-BruitRessentiBdxMet\4-Travaux_en_cours\Illustrations\carac_reponses_enquete'


##############################################
# METEO
##############################################
colonnesMeteo = ['id', 'date_heure', 'vts_vent_haut', 'rayonnement', 'temp_haut', 'hygro_haut',
                 'dir_vent_haut', 'dir_vent_bas', 'vit_vent_bas', 'pluie', 'temp_bas', 'hygro_bas']

##############################################
# TRAFIC
##############################################
dossierTrafic = r'C:\Users\martin.schoreisz\Box\Cerema\3E-Prod_Env_Risq\E6-Nuisances\E61-Acoustique\60-Etudes_Locales\2022-BruitRessentiBdxMet\4-Travaux_en_cours\mesures\trafic\Rocade'
listFichiers6Min = [os.path.join(dossierTrafic, f) for f in ['6Min_20220321-20220419_S43-0.xls', '6Min_20220321-20220419_S43-1.xls',
                                                       '6Min_20220321-20220419_S43-2.xls', '6Min_20220321-20220419_S44-S45-0.xls',
                                                       '6Min_20220321-20220419_Ech24-0.xls', '6Min_20220321-20220419_Ech24-1.xls']]
fichierHoraireS43 = 'S43_Mar-Avril_2022.xls'
fichierHoraireS44S45 = 'S44-S45_Mar-Avril_2022.xls'
attributMesureTrafic = ['id', 'date_heure', 'id_instru_site', 'indicateur', 'valeur', 'periode_agreg', 'voie', 'sens']
dicoLibCanalVoie = {'5742.00 1': 'section courante', '5742.00 2': 'section courante', '5742.91 1': 'voie lente', '5742.93 1': 'voie médiane', '5742.95 1': 'voie rapide',
                    '5742.90 2': 'voie lente', '5742.92 2': 'voie médiane', '5742.94 2': 'voie rapide', '5243.00 1': 'section courante', '5143.00 2':'section courante',
                    '5042.11 1': 'sortie sud', '5042.13 1': 'entrée sud', '5043.15 1': 'entrée nord',
                    '5043.20 2': 'sortie nord', '5043.21 2': 'entrée nord', '5042.14 2': 'entrée sud'}
dicoLibCanalSens = {'5742.00 1': 'sens exter', '5742.00 2': 'sens inter', '5742.91 1': 'sens exter', '5742.93 1': 'sens exter', '5742.95 1': 'sens exter',
                    '5742.90 2': 'sens inter', '5742.92 2': 'sens inter', '5742.94 2': 'sens inter', '5243.00 1': 'sens exter', '5143.00 2': 'sens inter',
                    '5042.11 1': 'sens exter', '5042.13 1': 'sens exter', '5043.15 1': 'sens exter',
                    '5043.20 2': 'sens inter', '5043.21 2': 'sens inter', '5042.14 2': 'sens inter'}
dicoSiteLibCanal = {8: ['5243.00 1', '5143.00 2'], 
                    7: ['5742.00 1', '5742.00 2', '5742.91 1', '5742.93 1', '5742.95 1', '5742.90 2', '5742.92 2', '5742.94 2'], 
                    9: ['5042.11 1', '5042.13 1', '5043.15 1', '5043.20 2', '5043.21 2', '5042.14 2']}
listNatureMesure = ['Débit', 'Vitesse']

