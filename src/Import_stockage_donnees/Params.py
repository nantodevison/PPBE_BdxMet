# -*- coding: utf-8 -*-
'''
Created on 7 juin 2022

@author: martin.schoreisz
Module de parametres d'import_export des donnees
'''


def conversionNumerique(numTexte):
    """
    conversion des donnees texte dans les fichiers acoustiques
    """
    try:
        numNumeric = float(numTexte.replace(',','.'))
    except ValueError:
        numNumeric = 0
    return numNumeric
    
    
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