#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import sys
from pathlib import Path
if '__file__' not in globals():
    sys.path.append(str(Path().absolute().parent))
from modules.config import session
from loguru import logger


# In[2]:


@logger.catch
def loadCountries(file: str, codeFormat: str = 'ISO2') -> pd.DataFrame:
    """
    """
    indexCode = 'CODE' + codeFormat.upper()
    df = pd.read_csv(session['countries'])
    df.dropna(subset= indexCode, inplace= True)
    df.set_index(indexCode, drop= True, inplace= True, verify_integrity= True)
    
    return df


@logger.catch
def loadRegions(file: str) -> pd.DataFrame:
    """
    """
    regions_types = {'REG': str, 'CHEFLIEU': str, 'TNCC': str, 'NCC':str, 'NCCENR': str, 'LIBELLE':str}
    df = pd.read_csv(file, dtype= regions_types)
    df.set_index('REG', drop= True, inplace= True, verify_integrity= True)
    
    return df


@logger.catch
def loadCommune2Post(file: str) -> pd.DataFrame:
    """
    """
    postcodes_types = {'#Code_commune_INSEE': str, 'Nom_de_la_commune': str, 'Code_postal': str,
                       'Libellé_d_acheminement': str, 'Ligne_5': str
                      }
    df = pd.read_csv(session['postcodes'], sep= ';', encoding_errors= 'ignore', dtype= postcodes_types,
                     usecols= ['#Code_commune_INSEE', 'Nom_de_la_commune', 'Code_postal']
                    )
    df.drop_duplicates(subset= '#Code_commune_INSEE', inplace= True, ignore_index= True)
    df.set_index(['#Code_commune_INSEE'], drop= True, inplace= True, verify_integrity= True)
    return df


@logger.catch
def loadPost2Commune(file: str) -> pd.DataFrame:
    """
    """
    postcodes_types = {'#Code_commune_INSEE': str, 'Nom_de_la_commune': str, 'Code_postal': str,
                       'Libellé_d_acheminement': str, 'Ligne_5': str
                      }

    df = pd.read_csv(session['postcodes'], sep= ';', 
                     usecols= ['#Code_commune_INSEE', 
                               'Nom_de_la_commune', 
                               'Code_postal'], 
                     encoding_errors= 'ignore', dtype= postcodes_types
                     )
    df.drop_duplicates(subset= 'Code_postal', inplace= True, ignore_index= True)
    df.set_index(['Code_postal'], drop= True, inplace= True, verify_integrity= True)
    return df


@logger.catch
def loadCommunes(file: str) -> pd.DataFrame:
    """
    """
    communes_types = {'REG': str, 'DEP': str, 'CTCD': str, 'ARR': str, 'TNCC': str, 
                      'NCC': str, 'NCCENR': str, 'LIBELLE': str, 'CAN': str, 'COMPARENT': str
                     }
    df = pd.read_csv(file, dtype= communes_types)
    
    # on supprime les lignes où le type de commune 'COMD' désignant les communes déléguées est en doublon avec
    # une commune standard ('COM')
    df.drop(df.loc[
                    (df.duplicated(subset= ['COM'], keep= False)) & 
                    (df.TYPECOM == 'COMD')].index,
                    inplace= True)
    # obsolète : on n'utilise plus TYPECOM dans l'index, mais seulement COM
    # en raison des codes arrondissements en particulier, pour MArseille, Lyon et Paris
    df.set_index(['COM'], drop= True, inplace= True, verify_integrity= True)

    return df

@logger.catch
def loadNuts(file: str) -> pd.DataFrame:
    """
    """
    return pd.read_csv(file, sep= ';', index_col= 'NUTS3', dtype= object)


@logger.catch
def loadIso3166(file: str) -> pd.DataFrame:
    """
    """
    df =  pd.read_csv(file, sep= ';', index_col= 'ISO-3166-2', dtype= object, encoding_errors= 'ignore')
    df['Ancien'] = df.Ancien.str.strip()
    return df


@logger.catch
def loadDepartements(file: str) -> pd.DataFrame:
    """
    """
    return pd.read_csv(file, dtype= object, index_col= 'DEP')


@logger.catch
def loadArrondissements(file: str) -> pd.DataFrame:
    """
    """
    df = pd.read_csv(file, index_col= 'ARR', dtype= object)
    return df


@logger.catch
def loadCantons(file: str) -> pd.DataFrame:
    """
    """
    df = pd.read_csv(file, index_col= 'CAN', dtype= object)
    return df


@logger.catch
def loadComer(file: str, index: str) -> pd.DataFrame:
    """
    """
    df = pd.read_csv(file, dtype= object)
    df.set_index(index, drop= True, inplace= True)
    return df

