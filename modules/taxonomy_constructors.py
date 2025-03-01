#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import sys
from pathlib import Path
if '__file__' not in globals():
    sys.path.append(str(Path().absolute().parent))
from modules.config import session

from modules.neomodel_classes import modelVersion, CPV, NAF
from loguru import logger
from datetime import date, datetime
import pytz
from dateutil import parser
import re
from numpy import ndarray
import numpy as np


# In[2]:


@logger.catch
def loadCPV2008() -> pd.DataFrame:
    """
    """
    df = pd.read_csv(session['cpv2008'], sep= ';', dtype= str, usecols= ['CODE CPV', 'FR Label'])
    df = df.rename(columns = {'CODE CPV': 'code', 'FR Label': 'designation'})
    df.code = df.code.apply(lambda x: x.split('-')[0])

    upperCategoryMaskSize = {2: None, 3: 2, 4: 3, 5: 4, 6: 5, 7: 5, 8: 5}
    values = df.code.values
    df['upper'] = df.code.apply(getUpperCategory, values= values, upperCategoryMaskSize= upperCategoryMaskSize, versionCPV= '2008')

    # ajout de la désignation de la catégorie supérieure - mais n'est plus utilisé dans le reste du code
    df = df.set_index('upper')
    df2 = df.set_index('code')
    df['designationUpper'] = df2['designation']
    df = df.reset_index().reindex(columns= ['code', 'designation', 'upper', 'designationUpper'])
    
    return df


@logger.catch
def loadCPV2003() -> pd.DataFrame:
    """
    """
    df = pd.read_csv(session['cpv2003'], sep= ';', dtype= str, encoding= 'latin-1', encoding_errors= 'strict',
                     header= 0, usecols= [0, 2, 3], names= ['CPV 2007', 'code', 'designation'],
                     na_values='/')
    df = df[df.code.notna()]
    df.code = df.code.apply(lambda x: x.split('-')[0])
    df['CPV 2007'] = df['CPV 2007'].apply(lambda x: x.split('-')[0])

    upperCategoryMaskSize = {2: None, 3: 2, 4: 3, 5: 4, 6: 5, 7: 5, 8: 5}
    values = df.code.values
    df['upper'] = df.code.apply(getUpperCategory, values= values, upperCategoryMaskSize= upperCategoryMaskSize, versionCPV= '2003')

    return df


# In[3]:


@logger.catch
def getUpperCategory(code: str, values: ndarray, upperCategoryMaskSize: dict, versionCPV: str) -> str:
    """
    """
    category = min(8, max(2, re.search('0*$', code).start()))

    maskSize = upperCategoryMaskSize[category]
    if maskSize is None:
        # renvoi du même pour indiquer qu'on doit rattacher au noeud racine 
        upperCategoryCode = code 
    else:
        found = False
        while not found and maskSize > 1:
            # on détermine la catégorie supérieure 'théorique', p. ex : 42924730 -> 42924000, ou bien 35612200 -> 35612000
            padding = '0' * (8 - maskSize)
            upperCategoryCode = code[0:maskSize] + padding
            # on recherche si ce code est présent
            if upperCategoryCode not in values:
                # pas de catégorie supérieure non connectée, on tente de se rattacher à la catégorie encore supérieure
                maskSize -= 1
                logger.trace("Rattachement à catégorie supérieure à {} pour code {} dans version CPV {}",
                             upperCategoryCode, code, versionCPV)
            else:
                found = True
        if not found:
            logger.trace("Catégorie supérieure de code {} dans version CPV {} non trouvée", code, versionCPV)
            upperCategoryCode = None
    
    return upperCategoryCode


# In[4]:


@logger.catch
def addCpvNodes(cpv: pd.Series) -> CPV :
    """
    Le code court sur 8 digits (sans la clé de contrôle) est utilisé pour constituer ce référentiel.
    categories = {1: 'Top', 2: 'Division', 3: 'Group', 4: 'Class', 5: 'Category', 6: 'Code', 7: 'Code', 8: 'Code'}
    """
    category = min(8, max(2, re.search('0*$', cpv.code).start()))
    thisNode = CPV(code = cpv.code, designation= cpv.designation, category = category,
               recordCreationDate = datetime.now(tz= pytz.timezone('CET')),
               modelVersion = modelVersion).save()

    return cpv.code, thisNode


# In[5]:


@logger.catch
def connectCpvNodes(cpv: pd.Series, top: CPV, toConnect: dict) -> CPV :
    """
    Le code court sur 8 digits (sans la clé de contrôle) est utilisé pour constituer ce référentiel.
    categories = {1: 'Top', 2: 'Division', 3: 'Group', 4: 'Class', 5: 'Category', 6: 'Code', 7: 'Code', 8: 'Code'}
    """
    fromNode = toConnect[cpv.code]
    
    if cpv.upper != cpv.code:
        toNode = toConnect[cpv.upper]
    else:
        toNode = top        # on connecte à la racine
    
    fromNode.parentCategory.connect(toNode)

    return cpv.code


# In[6]:


@logger.catch
def CpvEquivalences(cpv: pd.Series, toConnect2008: dict, toConnect2003: dict):
    """
    toConnect2008, toConnect2003 : dictionnaires avec clé = code, valeur = objet noeud correspondant
    """
    toConnect2008[cpv['CPV 2007']].replaces.connect(toConnect2003[cpv.code])

    return None


# In[7]:


@logger.catch
def installCpv():
    """
    """
    logger.trace("Chargement CPV 2008")
    cpv2008 = loadCPV2008()
    logger.trace("Création noeud racine 2008")
    start2008Node = CPV(versionCPV= '2008', code= '0', category= 1, modelVersion= modelVersion,
                        recordCreationDate = datetime.now(tz= pytz.timezone('CET'))).save()

    logger.trace("Création des noeuds CPV 2008")
    dfConnect = cpv2008.apply(addCpvNodes, axis= 1)
    logger.trace("Connexion des noeuds CPV 2008")
    toConnect2008 = {dfConnect.loc[x][0]: dfConnect.loc[x][1] for x in dfConnect.index}
    dfConnected = cpv2008.apply(connectCpvNodes, axis= 1, top= start2008Node, toConnect= toConnect2008)
    
    logger.trace("Chargement CPV 2003")
    cpv2003 = loadCPV2003()
    logger.trace("Création noeud racine 2003")
    start2003Node = CPV(versionCPV= '2003', code= '0', category= 1, modelVersion= modelVersion,
                        recordCreationDate = datetime.now(tz= pytz.timezone('CET'))).save()
    logger.trace("Création des noeuds CPV 2003")
    dfConnect = cpv2003.drop_duplicates(subset= 'code').apply(addCpvNodes, axis= 1)
    logger.trace("Connexion des noeuds CPV 2003")
    toConnect2003 = {dfConnect.loc[x][0]: dfConnect.loc[x][1] for x in dfConnect.index}
    dfConnected = cpv2003.drop_duplicates(subset= 'code').apply(connectCpvNodes, axis= 1, top= start2003Node,
                                                                toConnect= toConnect2003)

    logger.trace("Création des équvalences de codes 2008 - 2003")
    res = cpv2003.apply(CpvEquivalences, axis= 1, toConnect2008= toConnect2008, toConnect2003= toConnect2003)
    
    return cpv2008, cpv2003, toConnect2008, toConnect2003

    # pour installation :
    # cpv2008, cpv2003, toConnect2008, toConnect2003 = installCpv()


# In[8]:


@logger.catch
def loadNaf() -> pd.DataFrame:
    """
    """
    naf = pd.read_excel(session['naf'], usecols= [1, 2, 3, 4],    # on supprime les lignes intercalaires 
                        dtype= str, names= ['code', 'naf2', 'naf2_65', 'naf2_40']).dropna(how= 'all').reset_index(drop= True)

    # on va déplacer vers des colonnes les informations des lignes en-tête indiquant les sections de la NAF, puis les supprimer
    # c'est l'objet des 3 lignes suivantes.
    secNames = naf[naf.code.str.startswith('SECTION')][['code', 'naf2']]
    
    # l'index du dataframe des sections NAF indique les délimitations de ces sections dans le dataframe naf
    # on parcourt l'index de naf pour calculer (fonction np.where) l'index dans le dataframe des noms de section NAF
    # il y a un léger glitch sur les lignes en-tête (p. ex index 95 'SECTION B' est indiquée 'SECTION A' en colonne 'section')
    # mais ces lignes sont ensuite supprimées.

    naf[['section', 'nom_section']] = secNames.iloc[[max(0, np.where(np.sort(np.append(secNames.index, i)) == i)[0][0] - 1) for i in naf.index], [0, 1]].reset_index(drop= True)

    # on supprime les lignes en-tête
    naf = naf[~naf.code.str.startswith('SECTION')].reset_index(drop= True)

    return naf


# In[9]:


@logger.catch
def addNafSections(naf: pd.DataFrame) -> NAF:
    """
    naf
    """
    sections = naf[['section', 'nom_section']].drop_duplicates(keep= 'first')
    sectionsNodes = sections.apply(lambda x: NAF(sectionCode= x.section, sectionName= x.nom_section,
                                                 modelVersion= modelVersion).save(), axis= 1)
    
    return sectionsNodes


@logger.catch
def addNafNodes(naf: pd.Series) -> NAF:
    """
    """
    
    level = len(naf.code.replace('.', ''))
    
    nafNode = NAF(code= naf.code, levelName= list(NAF.levelsNaf.keys())[level - 1],
                  label= naf.naf2, label65= naf.naf2_65, label40= naf.naf2_40,
                  modelVersion= modelVersion).save()

    return nafNode


@logger.catch
def connectNafNodes(naf: pd.Series):
    """
    Les codes sont de la forme : 
      12.34A -> longueur 6, niveau = sous-classe
      12.34  -> longueur 5, niveau = classe
      12.3   -> longueur 4, niveau = groupe
      12     -> longueur 2, niveau = division
      Il n'y a donc pas de code de longueur 3.
    """
    if len(naf.code) == 2:
        toNode = NAF.nodes.get(sectionCode= naf.section)
    else:
        if len(naf.code) == 4:
            toNode = NAF.nodes.get(code= naf.code[0:len(naf.code) - 2])
        else:
            toNode = NAF.nodes.get(code= naf.code[0:len(naf.code) - 1])
            
    fromNode = NAF.nodes.get(code= naf.code)
    fromNode.upperSection.connect(toNode)

    return None


@logger.catch
def installNaf():
    """
    """
    logger.trace("Chargement dataset")
    naf = loadNaf()
    logger.trace("Ajout des noeuds section")
    addNafSections(naf)
    logger.trace("Ajout des noeuds NAF")
    naf.apply(addNafNodes, axis= 1)
    logger.trace("Connexion des noeuds NAF")
    naf.apply(connectNafNodes, axis= 1)
    
    return naf

