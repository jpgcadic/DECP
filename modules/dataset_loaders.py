#!/usr/bin/env python
# coding: utf-8

# In[8]:


import pandas as pd
import numpy as np
import sys
from pathlib import Path
import glob


# In[2]:


if '__file__' not in globals():
    sys.path.append(str(Path().absolute().parent))

from modules.config import *


# In[9]:


def loadDecpV3(dataset: str):
    """
    Chargement du dataset et prétraitement des colonnes pour uniformisation entre les versions successives.
    Par défaut le dataset chargé est celui défini dans la variable session.
    """
    # par défaut on considère des données type string, seules les exceptions de type numériques sont listées explicitement
    typesDecpV3 = {x: str if x not in colonnesNum.keys() else colonnesNum[x] for x in useColsV3}

    decp = pd.read_csv(dataset, sep= ';', usecols= useColsV3, dtype= typesDecpV3)
    
    decp.fillna({'lieuExecution.code': ''}, inplace= True)
    
    # suppression des identifiants non SIRET (TVA, hors UE etc)
    decp = decp.loc[(decp['titulaire_typeIdentifiant_1'] == 'SIRET') & 
                    ((decp['titulaire_typeIdentifiant_2'].isna())|(decp['titulaire_typeIdentifiant_2'] == 'SIRET')) &
                    ((decp['titulaire_typeIdentifiant_3'].isna())|(decp['titulaire_typeIdentifiant_3'] == 'SIRET'))]
    
    decp.rename(columns= renameColsV3, inplace= True)

    # conversion des colonnes de type float en int si besoin
    decp[['dureeMois', 'offresRecues']] = decp[['dureeMois', 'offresRecues']].astype('Int64')
    
    # normalisation des colonnes considérations sociales, considérations environnementales, modalités d'exécution 
    colsConsiderationsSociales = ['Pas de considération sociale', 'Clause sociale', 'Critère social', 'Marché réservé']
    colsConsiderationsEnvironnementales = ['Pas de considération environnementale',
                                           'Critère environnemental',
                                           'Clause environnementale']
    colsModaliteExecution = ['Bons de commande', 'Tranches', 'Marchés subséquents', 'Mixte']
    varsModaliteExecution = ['Bon* de commande', 'Tranche*', 'Marché* subséquent*', 'Mixte']
    
    for c in colsConsiderationsSociales:
        decp[c] = decp.considerationsSociales.str.contains(c, regex= False)
    
    for c in colsConsiderationsEnvironnementales:
        decp[c] = decp.considerationsEnvironnementales.str.contains(c, regex= False)
    
    for c, v in zip(colsModaliteExecution, varsModaliteExecution):
        decp[c] = decp.considerationsEnvironnementales.str.contains(v, regex= True)

    for c, v in zip(colsTechniques, varsTechniques):
        decp[c] = decp.techniques.str.contains(v, regex= False)

    decp = mergeTypePrixV3(decp)
    
    decp.drop(columns= dropColsV3, inplace= True)

    # suppression des doublons sur la clé (id, acheteur.id) :
    # dans les cas de partenariats on peut retrouver le même contrat avec les mêmes titulaires notés dans un ordre différent
    decp.drop_duplicates(subset= ['id', 'acheteur.id'], keep= 'first', inplace= True)

    repBools = {'non': 0, 'Non': 0, 'false': 0, 'False': 0, 'oui': 1, 'Oui': 1, 'true': 1, 'True': 1, 0: 0}
    decp.loc[:,colonnesBool] = decp.loc[:, colonnesBool].fillna(value= 0)
    decp.loc[:, colonnesBool] = decp.loc[:, colonnesBool].map(lambda x: repBools[x])
    
    return decp

def mergeTypePrixV3(decp: pd.DataFrame):
    """
    Combine les 3 colonnes ['typePrix', 'typesPrix', 'TypePrix'] en une seule colonne typesPrix, pour alignement avec v4
    Les types de prix sont exclusifs l'un de l'autre.
    """

    colsTypesPrix = ['typePrix', 'typesPrix', 'TypePrix']

    # option de création de nouvelles colonnes pas retenue pour le moment
    # for c in valTypesPrix:
    #     decp[c] = decp.typesPrix.str.contains(c, regex= False)
    
    for c in valTypesPrix:
        decp[colsTypesPrix] = decp[colsTypesPrix].apply(lambda x: c if c in x else x) # uniformisation des valeurs
    decp[colsTypesPrix] = decp[colsTypesPrix].apply(lambda x: '' if 'NC' in x else x) # remplacement des NC par ''

    decp['typesPrix'] = decp.typePrix.fillna('') + decp.TypePrix.fillna('') + decp.typesPrix.fillna('')

    return decp


# In[11]:


def loadDecpV4(dataset: str):
    """
    Pour chargement des données conformes arrêté 2022
    Chargement du dataset et prétraitement des colonnes pour uniformisation entre les versions successives.
    Par défaut le dataset chargé est celui défini dans la variable session.
    """
    
    # par défaut on considère des données type string, seules les exceptions de type numériques sont listées explicitement
    typesDecpV4 = {x: str if x not in colonnesNum.keys() else colonnesNum[x] for x in useColsV4}
    
    decp = pd.read_csv(dataset, sep= ';', usecols= useColsV4, dtype= typesDecpV4,
                       na_values= ['CDL', 'INX None', 'MQ'])
    
    decp.fillna({'lieuExecution.code': ''}, inplace= True)
    
    # suppression des identifiants non SIRET (TVA, hors UE etc)
    decp = decp.loc[(decp['titulaire_typeIdentifiant_1'] == 'SIRET') & 
                    ((decp['titulaire_typeIdentifiant_2'].isna())|(decp['titulaire_typeIdentifiant_2'] == 'SIRET')) &
                    ((decp['titulaire_typeIdentifiant_3'].isna())|(decp['titulaire_typeIdentifiant_3'] == 'SIRET'))]

    # correction des quelques lignes où la date de notification est absente, remplacement par date de publication
    # et inversement.
    decp.loc[decp.dateNotification.isna(), 'dateNotification'] = decp.loc[decp.dateNotification.isna()].datePublicationDonnees
    decp.loc[decp.datePublicationDonnees.isna(), 'datePublicationDonnees'] = decp.loc[decp.datePublicationDonnees.isna()].dateNotification
    
    # ajout des colonnes 'denomination sociale'
    decp[addColsV4] = ''

    # initialisation spécifique de 'acheteur.nom' pour déclencher sa mise à jour dans le constructeur de noeud acheteur  
    decp['acheteur.nom'] = np.nan

    # conversion des colonnes de type float en int si besoin
    decp[['dureeMois', 'offresRecues']] = decp[['dureeMois', 'offresRecues']].astype('Int64')
    
    # normalisation des colonnes considérations sociales, considérations environnementales, modalités d'exécution 
    for c in colsConsiderationsSociales:
        decp[c] = decp.considerationsSociales.str.contains(c, case= False, regex= False)
    
    for c in colsConsiderationsEnvironnementales:
        decp[c] = decp.considerationsEnvironnementales.str.contains(c, case= False, regex= False)
    
    for c, v in zip(colsModaliteExecution, varsModaliteExecution):
        decp[c] = decp.modalitesExecution.str.contains(v, case= False, regex= False)
    
    for c, v in zip(colsTechniques, varsTechniques):
        decp[c] = decp.techniques.str.contains(v, case= False, regex= False)
    
    # les types de prix sont exclusifs l'un de l'autre
    # for c in colsTypesPrix:
    #     decp[c] = decp.typesPrix.str.contains(c, regex= False)
    for c in valTypesPrix:
        decp.typesPrix = decp.typesPrix.apply(lambda x: c if c in x else x)
    decp.typesPrix = decp.typesPrix.apply(lambda x: '' if 'NC' in x else x)

    # suppression des doublons sur la clé (id, acheteur.id) :
    # dans les cas de partenariats on peut retrouver le même contrat avec les mêmes titulaires notés dans un ordre différent
    decp.drop_duplicates(subset= ['id', 'acheteur.id'], keep= 'first', inplace= True)

    repBools = {'non': 0, 'Non': 0, 'false': 0, 'False': 0, 'oui': 1, 'Oui': 1, 'true': 1, 'True': 1, 0: 0}
    decp.loc[:,colonnesBool] = decp.loc[:, colonnesBool].fillna(value= 0)
    decp.loc[:, colonnesBool] = decp.loc[:, colonnesBool].map(lambda x: repBools[x])

    decpv4.loc[decpv4['tauxAvance'] == 'INX 0.0', 'tauxAvance'] = '0'
    
    return decp


# In[7]:


def mergeDatasets(version, folder: str = 'Z:/datasets/DECP/', filePrefix: str = 'decp-v3-marches-valides-'):
    """
    version = 'v3' ou 'v4'
    """
    decp = None
    # par défaut on considère des données type string, seules les exceptions de type numériques sont listées explicitement
    typesDecpV3 = {x: str if x not in colonnesNum.keys() else colonnesNum[x] for x in useColsV3}

    for dataset in glob.glob(folder + filePrefix + "*.csv"):
        if version == 'v3':
            print(dataset)
            df = loadDecpV3(dataset)
            if isinstance(decp, pd.DataFrame):
                decp = pd.concat([decp, df], axis= 0, ignore_index= True)
            else:
                decp = df

    # suppression des doublons sur la clé (id, acheteur.id) :
    # dans les cas de partenariats on peut retrouver le même contrat avec les mêmes titulaires notés dans un ordre différent
    decp.drop_duplicates(subset= ['id', 'acheteur.id'], keep= 'first', inplace= True)

    fileOut = folder +'out/' + filePrefix + 'all.csv'
    print(fileOut)
    decp.to_csv(fileOut, sep= ';', index= False)

    return decp

