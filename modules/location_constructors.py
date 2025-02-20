#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import numpy as np
import openpyxl
from configparser import ConfigParser, ExtendedInterpolation
import os
import sys
from pathlib import Path
from unidecode import unidecode
import requests
from requests.exceptions import RequestException
from datetime import date, datetime
import pytz
from dateutil import parser
from loguru import logger

from neo4j import GraphDatabase

from neomodel import config, db, install_labels, install_all_labels
from neomodel import (StructuredNode,  RelationshipTo, RelationshipFrom, StructuredRel)
from neomodel import (StringProperty, IntegerProperty, FloatProperty, ArrayProperty, BooleanProperty, DateProperty, UniqueIdProperty)
# from neomodel import (AliasProperty, RegexProperty, JSONProperty, DateTimeProperty, DateTimeFormatProperty, PointProperty
from neomodel.exceptions import *

from neomodel.integration.pandas import to_dataframe, to_series
from neomodel.integration.numpy import to_ndarray

from socket import gaierror

from pynsee.utils.init_conn import init_conn
from pynsee.macrodata import  get_dataset_list, get_series_list, get_series, get_series_title, search_macrodata, get_dataset
from pynsee.localdata import get_local_metadata, get_local_data
from pynsee.metadata import get_activity_list
from pynsee.sirene import get_dimension_list, search_sirene, get_sirene_data, get_sirene_relatives
from pynsee.geodata import get_geodata_list, get_geodata
from pynsee import get_file_list, download_file

import eurostat 

from loguru import logger


# In[3]:


if '__file__' not in globals():
    sys.path.append(str(Path().absolute().parent))
from modules.config import *
from modules.neomodel_classes import *
from modules.location_loaders import *


# In[11]:


@logger.catch
def getCity(codes: dict) -> City | Cedex:
    """
    - codes : Code commune INSEE ou code postal, selon codeType, sous forme String
    Le codeType est soit 'code commune' ou 'code postal' ou 'cedex'
    code['final'] = 'commune' ou 'postal'

    Cette fonction crée également le département correspondant au code commune si non existant,
    et effectue son rattachement.
    
    retourne un noeud City ou Cedex.
    """
    node = None
    try:
        if codes['final'] == 'cedex':
            node = Cedex.nodes.get(code = codes[codes['final']])
        elif codes['final'] == 'commune':
            node = City.nodes.get(code = codes[codes['final']])
        else:
            node = None

    except DoesNotExist:
        if codes['final'] == 'cedex':
            # on crée le noeud correspondant
            logger.trace("Création Cedex {}", codes['cedex'])

            node = Cedex(modelVersion = modelVersion,
                         recordCreationDate = datetime.now(tz= pytz.timezone('CET')),
                         recordLastUpdate   = datetime.now(tz= pytz.timezone('CET')),
                         code = codes['cedex'], codeCedex = codes['cedex'],
                         nameOrigin = codes['nom'],
                         codeType = codes['type']
                         ).save()
        elif codes['final'] == 'commune':
            logger.trace("Création Commune {} {}", codes['commune'], codes['commune_name'])

            node = City(modelVersion = modelVersion,
                        recordCreationDate = datetime.now(tz= pytz.timezone('CET')),
                        recordLastUpdate   = datetime.now(tz= pytz.timezone('CET')),
                        code = codes['commune'], codeCommune = codes['commune'],
                        nameOrigin = codes['nom'],
                        postCode = codes['postal'], name = codes['commune_name'], cityName = codes['commune_name']
                       ).save()
        else:
            node = None

        # pour finaliser la création du noeud : récupérer le noeud Departement ou créer le noeud Departement si non existant
        departement = getDepartement(codes)
        if departement is not None:
            if node is not None:
                node.departement.connect(departement)

    if node is not None:
        node.save()
    
    return node


# In[12]:


@logger.catch
def getRegionFromDepartement(code: str) -> (str, str):
    """
    Le code peut être un code postal, un code commune ou le numéro du département.
    On ne s'intéresse qu'aux 2 ou 3 premiers caractères.
    """
    if len(code) < 2:
        return None
        
    numDpt = code[0:2]
    if numDpt in ['97', '98']: # DROMs
        numDpt = code[0:3]
        
    # cas particulier de la Corse, où les codes postaux et Cedex commencent par 20, avec 2 départements 2A et 2B
    if numDpt == '20':
        numDpt = '2A' # on prend le code de la Corse du Sud, peu importe
    try:
        if isinstance(session['df_departements'], pd.DataFrame):
            if isinstance(session['df_comer'], pd.DataFrame):
                if isinstance(session['df_communes_comer'], pd.DataFrame):
                    if isinstance(session['df_regions'], pd.DataFrame):
                        pass
    except KeyError:
        session['df_departements'] = loadDepartements(session['departements'])
        session['df_comer'] = loadComer(session['comer'], index= 'COMER')
        session['df_communes_comer'] = loadComer(session['comer'], index= 'COM_COMER')
        session['df_regions'] = loadRegions(session['regions'])

    if numDpt in session['df_comer'].index:
        try:
            region = session['df_communes_comer'][session['df_communes_comer'].COMER == numDpt].NATURE_ZONAGE.values[0]
            region_name = region
        except (KeyError, IndexError):
            region = '0'
            region_name = '0'

    else:
        try:
            region = session['df_departements'].loc[numDpt, 'REG']
            try:
                region_name = session['df_regions'].loc[region, 'NCC']
            except KeyError:
                logger.trace("Nom de région {} pas trouvé depuis le code département {}, code en entrée : {}",
                             region, numDpt, code)
        except KeyError:
            # code département pas trouvé dans la table
            logger.trace("Code département {} pas trouvé depuis le code département en entrée : {}", numDpt, code)
            region = '0'
            region_name = '0'
        
    return region, region_name


# In[13]:


@logger.catch
def getArrondissement(codes: dict) -> Arrondissement:
    """
    - codes['code'] : code sur 3 ou 4 caractères, numérique, sauf 2Ax et 2Bx (Corse), 97xy (DROMs):

    Cette fonction crée également le noeud de niveau supérieur (Departement) si non existant.
    retourne un objet Arrondissement.
    """
    if codes['arrondissement'] == '':
        logger.trace("Code arrondissement non fourni : {}", codes)
        return None

    arrondissement = None
    # récupérer le noeud Arrondissement ou créer le noeud Arrondissement si non existant
    try:
        arrondissement = Arrondissement.nodes.get(arrondissementCode = codes['arrondissement'])

    except DoesNotExist:
        # Arrondissement pas encore créé
        logger.trace("Création Arrondissement {} {}", codes['arrondissement'], codes['arrondissement_name'])

        arrondissement = Arrondissement(modelVersion = modelVersion,
                                        recordCreationDate = datetime.now(tz= pytz.timezone('CET')),
                                        recordLastUpdate   = datetime.now(tz= pytz.timezone('CET')),
                                        code = codes['arrondissement'],
                                        name = codes['arrondissement_name'],
                                        nameOrigin = codes['nom'],
                                        arrondissementCode = codes['arrondissement'],
                                        arrondissementName = codes['arrondissement_name']).save()

        departement = getDepartement(codes)
        if departement is not None:
            if arrondissement is not None:
                arrondissement.departement.connect(departement)
                arrondissement.save()

    return arrondissement


# In[14]:


@logger.catch
def getCanton(codes: dict) -> Canton:
    """
    - codes['code'] : code sur 4 ou 5 caractères, numérique, sauf 2Axx et 2Bxx (Corse), 97xyy (DROMs):

    Cette fonction crée également le noeud de niveau supérieur (Departement) si non existant.
    retourne un objet Canton.
    """
    if codes['canton'] == '':
        logger.trace("Code canton non fourni : {}", codes)
        return None

    canton = None
    # récupérer le noeud Canton ou créer le noeud Canton si non existant
    try:
        canton = Canton.nodes.get(cantonCode = codes['canton'])

    except DoesNotExist:
        # Canton pas encore créé
        logger.trace("Création Canton {} {}", codes['canton'], codes['canton_name'])
        canton = Canton(modelVersion = modelVersion,
                        recordCreationDate = datetime.now(tz= pytz.timezone('CET')),
                        recordLastUpdate   = datetime.now(tz= pytz.timezone('CET')),
                        code = codes['canton'],
                        name = codes['canton_name'],
                        nameOrigin = codes['nom'],
                        cantonCode = codes['canton'],
                        cantonName = codes['canton_name']).save()

        departement = getDepartement(codes)
        if departement is not None:
            if canton is not None:
                canton.departement.connect(departement)
                canton.save()

    return canton


# In[15]:


@logger.catch
def getDepartement(codes: dict) -> Departement:
    """
    - codes['code'] : code, sous forme String, selon dénomination :
        - INSEE : 01, ..., 95, 971, 972, 973, 974, 976
        - ISO : FR-xy[z]
        - extension du code ISO après le 'FR-'
        - valeurs 'custom' au cas par cas

    Cette fonction crée également le noeud de niveau supérieur (Region) si non existant.
    retourne un objet Departement.
    """
    if codes['departement'] == '':
        logger.trace("Code département non fourni : {}", codes)
        return None

    departement = None
    # récupérer le noeud Departement ou créer le noeud Departement si non existant
    try:
        departement = Departement.nodes.get(departementCode = codes['departement'])

    except DoesNotExist:
        # departement pas encore créé
        logger.trace("Création Département {} {}", codes['departement'], codes['departement_name'])

        departement = Departement(modelVersion = modelVersion,
                                  recordCreationDate = datetime.now(tz= pytz.timezone('CET')),
                                  recordLastUpdate   = datetime.now(tz= pytz.timezone('CET')),
                                  code = codes['departement'],
                                  name = codes['departement_name'],
                                  nameOrigin = codes['nom'],
                                  departementCode = codes['departement'],
                                  departementName = codes['departement_name']).save()

        region = getRegion(codes)
        if region is not None:
            if departement is not None:
                departement.region.connect(region)
                departement.save()

    return departement


# In[16]:


@logger.catch
def getRegion(codes: dict) -> Region:
    """
    - regionCode : code, sous forme String, selon dénomination :
        - INSEE : 1, 2, 3, 4, 6, 11, 24, 27, 28, 32, 44, 52, 53, 75, 76, 84, 93, 94
        - ISO : FR-xy[z]
        - extension du code ISO après le 'FR-'
        - valeurs 'custom' au cas par cas

    Cette fonction crée également le noeud de niveau supérieur (Country) si non existant.
    retourne un objet Region.
    """
    # récupérer le noeud Region ou créer le noeud Region si non existant
    if codes['region'] == '':
        logger.trace("Code région non fourni : {}", codes)
        return None

    region = None
    try:
        region = Region.nodes.get(regionCode = codes['region'])

    except DoesNotExist:
        # région pas encore créée
        logger.trace("Création Région {} {}", codes['region'], codes['region_name'])

        region = Region(modelVersion = modelVersion,
                        recordCreationDate = datetime.now(tz= pytz.timezone('CET')),
                        recordLastUpdate   = datetime.now(tz= pytz.timezone('CET')),
                        code = codes['region'],
                        name = codes['region_name'],
                        nameOrigin = codes['nom'],
                        regionCode = codes['region'],
                        regionName = codes['region_name']).save()

        country = getCountry(codes)     # par défaut on cherche en format ISO2
        if country is not None:
            if region is not None:
                region.country.connect(country)        
                region.save()

    return region


# In[17]:


@logger.catch
def getCountry(codes: dict, codeFormat: str = 'ISO2') -> Country:
    """
    - countryCode : code pays au format codeFormat (par défaut ISO2, autres valeurs 'ISO3', 'NUM')

    retourne un objet Country.
    """
    countryCode = codes['country']
    if countryCode == '':
        logger.trace("Code pays non fourni : {}", codes)
        countryCode = 'FR'   # valeur par défaut
    
    # récupérer le noeud Country ou créer le noeud Country si non existant
    country = None
    try:
        country = Country.nodes.get(countryCode = countryCode)
        
    except DoesNotExist:
            # récupérer le nom du pays sur la base du code pays
        try:
            if isinstance(session['df_countries'], pd.DataFrame): pass
        except KeyError:
            # charger le df pays
            session['df_countries'] = loadCountries(session['countries'], codeFormat)

        try:
            countryName = str(session['df_countries'].loc[countryCode, 'LIBCOG'])

        except KeyError:
            logger.trace("Non de pays pas trouvé : {}", codes)
            countryName = 'unknown'

        logger.trace("Création Pays {} {}", countryCode, countryName)
        country = Country(modelVersion = modelVersion,
                          recordCreationDate = datetime.now(tz= pytz.timezone('CET')),
                          recordLastUpdate   = datetime.now(tz= pytz.timezone('CET')),
                          codeFormat = codeFormat,
                          code = countryCode,
                          name = countryName,
                          nameOrigin = codes['nom'],
                          countryCode = countryCode,
                          countryName = countryName
                         ).save()

    return country

