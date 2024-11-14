#!/usr/bin/env python
# coding: utf-8

# In[ ]:


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
from datetime import date
from dateutil import parser

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


# In[ ]:


if '__file__' not in globals():
    sys.path.append(str(Path().absolute().parent))
from modules.config import session
from modules.neomodel_classes import *
from modules.location_loaders import *
from modules.location_constructors import getCity, getRegionFromDepartement, getArrondissement, getCanton, getDepartement, getRegion, getCountry


# In[ ]:


def getDeliveryLocation(codes: dict) -> tuple[dict, (LocationNode | Cedex | City | Region | Country)]:
    """
    codes = {'code':  code du lieu, selon codeType,
             'type': type de code, selon nomenclature DECP, transcrit en caractères ASCII minuscules
                     'code pays', 'code region', 'code departement', 'code canton', 'code arrondissement', 'code commune' 
             'nom': nom du lieu, interprété selon typeCode,
             'final': '', 'subtype': '', 'country': '', 'region': '', 'departement': '', 'commune': '', 'postal': '',
             'cedex': '', 'canton': '', 'arrondissement': ''}
    - subcode  : 'iso', 'nuts', 'custom', ...

    renvoie un objet d'une sous-classe de LocationNode
    """
    # traitement des cas particuliers liées à une catégorisation inadéquate des codes
    codes = getLocationCode(codes)

    try:
        locationNode = LocationNode.nodes.get(code= codes[codes['final']], codeType= codes['type'], name= codes['nom'])

    # + 'canton', 'arrondissement', 'departement' ?
    except DoesNotExist:
        match codes['final']:
            case 'postal' | 'commune' | 'cedex':
                locationNode = getCity(codes)
            case 'canton':
                locationNode = getCanton(codes)
            case 'arrondissement':
                locationNode = getArrondissement(codes)
            case 'departement':
                locationNode = getDepartement(codes)
            case 'region':
                locationNode = getRegion(codes)
            case 'country':
                locationNode = getCountry(codes)
            case other:
                locationNode = None

    return codes, locationNode


# In[ ]:


def getLocationCode(codes:dict) -> dict:
    """
    Renvoie les codes pays, region, departement, commune dans un dictionnaire :
    codes = {'code': code, 'final': '', 'type': codeType, 'subtype': '', 'country': '', 'region': '', 'departement': '',
             'commune': '', 'postal': '', 'cedex': ''}

    """
    # on procède prioritairement en fonction du type de code (cas général, où le decp est correctement renseigné)
    # dans le cas où le code n'est pas identifié on retourne codes['final'] = ''
    codeType = codes['type']
    if codeType == 'code region':
        codes = checkRegionCode(codes)

    elif codeType == 'code pays':
        codes = checkCountryCode(codes)
    
    elif codeType == 'code departement':
        codes = checkDepartementCode(codes)
    
    elif codeType == 'code arrondissement':
        codes = checkArrondissementCode(codes)
    
    elif codeType == 'code commune':
        codes = checkCommuneCode(codes)
    
    elif codeType == 'code postal':
        codes = checkPostalCode(codes)
    
    elif codeType == 'code canton':
        codes = checkCantonCode(codes)
    
    if codes['final'] == '':
        codes = checkRegionCode(codes)
        # if codes['subtype'] not in ['insee', 'nuts', 'iso', 'custom']: # région pas trouvée. code pays ?
        if codes['final'] == '':
            codes = checkCountryCode(codes)
            # if codes['subtype'] not in ['CODEISO2', 'CODEISO3', 'CODENUM3', 'LIBCOG', 'ANCNOM', 'LIBENR']:
            if codes['final'] == '':
                codes  = checkDepartementCode(codes)
                # if codes['subtype'] != 'departement':
                if codes['final'] == '':
                    codes = checkCantonCode(codes)
                    # if codes['subtype'] != 'canton':
                    if codes['final'] == '':
                        codes  = checkArrondissementCode(codes)
                        # if codes['subtype'] != 'arrondissement':
                        if codes['final'] == '':
                            codes = checkCommuneOrPostalCode(codes)
                            # if codes['subtype'] not in ['commune', 'postal', 'cedex']:
                            if codes['final'] == '':
                                # rien d'identifié, on pointe par défaut sur la région '0'
                                codes['type'] = 'code region'
                                codes['subtype'] = 'default'
                                codes['region'] = '0'
                                codes['final'] = 'region'

    # au bout du compte absolument rien d'identifié. on pointe par défaut sur la région '0'
    if codes['final'] == '':
        print(codes)

    return codes


def checkCountryCode(codes: dict) -> dict:
    """
    Vérifie si le code est présent dans la table 'countries' sous l'une des formes:
    CODEISO2, CODEISO3, CODENUM3, LIBCOG
    La table countries est chargée par défaut avec CODEISO2 en index.
    On retourne uniquement dans codes['code'] le code ISO2.
    codes = {'code': code, 'final': '', 'type': codeType, 'subtype': '', 'country': '', 'region': '', 'departement': '',
             'commune': '', 'postal': '', 'cedex': ''}

    """
    try:
        if isinstance(session['df_countries'], pd.DataFrame): pass

    except KeyError:
        session['df_countries'] = loadCountries(session['countries'])

    codes['type'] = 'code pays'
    codes['final'] = 'country'
    codes['subtype'] = 'CODEISO2'

    if codes['code'] in session['df_countries'].index: pass
    elif unidecode(codes['code'].upper()) in session['df_countries'].LIBCOG.values:
        codes['country'] = \
        session['df_countries'][session['df_countries'].LIBCOG == unidecode(codes['code'].upper())].index[0]
        codes['subtype'] = 'LIBCOG'
    elif unidecode(codes['code'].upper()) in session['df_countries'].ANCNOM.values:
        codes['country'] = \
        session['df_countries'][session['df_countries'].ANCNOM == unidecode(codes['code'].upper())].index[0]
        codes['subtype'] = 'ANCNOM'
    elif codes['code'].upper() in session['df_countries'].LIBENR.values:
        codes['country'] = session['df_countries'][session['df_countries'].LIBENR == codes['code'].upper()].index[0]
        codes['subtype'] = 'LIBENR'
    elif codes['code'].upper() in session['df_countries'].CODEISO3.values:
        codes['country'] = session['df_countries'][session['df_countries'].CODEISO3 == codes['code'].upper()].index[0]
        codes['subtype'] = 'CODEISO3'
    elif codes['code'].upper() in session['df_countries'].CODENUM3.values:
        codes['country'] = session['df_countries'][session['df_countries'].CODENUM3 == codes['code'].upper()].index[0]
        codes['subtype'] = 'CODENUM3'
    elif codes['code'].upper() in session['df_countries'].COG.values:
        codes['country'] = session['df_countries'][session['df_countries'].COG == codes['code'].upper()].index[0]
        codes['subtype'] = 'LIBCOG'
    else:
        codes['country'] = 'FR'       # par défaut on considère que c'est un code en France
        codes['type'] = 'code region' # on recherchera au niveau régional
        codes['region'] = '0'
        codes['subtype'] = 'unknown'
        codes['final'] = ''

    return codes

def checkRegionCode(codes: dict) -> dict:
    """
    codes = {'code': code, 'final': '', 'type': codeType, 'subtype': '', 'country': '', 'region': '', 'departement': '',
             'commune': '', 'postal': '', 'cedex': ''}

    """
    # d'abord récupérer le nom de la région sur la base du code région
    try:
        if isinstance(session['df_regions'], pd.DataFrame): pass
    except KeyError:
        session['df_regions'] = loadRegions(session['regions'])
        
    if codes['code'] in session['df_regions'].index:
        codes['region'] = codes['code']
        codes['final'] = 'region'
        codes['region_name'] = str(session['df_regions'].loc[codes['code'], 'LIBELLE'])
        codes['subtype'] ='insee'
    else:
        # code NUTS ?
        codes = checkNutsCode(codes)
        if codes['final'] == '':
            codes  = checkIsoCode(codes) # code ISO 3166 ?
            if codes['final'] == '':
                codes = checkCustomCode(codes)
                if codes['final'] == '':
                    codes['region'] = ''
                    codes['region_name'] = ''
        
    return codes

def checkDepartementCode(codes: dict) -> dict:
    """
    en pratique on retrouve sous le type 'code departement' tous les types de localisations : pays, région, 
    département, commune.
    codes = {'code': code, 'final': '', 'type': codeType, 'subtype': '', 'country': '', 'region': '', 'departement': '',
             'commune': '', 'postal': '', 'cedex': ''}
    On traite également les communautés d'outre mer en tant qu'entités département, associés à des types de régions
    """
    codes['final'] = ''

    if len(codes['code']) == 0:
        return codes
    
    if len(codes['code']) == 1:
        codes['code'] = codes['code'].zfill(2)
        
    if codes['code'].isnumeric():
        # probablement numéro département français
        if len(codes['code']) <= 3:
            code = codes['code'][0:3] if codes['code'][0:2] in ['97', '98'] else codes['code'][0:2]
        else:
            # probablement code commune ou postal ou cedex
            code = codes['code']
    
    else:
        if codes['code'] in ['2A', '2B']: # Corse
            code = codes['code']
        elif codes['code'][0:3] in ['DEP', 'DPT']: # mentions de la forme 'DPT07', 'DEP06'
            codes['code'] = codes['code'][3:]
            if codes['code'].isnumeric():
                if len(codes['code']) == 1:
                    code = codes['code'].zfill(2)
                if len(codes['code']) <= 3:
                    code = codes['code'][0:3] if codes['code'][0:2] in ['97', '98'] else codes['code'][0:2]
                else:
                    code = codes['code']
            else:
                code = codes['code']
        else:
            code = codes['code']

    if code == '20': # ancien code départemental pour la Corse:
        code = '2A'  # whatever
    

    #  numéro département français ?
    try:
        if isinstance(session['df_departements'], pd.DataFrame):
            if isinstance(session['df_regions'], pd.DataFrame):
                if isinstance(session['df_comer'], pd.DataFrame):
                    if isinstance(session['df_communes_comer'], pd.DataFrame):
                        pass
    except KeyError:
        session['df_departements'] = loadDepartements(session['departements'])
        session['df_regions'] = loadRegions(session['regions'])
        session['df_comer'] = loadComer(session['comer'], index= 'COMER')
        session['df_communes_comer'] = loadComer(session['communes_comer'], index= 'COM_COMER')

    if code in session['df_departements'].index:
        codes['departement'] = code
        codes['departement_name'] = session['df_departements'].loc[code, 'NCC']
        codes['region'] = session['df_departements'].loc[code, 'REG']
        codes['region_name'] = session['df_regions'].loc[codes['region'], 'NCC']
        codes['final'] = 'departement'
        codes['subtype'] = 'departement'

    elif code in session['df_comer'].index:
        codes['departement'] = code
        codes['departement_name'] = session['df_comer'].loc[code, 'NCC']
        try:
            codes['region'] = session['df_communes_comer'][session['df_communes_comer'].COMER == code].NATURE_ZONAGE.values[0]
            codes['region_name'] = codes['region']
        except (KeyError, IndexError):
            codes['region'] = ''
            codes['region_name'] = ''

        codes['final'] = 'departement'
        codes['subtype'] = 'comer'
        
    elif len(code) >= 5:
        if code[0:2].upper() == 'FR':
            if code[2] == '-':
                codes = checkIsoCode(codes)
            else:
                codes = checkNutsCode(codes)
    else:
        for ix in ['final', 'subtype', 'departement', 'departement_name', 'region', 'region_name']:
                codes[ix] = ''

    return codes


def isCommuneCodeFormat(codes: dict) -> bool:
    """
    Renvoie False si longueur de codes['code'] != 5
    si longueur = 5 : True si code commune Corse, True si code numérique
    """
    ret = False
    code = codes['code']
    if len(code) == 5:
        if code[0:2] in ['2A', '2B']: # Corse
            if code[2:].isnumeric():
                ret = True
        elif code.isnumeric():
            ret = True
    
    return ret


def checkNutsCode(codes: dict) -> dict:
    """
    vérifie si le code est un code NUTS3 (5 digits) ou NUTS 2 (4 digits) (de la forme FRxy[z] pour la France)
    la classification NUTS renvoie une identification au niveau département, que l'on convertit ici au niveau région
    En entrée le code à contrôler est dans l'entrée 'code' du dictionnaire codes
    Renvoie les codes pays, region, departement, commune dans un dictionnaire :
    codes = {'code': code, 'final': '', 'type': codeType, 'subtype': '', 'country': '', 'region': '', 'departement': '',
             'commune': '', 'postal': '', 'cedex': ''}
    """
    if len(codes['code']) < 3:
        # sinon code = code pays et risque de confusion
        return codes

    codeFound = '0'

    try:
        if isinstance(session['df_nuts'], pd.DataFrame):
            if isinstance(session['df_regions'], pd.DataFrame):
                if isinstance(session['df_departements'], pd.DataFrame):
                    pass
    
    except KeyError:
        session['df_nuts'] = loadNuts(session['nuts'])
        session['df_regions'] = loadRegions(session['regions'])
        session['df_departements'] = loadDepartements(session['departements'])
    
    try:
        # on ne dispose que d'une table NUTS3 avec des codes sur 5 caractères. on cherche un match sur les premiers
        # et on extrait uniquement la première valeur, pour en déduire un code région.
        codeFound = session['df_nuts'].loc[session['df_nuts'].index.str.startswith(codes['code'].upper()), 'CODE'].values[0]
        codes['subtype'] = 'nuts'
        codes['code'] = codes['code'].upper()
        codes['country'] = codes['code'][0:2]
        # codeFound contient le premier numéro de commune de la table NUTS, on en extrait le seul élément significatif
        #i.e. le numéro de département (si longueur = 5) ou la région d'appartenance du premier département
        # correspondant au code (si longueur < 5).
        if len(codes['code']) == 5:
            codes['departement'] = codeFound[0:3] if codeFound[0:2] in ['97', '98'] else codeFound[0:2]
            codes['type'] = 'code departement'
            codesDep = checkDepartementCode({'code': codes['departement']})
            for ix in ['final', 'subtype', 'departement', 'departement_name', 'region', 'region_name']:
                codes[ix] = codesDep[ix]
                
        else:
            codes['final'] = 'region'
            codes['type'] = 'code region'

            codes['region'], codes['region_name'] = getRegionFromDepartement(codeFound)            

    except (KeyError, IndexError):
        # code NUTS inconnu
        # on indique que pas trouvé
        codes['subtype'] = 'unknown'
        codes['final'] = ''
        codes['country'] = 'FR' # valeur par défaut

    # dans tous les cas on efface les entrées cedex et commune.
    codes['cedex'] = ''
    codes['commune'] = ''
    codes['postal'] = ''

    return codes


def checkIsoCode(codes: dict) -> dict:
    """
    vérifie si le code est un code ISO 3166 (de la forme FR-xy[z] pour la France)
    la classification ISO renvoie une identification au niveau région Insee dans le champ region_insee.
    En entrée le code à contrôler est dans l'entrée 'code' du dictionnaire codes
    Renvoie les codes pays, region, departement, commune dans un dictionnaire :
    codes = {'code': code, 'final': '', 'type': codeType, 'country': '', 'region': '', 'departement': '',
             'commune': '', 'postal': ''}
    """

    try:
        if isinstance(session['df_iso_fr_regions'], pd.DataFrame):
            if isinstance(session['df_regions'], pd.DataFrame): pass
    
    except KeyError: # dataframe pas encore chargé
        session['df_iso_fr_regions'] = loadIso3166(session['iso_fr_regions'])
        session['df_regions'] = loadRegions(session['regions'])
        
    try:
        code = session['df_iso_fr_regions'].loc[codes['code'].upper(), 'region_insee']
        isoType = session['df_iso_fr_regions'].loc[codes['code'].upper(), 'Type']

        codes['code'] = codes['code'].upper()
        codes['departement_name'] = ''
        
        match isoType:
            case 'departement metropolitain':
                codes['region'] = session['df_iso_fr_regions'].loc[codes['code'], 'region_insee'].zfill(2)
                codes['departement'] = session['df_iso_fr_regions'].loc[codes['code'], 'code_insee']
                codes['departement'] = codes['departement'].zfill(max(2, len(codes['departement'])))
                codes['region_name'] = session['df_regions'].loc[codes['region'], 'NCC']
            case 'collectivite europeenne':
                codes['region'] = session['df_iso_fr_regions'].loc[codes['code'], 'region_insee'].zfill(2)
                codes['region_name'] = session['df_regions'].loc[codes['region'], 'NCC']
                codes['departement'] = ''
            case 'region metropolitaine':
                codes['region'] = session['df_iso_fr_regions'].loc[codes['code'], 'region_insee'].zfill(2)
                codes['region_name'] = session['df_regions'].loc[codes['region'], 'NCC']
                codes['departement'] = ''
            case 'dependance':
                codes['region'] = codes['code']
                codes['region_name'] = session['df_iso_fr_regions'].loc[codes['code'], 'Nom']
                codes['departement'] = ''
            case 'collectivite metropolitaine a statut particulier':
                codes['region'] = session['df_iso_fr_regions'].loc[codes['code'], 'region_insee'].zfill(2)
                codes['region_name'] = session['df_regions'].loc[codes['region'], 'NCC']
                codes['departement'] = ''
            case "collectivite departementale d'outre-mer" | "collectivite territoriale unique d'outre-mer":
                codes['region'] = session['df_iso_fr_regions'].loc[codes['code'], 'region_insee'].zfill(2)
                codes['departement'] = session['df_iso_fr_regions'].loc[codes['code'], 'code_insee']
                codes['departement'] = codes['departement'].zfill(max(2, len(codes['departement'])))
                codes['region_name'] = session['df_regions'].loc[codes['region'], 'NCC']
            case "collectivite d'outre-mer" | "collectivite d’outre-mer a statut particulier" | "territoire d'outre-mer":
                codes['region'] = codes['code']
                codes['region_name'] = session['df_iso_fr_regions'].loc[codes['code'], 'Nom']
                codes['departement'] = session['df_iso_fr_regions'].loc[codes['code'], 'code_insee'] # patch dans source
                codes['departement'] = codes['departement'].zfill(max(2, len(codes['departement'])))

            case _:
                print('XXXXXXXXXX type non trouvé XXXXXXXX', isoType, codes['code'])
                
        
        if isoType in ["departement metropolitain", "collectivite departementale d'outre-mer",
                       "collectivite territoriale unique d'outre-mer", "collectivite d'outre-mer",
                         "collectivite d’outre-mer a statut particulier", "territoire d'outre-mer"]:
            codesDep = checkDepartementCode({'code': codes['departement']})
            for ix in ['final', 'subtype', 'departement', 'departement_name', 'region', 'region_name']:
                codes[ix] = codesDep[ix]

        else:
            codes['type'] = 'code region'
            codes['final'] = 'region'
        
        codes['subtype'] = 'iso'
        codes['country'] = codes['code'][0:2]
        
    except KeyError:
        # le code n'est pas ISO 3166-2
        # on indique que pas trouvé
        codes['subtype'] = 'unknown'
        codes['final'] = ''
        codes['country'] = 'FR' # valeur par défaut

    # dans tous les cas on efface les entrées cedex et commune.
    codes['cedex'] = ''
    codes['commune'] = ''
    codes['postal'] = ''

    return codes


def checkCommuneOrPostalCode(codes: dict) -> dict:
    """
    Si le code n'est pas dans un format vraisemblable on ne fait rien. 
    """
    if isCommuneCodeFormat(codes):
        try:
            if isinstance(session['commune2post'], pd.DataFrame):
                if isinstance(session['post2commune'], pd.DataFrame):
                    if isinstance(session['df_communes'], pd.DataFrame):
                        pass
        except KeyError:
            # dataframes pas chargé, chargement
            session['post2commune'] = loadPost2Commune(session['postcodes'])
            session['commune2post'] = loadCommune2Post(session['postcodes'])
            session['df_communes'] = loadCommunes(session['communes'])

        codeType = codes['type']
        if codeType == 'code commune':
            # on vérifie d'abord le code commune, puis ensuite en cas d'échec s'il s'agit d'un code postal
            codes = checkCommuneCode(codes)
    
        elif codeType == 'code postal':
            # on vérifie d'abord le code postal, puis ensuite en cas d'échec s'il s'agit d'un code commune
            codes = checkPostalCode(codes)
            
        elif codeType in ['code departement', 'code region', 'code pays', 'code canton', 'code arrondissement']:
            # cas où le type de code est incorrectement renseigné dans le dataframe decp
            codes = checkCommuneCode(codes)
            if codes['final'] not in ['commune', 'cedex']:
                codes = checkPostalCode(codes)
                
        else:
            # pas prévu
            codes['cedex'] = ''
            codes['postal'] = ''
            codes['commune'] = ''
            codes['final'] = ''
            codes['subtype'] = 'unknown'

    return codes
    

def checkCommuneCode(codes: dict) -> dict:
    """
    """
    try:
        if isinstance(session['commune2post'], pd.DataFrame):
            if isinstance(session['post2commune'], pd.DataFrame):
                if isinstance(session['df_communes'], pd.DataFrame):
                    if isinstance(session['df_departements'], pd.DataFrame):
                        if isinstance(session['df_regions'], pd.DataFrame):
                            if isinstance(session['df_comer'], pd.DataFrame):
                                if isinstance(session['df_communes_comer'], pd.DataFrame):
                                    pass

    except KeyError:
        # dataframes pas chargé, chargement
        session['post2commune'] = loadPost2Commune(session['postcodes'])
        session['commune2post'] = loadCommune2Post(session['postcodes'])
        session['df_communes'] = loadCommunes(session['communes'])
        session['df_departements'] = loadDepartements(session['departements'])
        session['df_regions'] = loadRegions(session['regions'])
        session['df_comer'] = loadComer(session['comer'], index= 'COMER')
        session['df_communes_comer'] = loadComer(session['communes_comer'], index= 'COM_COMER')

    
    if isCommuneCodeFormat(codes):
        try:
            codes['postal'] = session['commune2post'].loc[codes['code'], 'Code_postal']
            codes['commune'] = codes['code']
            codes['commune_name'] = session['commune2post'].loc[codes['code'], 'Nom_de_la_commune']
            codes['final'] = 'commune'
            codes['type'] = 'code commune'
            codes['subtype'] = 'commune'
    
        except KeyError:
            # le code commune n'est pas présent dans la base des codes postaux...
            # prendre en compte le cas des collectivités d'outre-mer
            source = 'communes_comer' if codes['code'] in session['df_communes_comer'].index else 'communes'

            if codes['code'] in session['df_' + source].index:
                # ... mais il est présent dans la table communes ou bien communes_comer
                codes['commune'] = codes['code']
                codes['commune_name'] = session['df_' + source].loc[codes['commune'], 'NCC']
                codes['postal'] = ''
                codes['final'] = 'commune'
                codes['type'] = 'code commune'
                codes['subtype'] = 'commune'
            else:
                if codes['code'] in session['post2commune'].index:
                    # le code est en fait un code postal
                    # on va retourner le code commune avec le type 'code commune'
                    codes['commune'] = session['post2commune'].loc[codes['code'], '#Code_commune_INSEE']
                    codes['commune_name'] = session['post2commune'].loc[codes['code'], 'Nom_de_la_commune']
                    codes['postal'] = codes['code']
                    codes['final'] = 'commune'
                    codes['type'] = 'code commune'
                    codes['subtype'] = 'postal'

                else:
                    # le code n'est ni dans la base commune ni dans la base des codes postaux
                    # on considère que c'est un code cedex
                    codes['type'] = 'cedex'
                    codes['cedex'] = codes['code']
                    codes['postal'] = ''
                    codes['commune'] = ''
                    codes['commune_name'] = ''
                    codes['subtype'] = 'cedex'
                    codes['final'] = 'cedex'
        
        codes['departement'] = codes[codes['final']][0:3] if codes[codes['final']][0:2] in ['97', '98'] else \
        codes[codes['final']][0:2]

        codesDep = checkDepartementCode({'code': codes['departement']})
        for ix in ['departement', 'departement_name', 'region', 'region_name']:
            codes[ix] = codesDep[ix]

    return codes


def checkPostalCode(codes: dict) -> dict:
    """
    """
    if isCommuneCodeFormat(codes):
        try:
            if isinstance(session['commune2post'], pd.DataFrame):
                if isinstance(session['post2commune'], pd.DataFrame):
                    if isinstance(session['df_communes'], pd.DataFrame):
                        if isinstance(session['df_departements'], pd.DataFrame):
                            if isinstance(session['df_regions'], pd.DataFrame):
                                if isinstance(session['df_comer'], pd.DataFrame):
                                    if isinstance(session['df_communes_comer'], pd.DataFrame):
                                        pass
        except KeyError:
            # dataframes pas chargé, chargement
            session['post2commune'] = loadPost2Commune(session['postcodes'])
            session['commune2post'] = loadCommune2Post(session['postcodes'])
            session['df_communes'] = loadCommunes(session['communes'])
            session['df_departements'] = loadDepartements(session['departements'])
            session['df_regions'] = loadRegions(session['regions'])
            session['df_comer'] = loadComer(session['comer'], index= 'COMER')
            session['df_communes_comer'] = loadComer(session['communes_comer'], index= 'COM_COMER')

        try:
            codes['commune'] = session['post2commune'].loc[codes['code'], '#Code_commune_INSEE']
            try:
                # prendre en compte le cas des collectivités d'outre-mer
                source = 'communes_comer' if codes['commune'] in session['df_communes_comer'].index else 'communes'
                codes['commune_name'] = session['df_' + source].loc[codes['commune'], 'NCC']
            except KeyError:
                codes['commune_name'] = ''
                
            codes['postal'] = codes['code']
            codes['final'] = 'commune'
            codes['subtype'] = 'commune'
            codes['type'] = 'code commune'
    
        except KeyError:
            # le code n'est pas dans la base des codes postaux
            # on regarde s'il ne s'agit pas en fait d'un code commune
            # prendre en compte le cas des collectivités d'outre-mer
            try:
                source = 'communes_comer' if codes['code'] in session['df_comer'].index else 'communes'
                codes['commune_name'] = session['df_' + source].loc[codes['commune'], 'NCC']
                # c'est un code commune
                codes['commune'] = codes['code']
                codes['final'] = 'commune'
                codes['subtype'] = 'commune'
                codes['type'] = 'code commune'
                try:
                    # la base des codes postaux contient également les communautés d'Outre-Mer
                    codes['postal'] = session['commune2post'].loc[codes['commune'], 'Code_postal']
                except KeyError:
                    codes['postal'] =''
            except KeyError:
                # on considère qu'il s'agit d'un cedex
                codes['type'] = 'cedex'
                codes['cedex'] = codes['code']
                codes['postal'] = ''
                codes['commune'] = ''
                codes['commune_name'] = ''
                codes['subtype'] = 'cedex'
                codes['final'] = 'cedex'

        codes['departement'] = codes[codes['final']][0:3] if codes[codes['final']][0:2] in ['97', '98'] else \
        codes[codes['final']][0:2]
            
        if codes['departement'] == '20':
            # Corse
            codes['departement'] = '2A' # à affiner ultérieurement avec lieuExecution.nom

        codesDep = checkDepartementCode({'code': codes['departement']})
        for ix in ['departement', 'departement_name', 'region', 'region_name']:
            codes[ix] = codesDep[ix]

    return codes

def checkCustomCode(codes: dict) -> dict:
    """
    vérifie si le code est un spécification non normalisée.
    En entrée le code à contrôler est dans l'entrée 'code' du dictionnaire codes
    Renvoie les codes pays, region, departement, commune dans un dictionnaire :
    codes = {'code': code, 'final': '', 'type': codeType, 'country': '', 'region': '', 'departement': '',
             'commune': '', 'postal': ''}
    """
    codes['subtype'] = 'custom' # soyons optimistes
    codes['final'] = ''

    try:
        if isinstance(session['df_regions'], pd.DataFrame): pass
    except KeyError: # dataframe pas encore chargé
        session['df_regions'] = loadRegions(session['regions'])

    regions1 = session['df_regions'].NCC.to_numpy()
    regions2 = session['df_regions'].NCCENR.to_numpy()
    regions3 = session['df_regions'].LIBELLE.to_numpy()
    
    if unidecode(codes['code']) in regions1:
        # le code est un nom de région, sous la forme NCC. L'index donne le code région Insee        
        codes['region'] = session['df_regions'][session['df_regions'].NCC == unidecode(codes['code'])].index.values[0]
        codes['region_name'] = session['df_regions'].loc[codes['region'], 'NCC']
        codes['final'] = 'region'


    elif codes['code'] in regions2:
        # le code est sous sa forme NCCENR en caractères unicode
        codes['region'] = session['df_regions'][session['df_regions'].NCCENR == codes['code']].index.values[0]
        codes['region_name'] = session['df_regions'].loc[codes['region'], 'NCC']
        codes['final'] = 'region'
    
    elif codes['code'] in regions3:
        # le code est sous sa forme LIBELLE en caractères unicode
        codes['region'] = session['df_regions'][session['df_regions'].LIBELLE == codes['code']].index.values[0]
        codes['region_name'] = session['df_regions'].loc[codes['region'], 'NCC']
        codes['final'] = 'region'
    
    else:
        # on regarde si le code n'est pas un suffixe d'une codification ISO (ex: 'HDF' pour 'FR-HDF') 
        codes['code'] = 'FR-' + codes['code']
        codes = checkIsoCode(codes)
        if codes['final'] == '':
            # le code reconstitué n'est pas ISO 3166-2, on remet l'ancien
            codes['code'] = codes['code'][3:]
            # et on regarde si ce n'est pas un ancien nom ISO
            codeSave = codes['code']
            dfIso = session['df_iso_fr_regions']
            if codes['code'][0:3] != 'FR-':
                codes['code'] = 'FR-' + codes['code']
            if codes['code'] in dfIso.Ancien[dfIso.Ancien.notna()].values: # yess !
                codes['code'] = dfIso.loc[dfIso.Ancien == codes['code']].index.values[0] # on met nouveau code ISO
                codes = checkIsoCode(codes)
            else:
                # le code reconstitué n'est pas ISO 3166-2, on remet l'ancien
                codes['code'] = codeSave
            if codes['final'] == '':
                # on regarde si ce n'est pas le nom d'une commune dans la base communes, ou sinon dans la base codes postaux
                codes['final'] = 'commune'  # soyons optimistes
                try:
                    if isinstance(session['df_communes'], pd.DataFrame): pass
                except KeyError:
                    session['df_communes'] = loadCommunes(session['communes'])
                    
                if codes['code'].upper() in session['df_communes'].NCC.values:
                    codes['commune'] = session['df_communes'][session['df_communes'].NCC == codes['code'].upper()].index[0]

                elif codes['code'] in session['df_communes'].NCCENR.values:
                    codes['commune'] = session['df_communes'][session['df_communes'].NCCENR == codes['code']].index[0]
    
                elif codes['code'] in session['df_communes'].LIBELLE.values:
                    codes['commune'] = session['df_communes'][session['df_communes'].LIBELLE == codes['code']].index[0]
                
                else:
                    # on regarde dans la base des codes postaux, indexée suivant le code commune
                    # car 1 code postal renvoie souvent à plusieurs communes
                    try:
                        if isinstance(session['df_commune2post'], pd.DataFrame): pass
                    except KeyError:
                        session['df_commune2post'] = loadCommune2Post(session['postcodes'])
    
                    if codes['code'].upper() in session['df_commune2post'].Nom_de_la_commune.values:
                        codes['commune'] = \
                            session['df_commune2post'][session['df_commune2post'].Nom_de_la_commune == codes['code'].upper()].index[0]
                    else:
                        # ce n'est pas un nom de commune
                        # par défaut on prend le numéro de commune de l'acheteur
                        if isCommuneCodeFormat({'code': codes['communeBuyer']}):
                            codes['code'] = codes['communeBuyer']
                            codes = checkCommuneCode(codes)
                        else:
                            # on indique qu'aucun résultat n'a été trouvé 
                            codes['final'] = ''
                            codes['subtype'] = ''
                            codes['country'] = 'FR' # valeur par défaut
    
                if codes['final'] in ['commune', 'postal']: # on a trouvé un nom correspondant, quelque part
                    try:
                        if isinstance(session['df_commune2post'], pd.DataFrame): pass
                    except KeyError:
                        session['df_commune2post'] = loadCommune2Post(session['postcodes'])
    
                    try:
                        codes['postal'] = session['df_commune2post'].loc[codes['commune'], 'Code_postal']
                    except KeyError:
                        codes['postal'] = ''
    
                    # on récupère les informations de département et région
                    codeDep = codes['commune'][0:3] if codes['commune'][0:2] in ['97', '98'] else codes['commune'][0:2]
                    codesDep = checkDepartementCode({'code': codeDep})
                    for ix in ['departement', 'departement_name', 'region', 'region_name']:
                        codes[ix] = codesDep[ix]

                    # on récupère le reste des informations de la commune
                    codesCommune = checkCommuneCode({'code': codes['commune']})
                    for ix in ['commune_name', 'type', 'subtype']:
                        codes[ix] = codesCommune[ix] 

    # dans tous les cas on efface l'entrée cedex.
    codes['cedex'] = ''

    return codes


def checkArrondissementCode(codes: dict) -> dict:
    """
    """
    ok = False
    if len(codes['code']) == 4:
        if codes['code'][0:2] in ['97', '98']:
            if codes['code'][2:].isnumeric():
                ok = True
    else:
        if len(codes['code']) == 3:
            if codes['code'][0:2] in ['2A', '2B']:
                if codes['code'][2:].isnumeric():
                    ok = True
            else:
                if codes['code'].isnumeric():
                    ok = True

    if ok:
        try:
            if isinstance(session['df_arrondissements'], pd.DataFrame):
                if isinstance(session['df_regions'], pd.DataFrame):
                    if isinstance(session['df_departements'], pd.DataFrame): pass
                    
        except KeyError:
            session['df_arrondissements'] = loadArrondissements(session['arrondissements'])
            session['df_regions'] = loadRegions(session['regions'])
            session['df_departements'] = loadDepartements(session['departements'])
            
        try:
            codes['departement'] = session['df_arrondissements'].loc[codes['code'], 'DEP']
            codes['region'] = session['df_arrondissements'].loc[codes['code'], 'REG']
            codes['arrondissement_name'] = session['df_arrondissements'].loc[codes['code'], 'NCC']
            codes['arrondissement'] = codes['code']
            codes['final'] = 'arrondissement'
            codes['type'] = 'code arrondissement'
            codes['subtype'] = 'arrondissement'
            # pas d'arrondissement dans les comer donc a priori on ne teste pas sur df_comer
            try:
                codes['departement_name'] = session['df_departements'].loc[codes['departement'], 'NCC']
                try:
                  codes['region_name'] = session['df_regions'].loc[codes['region'], 'NCC']
                except KeyError:
                    codes['region_name'] = ''
            except KeyError:
                codes['departement_name'] = ''
            
        except KeyError:
            for ix in ['final', 'subtype', 'departement', 'departement_name', 'region', 'region_name',
                       'canton_name', 'canton']:
                codes[ix] = ''
        
    else:
        codes['final'] = ''
        
    return codes


def checkCantonCode(codes: dict) -> dict:
    """
    code canton sur 4 digits, ou 5 pour les DROM commençant par 97
    """
    ok = False
    if len(codes['code']) == 5:
        if codes['code'][0:2] in ['97', '98']:
            if codes['code'][2:].isnumeric():
                ok = True
    else:
        if len(codes['code']) == 4:
            if codes['code'][0:2] in ['2A', '2B']:
                if codes['code'][2:].isnumeric():
                    ok = True
            else:
                if codes['code'].isnumeric():
                    ok = True

    if ok:
        try:
            if isinstance(session['df_cantons'], pd.DataFrame):
                if isinstance(session['df_regions'], pd.DataFrame):
                    if isinstance(session['df_departements'], pd.DataFrame): pass
        except KeyError:
            session['df_cantons'] = loadCantons(session['cantons'])
            session['df_regions'] = loadRegions(session['regions'])
            session['df_departements'] = loadDepartements(session['departements'])
    
        try:
            codes['departement'] = session['df_cantons'].loc[codes['code'], 'DEP']
            codes['region'] = session['df_cantons'].loc[codes['code'], 'REG']
            codes['canton_name'] = session['df_cantons'].loc[codes['code'], 'NCC']
            codes['canton'] = codes['code']
            codes['final'] = 'canton'
            codes['type'] = 'code canton'
            codes['subtype'] = 'canton'
            # pas de cantons dans les comer donc a priori on ne teste pas sur df_comer
            try:
                codes['departement_name'] = session['df_departements'].loc[codes['departement'], 'NCC']
                try:
                    codes['region_name'] = session['df_regions'].loc[codes['region'], 'NCC']
                except KeyError:
                    codes['region_name'] = ''

            except KeyError:
                codes['departement_name'] = ''

        except KeyError:
            for ix in ['final', 'subtype', 'departement', 'departement_name', 'region', 'region_name',
                       'canton_name', 'canton']:
                codes[ix] = ''
    else:
        codes['final'] = ''
    
    return codes

