#!/usr/bin/env python
# coding: utf-8

# In[7]:


import pandas as pd
import numpy as np
import openpyxl
from configparser import ConfigParser, ExtendedInterpolation
import os
import sys
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


# In[8]:


def connect(session, mode):
    """
    - session : dictionary
    - mode : 'self-managed'
    """
    conf = get_config(session)
    if mode == 'self-managed':
        uri          = session['uri']
        auth_account = session["auth_account"]
        auth_pwd     = session["auth_pwd"]
        driver = GraphDatabase.driver(uri, auth=(auth_account, auth_pwd))
        config.DRIVER = driver
        db.set_connection(driver=driver)
        try:
            driver.verify_connectivity()
        except gaierror:
            print("erreur adresse ", session['uri'])
        except ValueError:
            print("erreur valeur ", session['uri'])
    else:
        pass
        # autre mode pas encore géré
    return driver


# In[4]:


def resumeInstance(session: dict):
    """
    """

    headers = {'accept': 'application/json', 'Authorization': 'Bearer %bearer_token%'}
    response = requests.post('https://api.neo4j.io/v1beta3/instances/c5606163/resume', headers=headers, verify=False)
    return response


# In[5]:


def get_config(session, config_file= 'config.ini'):
    """
    Fonction permettant d'extraire la configuration de l'environnement d'exécution (dossiers, fichiers).
    La configuration de l'environnement est personnalisée dans un fichier config_file, par défaut 'config.ini'
    qui se trouve à la racine de l'environnement du projet.
    Par convention, ce module se trouve dans un dossier 'app' qui lui-même se trouve sous la racine de l'environnement d'exécution.
    Le fichier de configuration contient 2 sections :
    - DEFAULT : des valeurs par défaut de l'environnement d'exécution.
    - LOCAL   : les valeurs spécifiques de l'environnement d'exécution.
    Dans le fichier config_file les séparateurs des chemins d'accès au fichier n'ont pas besoin
    d'être précédés par le caractère d'échappement (backslash). Le caractère '/' peut être utilisé.
    Les noms de fichier ne doivent pas être encadré par des caractères "" ou ''.
    au moins une entrée 'base_dir' doit être présente, et contenant un chemin absolu.
    Il est possible de préciser pour chaque ressource un chemin relatif à 'base_dir' ou bien un chemin absolu (qui peut
    référencer une ressource se trouvant ailleurs que dans 'base_dir').
    Il est possible de référencer une entrée quelconque du fichier de configuration dans une autre entrée, par exemple :
    ressource_2 = ${ressource_1}/dossier_2
    
    Exemple de fichier de configuration :
    
    [DEFAULT]
    base_dir = C:/Users/Utilisateur/projets/neo4j
    data_dir = data1
    model_dir = joblib1
    ressources_dir = ressources1
    streamlit_dir = streamlit1
    dataset = reviews_trust_augmented.csv

    [LOCAL]
    base_dir = C:/Users/Utilisateur/projets/py_content
    data_dir = ${base_dir}/data
    model_dir = joblib
    ressources_dir = ressources
    streamlit_dir = streamlit
    dataset = reviews_trust_complet.csv

    Paramètre :
    - session : un dictionnaire dans lequel sont enregistrées les paires clés-valeurs du fichier de configuration.
                Typiquement dans un environnement Streamlit on peut passer le paramètre st.session_state.
    - config_file : nom du fichier de configuration (chemin relatif uniquement).
    Variable en sortie :
    - objet ConfigParser
    """
    config = ConfigParser(interpolation=ExtendedInterpolation())

    # identification du dossier dans lequel se trouve le fichier de configuration
    # la variable __file__ est automatiquement créée par l'environnement python quand le module config est chargé.
    try:
        app_path = os.path.abspath(os.path.dirname(__file__))
    except (NameError, KeyError):
        # test en environnement anaconda
        print("cette fonction doit être exécutée dans un module importé.")
        app_path = 'C:/Users/Utilisateur/projets/neo4j/app'
        home_path = os.path.abspath(os.path.dirname(app_path))
        config_file = os.path.join(home_path, config_file)
    else:
        home_path = os.path.abspath(os.path.dirname(app_path))
        config_file = os.path.join(home_path, config_file)

    file_ok = config.read(config_file)
    if len(file_ok) == 0:
        print("fichier {0} absent".format(config_file))
        return None
    else:
        print("fichier de configuration {0}".format(config_file))

    # vérification de la présence des sections
    # une variable local_config est initialisée dans ce bloc
    if len(config.sections()) == 0:
        print("Pas de section autre que DEFAULT dans le fichier de configuration, valeurs par défaut assumées")
        local_config = config.defaults()
    else:
        if config.has_section('LOCAL'):
            local_config = config['LOCAL']
        else:
            print("Pas de section LOCAL dans le fichier de configuration, valeurs par défaut assumées")
            local_config = config.defaults()
        
    # on vérifie la présence de la clé 'base_dir'
    
    # mise à jour du dictionnaire passé en paramètre.
    # la présence dans le dictionnaire 'session' des clés présentes dans le fichier de configuration n'est pas testée.
    # si les mêmes clés y sont déjà présentes, leur valeur est remplacée par celles du fichier de configuration.
    for key, value in local_config.items():
        if key == 'base_dir':
            # le dossier de base doit être un chemin absolu
            # on teste l'existence du dossier
            if os.path.isabs(value):
                if os.path.exists(value) == False:
                    print("Le dossier {0} n'existe pas".format(value))
            else:
                print("Le dossier de base {0} doit être un chemin absolu".format(value))
        else:
            # le chemin indiqué peut être un chemin absolu ou relatif
            if os.path.isabs(value) == False:
                # On assume qu'un chemin relatif à 'base_dir' est indiqué
                # on construit alors le chemin complet
                value = os.path.join(local_config['base_dir'], value)
            # on teste l'existence du dossier
            if os.path.exists(value) == False:
                print("Le dossier {0} n'existe pas".format(value))
        
        session[key] = value
    if config.has_section('VARIABLES NUMERIQUES'):
        for key, value in config['VARIABLES NUMERIQUES'].items():
            if key not in config.defaults().keys():
                # on ne gère pas de valeurs par défaut pour les variables numériques
                session[key] = config.getfloat(section= 'VARIABLES NUMERIQUES', option= key)

    if config.has_section('VARIABLES TEXTE'):
        for key, value in config['VARIABLES TEXTE'].items():
            if key not in config.defaults().keys():
                # on ne gère pas de valeurs par défaut pour les variables texte
                session[key] = value

    return config


# In[6]:


def getDS(siret: str) -> str:
    """
    - siret : 14 caractères.

    retourne la dénomination sociale.
    """
    try:
        df = search_sirene(variable = ['siret'], number= 1, pattern = [siret], phonetic_search = False,
                           legal=True, closed=True)
        return df.at[0, 'denominationUniteLegale']
    
    except RequestException:
        return ''

