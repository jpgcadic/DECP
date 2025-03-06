#!/usr/bin/env python
# coding: utf-8

# In[1]:


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
from neo4j.exceptions import CypherSyntaxError 

from neomodel import config, db, install_labels, install_all_labels
from neomodel import (StructuredNode,  RelationshipTo, RelationshipFrom, StructuredRel)
from neomodel import (StringProperty, IntegerProperty, FloatProperty, ArrayProperty, BooleanProperty, DateProperty, UniqueIdProperty)
from neomodel import AliasProperty, RegexProperty, JSONProperty, DateTimeProperty, DateTimeFormatProperty
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

import json


# In[2]:


if '__file__' not in globals():
    sys.path.append(str(Path().absolute().parent))

from modules.config import *
from modules.neomodel_classes import *
from modules.location_management import getDeliveryLocation, getLocationCode
from modules.location_constructors import getCity, getRegionFromDepartement
from modules.utils import checkEnterprises
from modules.requests import getCodeInVersion


# In[3]:


def addBuyer(siretBuyer: str, buyerName: str, buyerLegalName: str):
    """
    """
    buyer = Buyer(modelVersion      = modelVersion,
                  buyerId           = siretBuyer,
                  buyerSiren        = siretBuyer[0:9],
                  buyerSite         = siretBuyer[9:14],
                  buyerOriginalName = buyerName,
                  buyerName         = buyerLegalName,
                  buyerLegalName    = buyerLegalName
             ).save()

    # recherche et sinon création de l'entreprise correspondant à l'acheteur
    entBuyer = pd.DataFrame(data= {'SIRET': [siretBuyer],
                                   'DS': [buyerName],
                                   'TYPE_ID': ['SIRET'],
                                   'NODE': [buyer]
                                  },
                            columns= ['SIRET', 'DS', 'TYPE_ID', 'NODE', 'SIRET_SIEGE', 'NODE_SIEGE']
                           )
    entBuyer = entBuyer.apply(addEnterprise, axis= 1)
    
    # création du lien acheteur -> entreprise acheteur
    entBuyer.apply(lambda x: buyer.buyerEnterprise.connect(x.NODE) if x.NODE is not None 
                   else logger.trace("Entreprise acheteur {} non trouvée", siretBuyer), axis= 1)
    
    # création de l'entité siège de l'acheteur 
    siegeBuyer = entBuyer[entBuyer.SIRET_SIEGE.notna()]
    if siegeBuyer.size != 0:
        siegeBuyer['TYPE_ID'] = 'SIRET'
        siegeBuyer['SIRET'] = siegeBuyer['SIRET_SIEGE']
        siegeBuyer = siegeBuyer.apply(addEnterprise, axis= 1)

        # création du lien entreprise acheteur -> siège entreprise acheteur
        siegeBuyer.apply(lambda x: x.NODE.siege.connect(x.NODE_SIEGE), axis= 1)

    buyer.recordCreationDate = datetime.now(tz= pytz.timezone('CET'))
    buyer.recordLastUpdate   = datetime.now(tz= pytz.timezone('CET'))
    buyer.save()
    
    return buyer


# In[4]:


@logger.catch
def addContract(contract: pd.Series):
    """
    - contract : Serie correspondant à une ligne de dataframe contenant les informations relatives à un contrat.
    - Sortie : renvoie la liste des noeuds Contract
    """
   
    col0 = ['titulaire_id_1', 'titulaire_id_2', 'titulaire_id_3']
    col1 = ['titulaire_denominationSociale_1', 'titulaire_denominationSociale_2', 'titulaire_denominationSociale_3']
    col2 = ['titulaire_typeIdentifiant_1', 'titulaire_typeIdentifiant_2', 'titulaire_typeIdentifiant_3']

    idsBuyer = {'acheteur.id': 'acheteur.nom'}

    titulaires = pd.concat([contract[col0].reset_index(drop= True),
                           contract[col1].reset_index(drop= True),
                           contract[col2].reset_index(drop= True),
                           pd.Series(index= range(0, 3), dtype= object),
                           pd.Series(index= range(0, 3), dtype= str),
                           pd.Series(index= range(0, 3), dtype= object)],
                           keys= ['SIRET', 'DS', 'TYPE_ID', 'NODE', 'SIRET_SIEGE', 'NODE_SIEGE'],
                           axis= 1)
    # création des titulaires
    titulaires = titulaires[titulaires.SIRET.notna()]   # on ne conserve que les valeurs renseignées

    if not titulaires.empty:
        titulaires = titulaires.apply(addEnterprise, axis= 1)
    else:
        # aucune entreprise identifiable. on ne crée rien.
        return None

    # création des noeuds sièges si distincts des titulaires
    sieges = titulaires[titulaires.SIRET_SIEGE.notna()]
    if sieges.size != 0:
        sieges['SIRET'] = sieges['SIRET_SIEGE']
        sieges = sieges.apply(addEnterprise, axis= 1)
        # création des liens avec les sièges
        sieges.apply(lambda x: x.NODE.siege.connect(x.NODE_SIEGE), axis= 1)
    

    # création du contrat
    contractKey = {'contractId': contract['id'], 'buyerId': contract['acheteur.id']}

    try:
        # dans le cas de plusieurs titulaires la classe Contract retournera également la sous-classe MultiPartyContract
        contractNode = Contract.nodes.get(key = contractKey)
        
    except DoesNotExist:

        # correction du champ montant le cas échéant
        montant = 0 if contract.isna().montant else contract.montant
        
        if contract[col0].notna().sum() > 1:
            # Les noeuds ainsi créés auront 2 labels : 'Contract' et 'MultiPartyContract'
            logger.trace("Création Contrat partenariat {}", contractKey)
            contractNode = MultiPartyContract(modelVersion = modelVersion,
                                              recordCreationDate = datetime.now(tz= pytz.timezone('CET')),
                                              key = contractKey,
                                              typeGroupementOperateurs= contract.typeGroupementOperateurs,
                                              year  = int(parser.parse(contract.dateNotification, fuzzy= True).year),
                                              month = int(parser.parse(contract.dateNotification, fuzzy= True).month),
                                              day   = int(parser.parse(contract.dateNotification, fuzzy= True).day),
                                              objet = contract.objet,
                                              procedure = contract.procedure,
                                              montant = montant
                                             ).save()
        else:
            logger.trace("Création Contrat {}", contractKey)
            contractNode = Contract(modelVersion = modelVersion,
                                    recordCreationDate = datetime.now(tz= pytz.timezone('CET')),
                                    key = contractKey,
                                    year  = int(parser.parse(contract.dateNotification, fuzzy= True).year),
                                    month = int(parser.parse(contract.dateNotification, fuzzy= True).month),
                                    day   = int(parser.parse(contract.dateNotification, fuzzy= True).day),
                                    objet = contract.objet,
                                    procedure = contract.procedure,
                                    montant = montant
                                   ).save()

        # connexion des entreprises au noeud contrat
        titulaires[titulaires.SIRET.notna()].apply(lambda x: x.NODE.publicContract.connect(contractNode), axis= 1)

    # mise à jour du noeud contrat à partir des colonnes exploitées depuis le dataset original
    colsContractOptions = colsContractBaseOptions + \
                          colsConsiderationsSociales + \
                          colsConsiderationsEnvironnementales + \
                          colsModaliteExecution + \
                          colsTechniques
    
    # on applique les modifications (ajouts, suppressions, renommages) appliquées sur les colonnes du dataset original
    # on se base pour cela sur le dataset v3. A revoir dans l'avenir.
    colsContractOptions = [c if c not in renameColsV3.keys() else renameColsV3[c] for c in useColsV3]
    colsContractOptions += addColsV3
    colsContractOptions = [c for c in colsContractOptions if c not in dropColsV3]
    
    contractOptions = contract[colsContractOptions]
    cols = contractOptions[contractOptions.notna()].index
 
    for col in cols:
        match col:
            case 'id'                       : contractNode.contractId = contract[col]
            case 'nature'                   : contractNode.nature = contract[col]
            case 'codeCPV'                  : connectToCpv(contractNode, contract[col])
            case 'dureeMois'                : contractNode.dureeMois = contract[col]
            case 'formePrix'                : contractNode.formePrix = contract[col]
            case 'objet'                    : contractNode.objet = contract[col]
            case 'source'                   : contractNode.source = contract[col]
            case 'techniques'               : contractNode.technique = contract[col]
            case 'modalitesExecution'       : contractNode.modaliteExecution = contract[col]
            case 'idAccordCadre'            : contractNode.idAccordCadre = contract[col]
            case 'marcheInnovant'           : contractNode.marcheInnovant = contract[col]
            case 'ccag'                     : contractNode.ccag = contract[col]
            case 'offresRecues'             : contractNode.offresRecues = contract[col]
            case 'attributionAvance'        : contractNode.attributionAvance = contract[col]
            case 'origineUE'                : contractNode.origineUE = contract[col]
            case 'origineFrance'            : contractNode.origineFrance = contract[col]
            case 'sousTraitanceDeclaree'    : contractNode.sousTraitanceDeclaree = contract[col]
            case 'actesSousTraitance'       : contractNode.actesSousTraitance = contract[col]
            case 'typesPrix'                : contractNode.typePrix = contract[col]
            case 'tauxAvance'               : contractNode.tauxAvance = contract[col]
            case 'booleanModification'      : contractNode.booleanModification = contract[col]
            case 'considerationsSociales'   : contractNode.considerationsSociales = contract[col]
            case 'considerationsEnvironnementales' : contractNode.considerationsEnvironnementales = contract[col]
            
            case 'Clause sociale'           : contractNode.clauseSociale = contract[col]
            case 'Critère social'           : contractNode.critereSocial = contract[col]
            case 'Marché réservé'           : contractNode.marcheReserve = contract[col]

            case 'Critère environnemental'  : contractNode.critereEnvironnemental = contract[col]
            case 'Clause environnementale'  : contractNode.clauseEnvironnementale = contract[col]

            case 'Bons de commande'         : contractNode.bonsDeCommande = contract[col]
            case 'Tranches'                 : contractNode.tranches = contract[col]
            case 'Marchés subséquents'      : contractNode.marchesSubsequents = contract[col]
            case 'Mixte'                    : contractNode.mixte = contract[col]

            case 'Accord-cadre'             : contractNode.accordCadre = contract[col]
            case 'Catalogue électronique'   : contractNode.catalogueElectronique = contract[col]
            case 'Concours',                : contractNode.concours = contract[col]
            case 'Acquisition dynamique'    : contractNode.AcquisitionDynamique = contract[col]
            case 'Système de qualification' : contractNode.systemeDeQualification = contract[col]
            case 'Enchère électronique'     : contractNode.enchereElectronique = contract[col]
            case _                          : pass             
           
    # mise à jour des dates au format datetime.date, en évitant les valeurs NaN
    colsDatesContract = ['dateNotification', 'datePublicationDonnees']
    datesContract = contract[colsDatesContract]
    cols = datesContract[datesContract.notna()].index
    for col in cols:
        match col:
            case 'dateNotification'      : contractNode.dateNotification = parser.parse(contract.dateNotification, fuzzy= True)
            case 'datePublicationDonnees': contractNode.datePublicationDonnees = parser.parse(contract.datePublicationDonnees, fuzzy= True)
            case 'created_at'            : contractNode.created_at = parser.parse(contract.created_at, fuzzy= True)
            case 'updated_at'            : contractNode.updated_at = parser.parse(contract.updated_at, fuzzy= True)
            case _: pass

    # recherche ou création du noeud acheteur
    siretBuyer = str(contract['acheteur.id'])[0:14]
    buyerName = str(contract['acheteur.nom'])
    dfBuyer = None
    buyerLegalDataExist = True

    # recherche si le noeud correspondant à la clé siretBuyer existe déjà
    try:
        buyer = Buyer.nodes.get(buyerId = siretBuyer)
        # si déjà existant sa relation siège est réputée avoir été créée en même temps
        try:
            # False si données Sirene pas trouvées lors de la création, None si jamais initialisé
            buyerLegalDataExist = buyer.buyerLegalData 

        except AttributeError:
            pass
        try:
            buyerLegalName = buyer.buyerLegalName
        except AttributeError:
            # noeud ancienne version
            contract = checkEnterprises(contract, idsBuyer)
            if not contract.isna().siret:
                dfBuyer = search_sirene(variable = ['siret'], number= 1,
                                        pattern = [siretBuyer], phonetic_search = False, legal=True, closed=True)
                # cette API peut renvoyer plusieurs lignes pour un même SIRET, même si number= 1.
                # on ne conserve que la première ligne retournée, en conservant néanmoins le format dataframe.
                dfBuyer = dfBuyer.iloc[0, :]
                dfBuyer = dfBuyer.to_frame().transpose()
                buyerLegalName = str(*dfBuyer.denominationUniteLegale)
                if dfBuyer.isna().denominationUniteLegale.all():
                    buyerLegalName = 'not populated in SIRENE'
                buyer.buyerLegalName = buyerLegalName
            else:
                buyerLegalName = buyerName + ' : unknown in SIRENE'
                dfBuyer = None
                
    except DoesNotExist:
        # on crée le noeud ainsi que ses relations siège et localisation
        # traitement du cas où l'identité de l'acheteur n'est pas renseignée.
        # par défaut on prendra la dénomination sociale correspondant au SIRET.
        try:
            dfBuyer = search_sirene(variable = ['siret'], number= 1,
                                    pattern = [siretBuyer], phonetic_search = False, legal=True, closed=True)
            # cette API peut renvoyer plusieurs lignes pour un même SIRET, même si number= 1.
            # on ne conserve que la première ligne retournée, en conservant néanmoins le format dataframe.
            dfBuyer = dfBuyer.iloc[0, :]
            dfBuyer = dfBuyer.to_frame().transpose()
            buyerLegalName = str(*dfBuyer.denominationUniteLegale)
            if dfBuyer.isna().denominationUniteLegale.all():
                buyerLegalName = 'not populated in SIRENE'
            
        except RequestException:
            buyerLegalName = buyerName + ' : unknown in SIRENE'
            dfBuyer = None
            
        buyerLegalDataExist = dfBuyer is not None

        logger.trace("Buyer does not exist, creation : id = {}, legal name = {}, name = {}",
                     siretBuyer, buyerName, buyerLegalName)
        buyer = addBuyer(siretBuyer= siretBuyer, buyerName= buyerName, buyerLegalName= buyerLegalName)
        
        buyer.buyerLegalData = buyerLegalDataExist
        buyer.save()
        
        # fin de la création du noeud acheteur

    # création du lien acheteur -> contrat
    if not buyer.managedContract.is_connected(contractNode):
        buyer.managedContract.connect(contractNode)

    # création le cas échéant du lien localisation de l'acheteur
    if len(buyer.buyerLocation.match()) == 0:
        logger.trace("Pas de lien localisation acheteur {} {}", siretBuyer, buyerName)
        if ((buyerLegalDataExist == None) | (buyerLegalDataExist == True)) & (dfBuyer is None):
            # On est dans le cas d'un noeud acheteur créé antérieurement sans création de lien localisation
            # le cas buyerLegalDataExist == True correspond à un noeud mal renseigné à la création en raison d'un bug
            try:
                logger.trace("Recherche acheteur {} {}", siretBuyer, buyerName)
                dfBuyer = search_sirene(variable = ['siret'], number= 1,
                                        pattern = [siretBuyer], phonetic_search = False, legal=True, closed=True)
                # cette API peut renvoyer plusieurs lignes pour un même SIRET, même si number= 1.
                # on ne conserve que la première ligne retournée, en conservant néanmoins le format dataframe.
                dfBuyer = dfBuyer.iloc[0, :]
                dfBuyer = dfBuyer.to_frame().transpose()
                
            except RequestException:
                logger.trace("Acheteur {} {} non trouvé dans la base SIRENE", siretBuyer, buyerName)
                dfBuyer = None
                
            buyer.buyerLegalData = dfBuyer is not None
            buyer.recordLastUpdate = datetime.now(tz= pytz.timezone('CET'))
            buyer.save()

        if dfBuyer is not None:
            codes = {'code': dfBuyer.codeCommuneEtablissement.values.all(),
                     'type': 'code commune',
                     'nom': '',
                     'communeBuyer': dfBuyer.codeCommuneEtablissement.values.all(),
                     'final': '', 'subtype': '', 'country': '', 'region': '', 'departement': '', 'commune': '', 'postal': '',
                     'cedex': '', 'canton': '', 'arrondissement': ''}
    
            codes, buyerLocation = getDeliveryLocation(codes)
            logger.trace("Localisation acheteur {} {} : {}", siretBuyer, buyerName, codes)

            if buyerLocation is not None:
                logger.trace("Connexion acheteur {} {} : {}", siretBuyer, buyerName, codes)
                buyer.buyerLocation.connect(buyerLocation, {'deliveryLocationTypeId': codes['subtype']})
                buyer.recordLastUpdate = datetime.now(tz= pytz.timezone('CET'))
                buyer.save()
    else:
        # des liens avec la localisation de l'acheteur existent, ce qui implique l'existence de ses informations légales
        # on confirme cette propriété du noeud acheteur
        buyer.buyerLegalData = True
        buyer.recordLastUpdate = datetime.now(tz= pytz.timezone('CET'))
        buyer.save()

    # création du lieu d'exécution
    if dfBuyer is not None:
        communeBuyer = dfBuyer.codeCommuneEtablissement.values.all()
    else:
        communeBuyer = ''

    codes = {'code': contract['lieuExecution.code'],
             'type': unidecode(str(contract['lieuExecution.typeCode']).lower()), 
             'nom': str(contract['lieuExecution.nom']),
             'communeBuyer': communeBuyer,
             'final': '', 'subtype': '', 'country': '', 'region': '', 'departement': '', 'commune': '', 'postal': '',
             'cedex': '', 'canton': '', 'arrondissement': ''}
    
    codes, deliveryLocation = getDeliveryLocation(codes)

    # création du lien contrat -> lieu d'exécution
    if deliveryLocation is not None:
        contractNode.deliveryLocation.connect(deliveryLocation, {'deliveryLocationTypeId': codes['subtype']})

    contractNode.recordLastUpdate = datetime.now(tz= pytz.timezone('CET'))
    contractNode.save()

    return contractNode


# In[5]:


@logger.catch
def updateContract(contract: pd.Series, cols= None, key= 'old'):
    """
    - contract : ligne de dataframe contenant les informations relatives à un contrat.
    - cols : optionnel, liste des colonnes à mettre à jour
    """
   
    # recherche du contrat

    contractKey = {'contractId': contract['id'], 'buyerId': contract['acheteur.id']}
        
    try:
        contractNode = Contract.nodes.get(key = contractKey)
    
        if cols == None:        
            # mise à jour du noeud contrat à partir des colonnes exploitées depuis le dataset original
            colsContractOptions = colsContractBaseOptions + \
                                  colsConsiderationsSociales + \
                                  colsConsiderationsEnvironnementales + \
                                  colsModaliteExecution + \
                                  colsTechniques
            
            # on applique les modifications (ajouts, suppressions, renommages) appliquées sur les colonnes du dataset original
            # on se base pour cela sur le dataset v3. A revoir dans l'avenir.
            colsContractOptions = [c if c not in renameColsV3.keys() else renameColsV3[c] for c in useColsV3]
            colsContractOptions += addColsV3
            colsContractOptions = [c for c in colsContractOptions if c not in dropColsV3]
        else:
            colsContractOptions = cols
            
        contractOptions = contract[colsContractOptions]
        cols = contractOptions[contractOptions.notna()].index
     
        for col in cols:
            match col:
                case 'id'                       : contractNode.contractId = contract[col]
                case 'nature'                   : contractNode.nature = contract[col]
                case 'codeCPV'                  : connectToCpv(contractNode, contract[col])
                case 'dureeMois'                : contractNode.dureeMois = contract[col]
                case 'formePrix'                : contractNode.formePrix = contract[col]
                case 'objet'                    : contractNode.objet = contract[col]
                case 'source'                   : contractNode.source = contract[col]
                case 'techniques'               : contractNode.technique = contract[col]
                case 'modalitesExecution'       : contractNode.modaliteExecution = contract[col]
                case 'idAccordCadre'            : contractNode.idAccordCadre = contract[col]
                case 'marcheInnovant'           : contractNode.marcheInnovant = contract[col]
                case 'ccag'                     : contractNode.ccag = contract[col]
                case 'offresRecues'             : contractNode.offresRecues = contract[col]
                case 'attributionAvance'        : contractNode.attributionAvance = contract[col]
                case 'origineUE'                : contractNode.origineUE = contract[col]
                case 'origineFrance'            : contractNode.origineFrance = contract[col]
                case 'sousTraitanceDeclaree'    : contractNode.sousTraitanceDeclaree = contract[col]
                case 'actesSousTraitance'       : contractNode.actesSousTraitance = contract[col]
                case 'typesPrix'                : contractNode.typePrix = contract[col]
                case 'tauxAvance'               : contractNode.tauxAvance = contract[col]
                case 'booleanModification'      : contractNode.booleanModification = contract[col]
                case 'considerationsSociales'   : contractNode.considerationsSociales = contract[col]
                case 'considerationsEnvironnementales' : contractNode.considerationsEnvironnementales = contract[col]
                
                case 'Clause sociale'           :
                    contractNode.clauseSociale = contract[col]
                case 'Critère social'           : 
                    contractNode.critereSocial = contract[col]
                case 'Marché réservé'           :
                    contractNode.marcheReserve = contract[col]
                case 'Critère environnemental'  :
                    contractNode.critereEnvironnemental = contract[col]
                case 'Clause environnementale'  :
                    contractNode.clauseEnvironnementale = contract[col]
                case 'Bons de commande'         :
                    contractNode.bonsDeCommande = contract[col]
                case 'Tranches'                 :
                    contractNode.tranches = contract[col]
                case 'Marchés subséquents'      : 
                    contractNode.marchesSubsequents = contract[col]
                case 'Mixte'                    : 
                    contractNode.mixte = contract[col]
                case 'Accord-cadre'             : 
                    contractNode.accordCadre = contract[col]
                case 'Catalogue électronique'   : 
                    contractNode.catalogueElectronique = contract[col]
                case 'Concours',                : 
                    contractNode.concours = contract[col]
                case 'Acquisition dynamique'    : 
                    contractNode.AcquisitionDynamique = contract[col]
                case 'Système de qualification' : 
                    contractNode.systemeDeQualification = contract[col]
                case 'Enchère électronique'     : 
                    contractNode.enchereElectronique = contract[col]
                case _                          : 
                    pass
               
        contractNode.recordLastUpdate = datetime.now(tz= pytz.timezone('CET'))
        contractNode.save()
        
        # recherche ou création du noeud acheteur
        siretBuyer = str(contract['acheteur.id'])[0:14]
        buyerName = str(contract['acheteur.nom'])
        dfBuyer = None
        buyerLegalDataExist = True
    
        # recherche si le noeud correspondant à la clé siretBuyer existe déjà
        try:
            buyer = Buyer.nodes.get(buyerId = siretBuyer)
            # si déjà existant sa relation siège est réputée avoir été créée en même temps
            try:
                # False si données Sirene pas trouvées lors de la création, None si jamais initialisé
                buyerLegalDataExist = buyer.buyerLegalData 
    
            except AttributeError:
                pass
    
        except DoesNotExist:
            # on crée le noeud ainsi que ses relations siège et localisation
            # traitement du cas où l'identité de l'acheteur n'est pas renseignée.
            # par défaut on prendra la dénomination sociale correspondant au SIRET.
            try:
                dfBuyer = search_sirene(variable = ['siret'], number= 1,
                                        pattern = [siretBuyer], phonetic_search = False, legal=True, closed=True)
                # cette API peut renvoyer plusieurs lignes pour un même SIRET, même si number= 1.
                # on ne conserve que la première ligne retournée, en conservant néanmoins le format dataframe.
                dfBuyer = dfBuyer.iloc[0, :]
                dfBuyer = dfBuyer.to_frame().transpose()
                buyerLegalName = str(*dfBuyer.denominationUniteLegale)
                if dfBuyer.isna().denominationUniteLegale.all():
                    buyerLegalName = 'not populated in SIRENE'
                
            except RequestException:
                buyerLegalName = buyerName + ' : unknown in SIRENE'
                dfBuyer = None
                
            buyerLegalDataExist = dfBuyer is not None
    
            logger.trace("Buyer does not exist, creation : id = {}, legal name = {}, name = {}",
                         siretBuyer, buyerName, buyerLegalName)
            buyer = addBuyer(siretBuyer= siretBuyer, buyerName= buyerName, buyerLegalName= buyerLegalName)
            
            buyer.buyerLegalData = buyerLegalDataExist
            buyer.save()
            
            # fin de la création du noeud acheteur
    
        # création du lien acheteur -> contrat
        if not buyer.managedContract.is_connected(contractNode):
            buyer.managedContract.connect(contractNode)
    
        # création le cas échéant du lien localisation de l'acheteur
        if len(buyer.buyerLocation.match()) == 0:
            logger.trace("Pas de lien localisation acheteur {} {}", siretBuyer, buyerName)
            if ((buyerLegalDataExist == None) | (buyerLegalDataExist == True)) & (dfBuyer is None):
                # On est dans le cas d'un noeud acheteur créé antérieurement sans création de lien localisation
                # le cas buyerLegalDataExist == True correspond à un noeud mal renseigné à la création en raison d'un bug
                try:
                    logger.trace("Recherche acheteur {} {}", siretBuyer, buyerName)
                    dfBuyer = search_sirene(variable = ['siret'], number= 1,
                                            pattern = [siretBuyer], phonetic_search = False, legal=True, closed=True)
                    # cette API peut renvoyer plusieurs lignes pour un même SIRET, même si number= 1.
                    # on ne conserve que la première ligne retournée, en conservant néanmoins le format dataframe.
                    dfBuyer = dfBuyer.iloc[0, :]
                    dfBuyer = dfBuyer.to_frame().transpose()
                    
                except RequestException:
                    logger.trace("Acheteur {} {} non trouvé dans la base SIRENE", siretBuyer, buyerName)
                    dfBuyer = None
                    
                buyer.buyerLegalData = dfBuyer is not None
                buyer.recordLastUpdate = datetime.now(tz= pytz.timezone('CET'))
                buyer.save()
    
            if dfBuyer is not None:
                codes = {'code': dfBuyer.codeCommuneEtablissement.values.all(),
                         'type': 'code commune',
                         'nom': '',
                         'communeBuyer': dfBuyer.codeCommuneEtablissement.values.all(),
                         'final': '', 'subtype': '', 'country': '', 'region': '', 'departement': '', 'commune': '', 'postal': '',
                         'cedex': '', 'canton': '', 'arrondissement': ''}
        
                codes, buyerLocation = getDeliveryLocation(codes)
                logger.trace("Localisation acheteur {} {} : {}", siretBuyer, buyerName, codes)
    
                if buyerLocation is not None:
                    logger.trace("Connexion acheteur {} {} : {}", siretBuyer, buyerName, codes)
                    buyer.buyerLocation.connect(buyerLocation, {'deliveryLocationTypeId': codes['subtype']})
                    buyer.recordLastUpdate = datetime.now(tz= pytz.timezone('CET'))
                    buyer.save()
        else:
            # des liens avec la localisation de l'acheteur existent, ce qui implique l'existence de ses informations légales
            # on confirme cette propriété du noeud acheteur
            buyer.buyerLegalData = True
            buyer.recordLastUpdate = datetime.now(tz= pytz.timezone('CET'))
            buyer.save()
    
        # création du lieu d'exécution
        if dfBuyer is not None:
            communeBuyer = dfBuyer.codeCommuneEtablissement.values.all()
        else:
            communeBuyer = ''
    
        codes = {'code': contract['lieuExecution.code'],
                 'type': unidecode(str(contract['lieuExecution.typeCode']).lower()), 
                 'nom': str(contract['lieuExecution.nom']),
                 'communeBuyer': communeBuyer,
                 'final': '', 'subtype': '', 'country': '', 'region': '', 'departement': '', 'commune': '', 'postal': '',
                 'cedex': '', 'canton': '', 'arrondissement': ''}
        
        codes, deliveryLocation = getDeliveryLocation(codes)
    
        # création du lien contrat -> lieu d'exécution
        if deliveryLocation is not None:
            contractNode.deliveryLocation.connect(deliveryLocation, {'deliveryLocationTypeId': codes['subtype']})
    
        contractNode.recordLastUpdate = datetime.now(tz= pytz.timezone('CET'))
        contractNode.save()

        
    except DoesNotExist:
        logger.trace("Contrat inconnu : {}", contractKey)
        contractNode = None

    return contractNode


# In[6]:


def AddContractsIncremental():
    """
    """
    prevDecp = loadDecpV4('Z:/datasets/DECP/decp-2022-marches-valides-previous.csv')
    newDecp =  loadDecpV4()

    diff = pd.concat([newDecp, prevDecp]).drop_duplicates(subset=  ['id', 'acheteur.id'], keep= False).reset_index(drop= True)

    nodes = diff.apply(addContract, axis= 1)

    return nodes


# In[7]:


@logger.catch
def addEnterprise(titulaire: pd.Series) -> pd.Series:
    """
    - titulaire : Series avec 6 champs : SIRET', 'DS', 'TYPE_ID', 'NODE', 'SIRET_SIEGE', 'NODE_SIEGE'

    Si le champ SIRET_SIEGE n'est pas NaN, on crée un siège (non titulaire) d'une entreprise titulaire.
    retour : Series titulaire, avec noeud existant ou créé renseigné dans la colonne 'NODE'.
    """

    siret = str(titulaire.SIRET)
    if siret.find('.') != -1:
        # la chaîne de caractères supposée représenter le numéro contient un point, probablement conversion erronée en float
        siret = siret.split('.')[0]
        if len(siret) in [8, 13]:
            # manque un digit, probablement un zéro initial supprimé par une conversion au format float
            siret = '0' + siret  # on le rajoute
    if len(siret) == 9:
        # on a probablement affaire en fait à un n° SIREN
        try:
            df = search_sirene(variable = ['siren'], number= 1, pattern = [siret], phonetic_search = False, legal=True,
                               closed=True)
            siret = df.siret[0]
        except RequestException:
            pass

    # vérifier si le site existe déjà
    try:
        enterprise = Enterprise.nodes.get(titulaireId = siret)

    except DoesNotExist:
        enterprise = addEnterpriseWithSiret(sirenId= siret, typeId= titulaire.TYPE_ID, originalDS= titulaire.DS)

    if titulaire.isna().SIRET_SIEGE:
        # le numéro de SIRET du siège n'est pas indiqué dans le dataframe en entrée.
        # le noeud créé est enregistré dans le champ NODE du dataframe qui sera retourné.
        titulaire['NODE'] = enterprise
    else:
        # SIRET_SIEGE avait été renseigné dans un précédent appel à AddEnterprise
        # le noeud 'enterprise' nouvellement créé est alors le siège d'une entreprise titulaire
        # le noeud créé est enregistré dans le champ NODE_SIEGE du dataframe qui sera retourné
        titulaire['NODE_SIEGE'] = enterprise

    if not enterprise.isSiege:
        # SIRET_SIEGE calculé, NODE_SIEGE = NaN
        titulaire['SIRET_SIEGE'] = enterprise.titulaireSiren + enterprise.nicSiegeUniteLegale
    
    return titulaire


# In[8]:


def addEnterpriseWithSiret(sirenId: str, typeId: str= 'SIRET', originalDS= ''):
    """
    """
    
    # récupérer les informations SIRENE sur la base du SIRET
    var = str(typeId).lower()
    if var not in ['siret', 'siren']:
        logger.trace("Type identifiant invalide : {} pour {}", typeId, sirenId)
    try:
        foundWithSiren = False
        df = search_sirene(variable = [var], number= 1,
                           pattern = [sirenId], phonetic_search = False, legal=True, closed=True)
    except RequestException:
        try:
            # on cherche avec le numéro SIREN
            df = search_sirene(variable = ['siren'], number= 1,
                               pattern = [sirenId[0:9]], phonetic_search = False, legal=True, closed=True)
            # trouvé avec le numéro SIREN
            foundWithSiren = True
        except RequestException:
            # décidément pas trouvé
            # on recherche d'abord si le noeud existe déjà, ce qui peut arriver si on insère un siret fictif avec 00000
            titulaireId= sirenId if typeId.lower() == 'siret' else sirenId + '00000'
            try:
                enterprise = Enterprise.nodes.get(titulaireId = titulaireId)
            except RequestException:
                # création du noeud avec informations minimales, le site est considéré siège par défaut
                # il n'est pas rattaché à une localisation.
                enterprise = Enterprise(modelVersion = modelVersion,
                                        recordCreationDate = datetime.now(tz= pytz.timezone('CET')),
                                        recordLastUpdate   = datetime.now(tz= pytz.timezone('CET')),
                                        titulaireId= titulaireId,
                                        titulaireSiren = sirenId[0:9],
                                        titulaireSite =  sirenId[9:14],
                                        titulaireDenominationSociale = 'not known in SIRENE',
                                        titulaireDenominationOriginale = originalDS,
                                        titulaireTypeIdentifiant = typeId,
                                        isSiege = True).save()
            return enterprise
        
    # à ce point on a identifié l'entreprise avec son numéro SIRET ou SIREN
    # l'API SIRENE peut renvoyer plusieurs lignes pour un même SIRET, même si number= 1.
    # on ne conserve que la première ligne retournée, en conservant néanmoins le format dataframe.
    df = df.iloc[0, :]
    df = df.to_frame().transpose()
        
    # application de quelques règles de gestion sur les attributs restitués par l'API SIRENE
    # si personne physique :
    if df['denominationUniteLegale'].values == None:
        if df['nomUsageUniteLegale'].values == None:
            nom = str(*df['nomUniteLegale'])
        else:
            nom = str(*df['nomUsageUniteLegale'])
        civilite = ''
        if df['sexeUniteLegale'].values is not None:
            if df['sexeUniteLegale'].values == 'M':
                civilite = 'M.'
            else:
                civilite = 'Mme.'
        prenom = ''
        if df['prenomUsuelUniteLegale'].values is not None:
            prenom = df['prenomUsuelUniteLegale'].values
        denominationUniteLegale = civilite + prenom + nom
        
    else:
        denominationUniteLegale = str(*df['denominationUniteLegale'])

    createEnterprise = True
    if foundWithSiren:
        logger.trace("Recherche dans la base entreprise {} {} , denomination sociale originale = {}, SIRET original = {}",
                     df.siret[0], denominationUniteLegale, originalDS, sirenId)
        try:
            enterprise = Enterprise.nodes.get(titulaireId = df.siret[0])
            createEnterprise = False
            logger.trace("Entreprise trouvée dans la base : {} {} , denomination sociale originale = {}, SIRET original = {}",
                         df.siret[0], denominationUniteLegale, originalDS, sirenId)
        except RequestException:
            pass

    if createEnterprise:
        # création du noeud Enterprise avec les informations minimales
        logger.trace("Création Entreprise {} {}, denomination sociale originale = {}, SIRET original = {}",
                     df.siret[0], denominationUniteLegale, originalDS, sirenId)
    
        enterprise = Enterprise(modelVersion = modelVersion,
                                recordCreationDate = datetime.now(tz= pytz.timezone('CET')),
                                titulaireId= df.siret[0],
                                titulaireSiren = df.siren[0],
                                titulaireSite =  df.nic[0],
                                titulaireTypeIdentifiant = typeId,
                                titulaireDenominationSociale = denominationUniteLegale,
                                titulaireDenominationOriginale = originalDS,
                                isSiege = df.etablissementSiege.all()
                               ).save()

    # on ne conserve ensuite que les informations effectivement renseignées
    df = df.dropna(axis= 'columns')
    cols = df.columns

    # enregistrement des informations optionnelles
    for col in cols:
        match col:
            case 'dateDebut'                           : enterprise.dateDebut = parser.parse(*df[col], fuzzy= True)
            case 'dateCreationEtablissement'           :
                enterprise.dateCreationEtablissement = parser.parse(*df[col], fuzzy= True)
            case 'dateCreationUniteLegale'             :
                enterprise.dateCreationUniteLegale = parser.parse(*df[col], fuzzy= True)
            case 'dateFin'                             : enterprise.dateFin = parser.parse(*df[col], fuzzy= True)                        ,
            # ligne suivante supprimée, incohérent avec traitement effectué précédemment 
            # case 'denominationUniteLegale'             : enterprise.denominationUniteLegale = str(*df[col])
            # 
            case 'nicSiegeUniteLegale'                 : enterprise.nicSiegeUniteLegale = str(*df[col])
            case 'categorieEntreprise'                 : enterprise.categorieEntreprise = str(*df[col])
            case 'categorieJuridiqueUniteLegale'       : enterprise.categorieJuridiqueUniteLegale = str(*df[col])
            case 'categorieJuridiqueUniteLegaleLibelle':
                enterprise.categorieJuridiqueUniteLegaleLibelle = str(*df[col])
            case 'activitePrincipaleUniteLegale'       : enterprise.activitePrincipaleUniteLegale = str(*df[col])
            case 'activitePrincipaleUniteLegaleLibelle':
                enterprise.activitePrincipaleUniteLegaleLibelle = str(*df[col])
            case 'activitePrincipaleEtablissement'     : connectToNaf(enterprise, str(*df[col]))
            case 'numeroVoieEtablissement'             : enterprise.numeroVoieEtablissement = str(*df[col])
            case 'typeVoieEtablissement'               : enterprise.typeVoieEtablissement = str(*df[col])
            case 'typeVoieEtablissementLibelle'        : enterprise.typeVoieEtablissementLibelle = str(*df[col])
            case 'libelleVoieEtablissement'            : enterprise.libelleVoieEtablissement = str(*df[col])
            case 'codePostalEtablissement'             : enterprise.codePostalEtablissement = str(*df[col])
            case 'libelleCommuneEtablissement'         : enterprise.libelleCommuneEtablissement = str(*df[col])
            case 'codeCommuneEtablissement'            : enterprise.codeCommuneEtablissement = str(*df[col])
            case 'statutDiffusionEtablissement'        : enterprise.statutDiffusionEtablissement = str(*df[col])
            case 'trancheEffectifsEtablissement'       : enterprise.trancheEffectifsEtablissement = str(*df[col])
            case 'anneeEffectifsEtablissement'         : enterprise.anneeEffectifsEtablissement = str(*df[col])
            case 'activitePrincipaleRegistreMetiersEtablissement':
                enterprise.activitePrincipaleRegistreMetiersEtablissement = str(*df[col])
            case 'trancheEffectifsUniteLegale'         : enterprise.trancheEffectifsUniteLegale = str(*df[col])
            case 'effectifsMinUniteLegale'             : enterprise.effectifsMinUniteLegale = float(*df[col])
            case 'effectifsMaxUniteLegale'             : enterprise.effectifsMaxUniteLegale = float(*df[col])
            case 'anneeEffectifsUniteLegale'           : enterprise.anneeEffectifsUniteLegale = str(*df[col])
            case 'anneeCategorieEntreprise'            : enterprise.anneeCategorieEntreprise = str(*df[col])
            case _                                     : pass

    enterprise.recordLastUpdate = datetime.now(tz= pytz.timezone('CET'))
    enterprise.save()

    # récupération du noeud City ou création du noeud City si non existant
    if enterprise.codeCommuneEtablissement is not None:
        codes = {'code': str(enterprise.codeCommuneEtablissement), 'final': 'commune',
                 'type': 'code commune', 'subtype': '', 'country': '', 'region': '', 'departement': '',
                 'nom' : '',
                 'commune': str(enterprise.codeCommuneEtablissement), 'postal': '', 'cedex': ''}

        codes = getLocationCode(codes)
        city = getCity(codes)  # city peut être un noeud City ou Cedex, sous-classes de LocationNode
        if city is not None:
            enterprise.enterpriseLocation.connect(city) # création de la relation avec city

    enterprise.save()
    
    return enterprise


# In[9]:


def updatePartnershipContractNode(contract: pd.Series):
    """
    """
    # création du noeud Contrat. La clé est supposée unique
    contractKey = {'contractId': contract['id'], 'buyerId': contract['acheteur.id']}

    try:
        contractNode = MultiPartyContract(modelVersion = modelVersion,
                                          key = contractKey,
                                          typeGroupementOperateurs= contract.typeGroupementOperateurs,
                                          year  = int(parser.parse(contract.dateNotification, fuzzy= True).year),
                                          month = int(parser.parse(contract.dateNotification, fuzzy= True).month),
                                          day   = int(parser.parse(contract.dateNotification, fuzzy= True).day),
                                          objet = contract.objet,
                                          procedure = contract.procedure,
                                          montant = contract.montant
                                         ).save()
        
    except (ConstraintValidationFailed, UniqueProperty):
        logger.trace("Clé contrat non unique : {} {} ", contractKey['contractId'], contractKey['buyerId'])
        return contract
        
    # mise à jour du noeud contrat à partir des colonnes exploitées depuis le dataset original
    colsContractOptions = colsContractBaseOptions + \
                          colsConsiderationsSociales + \
                          colsConsiderationsEnvironnementales + \
                          colsModaliteExecution + \
                          colsTechniques
    
    # on applique les modifications (ajouts, suppressions, renommages) appliquées sur les colonnes du dataset original
    # on se base pour cela sur le dataset v3. A revoir dans l'avenir.
    colsContractOptions = [c if c not in renameColsV3.keys() else renameColsV3[c] for c in useColsV3]
    colsContractOptions += addColsV3
    colsContractOptions = [c for c in colsContractOptions if c not in dropColsV3]
    
    contractOptions = contract[colsContractOptions]
    cols = contractOptions[contractOptions.notna()].index
 
    for col in cols:
        match col:
            case 'id'                       : contractNode.idContract = contract[col]
            case 'acheteur.id'              : contractNode.buyerId = contract[col]
            case 'nature'                   : contractNode.nature = contract[col]
            case 'codeCPV'                  : connectToCpv(contractNode, contract[col])
            case 'dureeMois'                : contractNode.dureeMois = contract[col]
            case 'formePrix'                : contractNode.formePrix = contract[col]
            case 'objet'                    : contractNode.objet = contract[col]
            case 'source'                   : contractNode.source = contract[col]
            case 'techniques'               : contractNode.technique = contract[col]
            case 'modalitesExecution'       : contractNode.modaliteExecution = contract[col]
            case 'idAccordCadre'            : contractNode.idAccordCadre = contract[col]
            case 'marcheInnovant'           : contractNode.marcheInnovant = contract[col]
            case 'ccag'                     : contractNode.ccag = contract[col]
            case 'offresRecues'             : contractNode.offresRecues = contract[col]
            case 'attributionAvance'        : contractNode.attributionAvance = contract[col]
            case 'origineUE'                : contractNode.origineUE = contract[col]
            case 'origineFrance'            : contractNode.origineFrance = contract[col]
            case 'sousTraitanceDeclaree'    : contractNode.sousTraitanceDeclaree = contract[col]
            case 'actesSousTraitance'       : contractNode.actesSousTraitance = contract[col]
            case 'typesPrix'                : contractNode.typePrix = contract[col]
            case 'tauxAvance'               : contractNode.tauxAvance = contract[col]
            case 'booleanModification'      : contractNode.booleanModification = contract[col]
            case 'considerationsSociales'   : contractNode.considerationsSociales = contract[col]
            case 'considerationsEnvironnementales' : contractNode.considerationsEnvironnementales = contract[col]
            
            case 'Clause sociale'           : contractNode.clauseSociale = contract[col]
            case 'Critère social'           : contractNode.critereSocial = contract[col]
            case 'Marché réservé'           : contractNode.marcheReserve = contract[col]

            case 'Critère environnemental'  : contractNode.critereEnvironnemental = contract[col]
            case 'Clause environnementale'  : contractNode.clauseEnvironnementale = contract[col]

            case 'Bons de commande'         : contractNode.bonsDeCommande = contract[col]
            case 'Tranches'                 : contractNode.tranches = contract[col]
            case 'Marchés subséquents'      : contractNode.marchesSubsequents = contract[col]
            case 'Mixte'                    : contractNode.mixte = contract[col]

            case 'Accord-cadre'             : contractNode.accordCadre = contract[col]
            case 'Catalogue électronique'   : contractNode.catalogueElectronique = contract[col]
            case 'Concours',                : contractNode.concours = contract[col]
            case 'Acquisition dynamique'    : contractNode.AcquisitionDynamique = contract[col]
            case 'Système de qualification' : contractNode.systemeDeQualification = contract[col]
            case 'Enchère électronique'     : contractNode.enchereElectronique = contract[col]
            case _                          : pass             
           
    # mise à jour des dates au format datetime.date, en évitant les valeurs NaN
    colsDatesContract = ['dateNotification', 'datePublicationDonnees']
    datesContract = contract[colsDatesContract]
    cols = datesContract[datesContract.notna()].index
    for col in cols:
        match col:
            case 'dateNotification'      : contractNode.dateNotification = parser.parse(contract.dateNotification, fuzzy= True)
            case 'datePublicationDonnees': contractNode.datePublicationDonnees = parser.parse(contract.datePublicationDonnees, fuzzy= True)
            case 'created_at'            : contractNode.created_at = parser.parse(contract.created_at, fuzzy= True)
            case 'updated_at'            : contractNode.updated_at = parser.parse(contract.updated_at, fuzzy= True)
            case _: pass

    contractNode.save()

    # création du lien acheteur -> contrat
    siretBuyer = str(contract['acheteur.id'])[0:14]
    try:
        buyer = Buyer.nodes.get(buyerId = siretBuyer)
    except DoesNotExist:
        buyer = Buyer(modelVersion = modelVersion,
              buyerId    = siretBuyer,
              buyerSiren = siretBuyer[0:9],
              buyerSite  = siretBuyer[9:14],
             ).save()

    buyer.managedContract.connect(contractNode)

    # création des liens entreprises - > contrat
    colTitulaireId = ['titulaire_id_1', 'titulaire_id_2', 'titulaire_id_3']
    ent = contract[colTitulaireId]
    entreprises = ent[ent.notna()].to_numpy() 

    for entreprise in entreprises:
        try:
            entNode = Enterprise.nodes.get(titulaireId = entreprise)
            entNode.publicContract.connect(contractNode)
        except DoesNotExist:
            logger.trace("Entreprise inconnue : {}", entreprise)
    return contract


# In[10]:


def reconnectEnterprises(contract: pd.Series):
    """
    """
    # recherche du noeud Contrat. La clé est supposée unique
    contractKey = {'contractId': contract['id'], 'buyerId': contract['acheteur.id']}

    try:
        contractNode = MultiPartyContract.nodes.get(key = contractKey)
        
    except DoesNotExist:
        logger.trace("Contrat inconnu : {} {} ", contractKey['contractId'], contractKey['buyerId'])
        raise DoesNotExist('MultiPartyContract')
        # return contract

    # création des liens entreprises - > contrat
    colTitulaireId = ['titulaire_id_1', 'titulaire_id_2', 'titulaire_id_3']
    ent = contract[colTitulaireId]
    entreprises = ent[ent.notna()].to_numpy() 
    
    for entreprise in entreprises:
        try:
            entNode = Enterprise.nodes.get(titulaireId = entreprise)
            entNode.publicContract.connect(contractNode)
        except DoesNotExist:
            logger.trace("Entreprise inconnue : {}", entreprise)
            raise DoesNotExist('Enterprise')
    return contract


# In[16]:


def updateBuyer(buyer):
    """
    """

    if buyer is not None:
        try:
            buyerId = buyer.buyerId
            buyerName = buyer.buyerName
        except AttributeError:
            logger.trace("Noeud acheteur incorrect : {}", buyer)
            return buyer
    else:
        logger.trace("Noeud acheteur = None")
        return buyer

    sirenId, sirenType = checkSiretOrSiren(buyerId)
    if sirenType is None:   # identifiant invalide
        logger.trace("Identifiant acheteur invalide : {}", buyerId)
        return buyer
        
    try:
        dfBuyer = search_sirene(variable = [sirenType], number= 1,
                                pattern = [sirenId], phonetic_search = False, legal=True, closed=True)
    
        # cette API peut renvoyer plusieurs lignes pour un même SIRET, même si number= 1.
        # on ne conserve que la première ligne retournée, en conservant néanmoins le format dataframe.
        dfBuyer = dfBuyer.iloc[0, :]
        dfBuyer = dfBuyer.to_frame().transpose()
        if buyer.buyerLegalName is None:
            buyer.buyerOriginalName = buyer.buyerName
            
        if dfBuyer.isna().denominationUniteLegale.all():
            buyer.buyerLegalName = 'not populated in SIRENE'
            logger.trace("Dénomination légale de l'acheteur {} de SIRET {} absente de SIRENE", buyer.buyerName, buyer.buyerId)
        else:
            buyer.buyerLegalName = str(*dfBuyer.denominationUniteLegale)
            buyer.buyerName = buyer.buyerLegalName
            
        # on met à jour le numéro de SIRET identifiant l'acheteur, même si initialement identifié par n° SIREN
        buyer.buyerId = str(*dfBuyer.siret)
        buyer.Siren = str(*dfBuyer.siren)
        buyer.Site = str(*dfBuyer.nic)
    
    except RequestException:
        dfBuyer = None
    
    buyerLegalDataExist = dfBuyer is not None
    buyer.buyerLegalData = buyerLegalDataExist

    # création le cas échéant du lien localisation de l'acheteur
    if len(buyer.buyerLocation.match()) == 0:
        logger.trace("Pas de lien localisation acheteur {} {} (contrôle : {} {})", buyerId, buyerName, sirenType, sirenId)

        if dfBuyer is not None:
            codes = {'code': dfBuyer.codeCommuneEtablissement.values.all(),
                     'type': 'code commune',
                     'nom': '',
                     'communeBuyer': dfBuyer.codeCommuneEtablissement.values.all(),
                     'final': '', 'subtype': '', 'country': '', 'region': '', 'departement': '', 'commune': '', 'postal': '',
                     'cedex': '', 'canton': '', 'arrondissement': ''}
    
            codes, buyerLocation = getDeliveryLocation(codes)
            logger.trace("Localisation acheteur {} {} : {}", buyerId, buyerName, codes)
    
            if buyerLocation is not None:
                logger.trace("Connexion acheteur {} {} : {}", buyerId, buyerName, codes)
                buyer.buyerLocation.connect(buyerLocation, {'deliveryLocationTypeId': codes['subtype']})
                buyer.recordLastUpdate = datetime.now(tz= pytz.timezone('CET'))
                buyer.save()
            else:
                logger.trace("Pas de localisation à connecter au noeud {}", buyer)
        else:
            buyer.buyerLegalData = False
            buyer.recordLastUpdate = datetime.now(tz= pytz.timezone('CET'))
            buyer.save()
    else:
        # des liens avec la localisation de l'acheteur existent, ce qui implique l'existence de ses informations légales
        # on confirme cette propriété du noeud acheteur
        buyer.buyerLegalData = True
        buyer.recordLastUpdate = datetime.now(tz= pytz.timezone('CET'))
        buyer.save()


# In[12]:


def refactorSite(dictId, pattern):
    """
    """
    countNodes = 0
    countRelations = 0
    enterprises = Enterprise.nodes.filter(Q(titulaireId__contains= '.') & Q(titulaireId__regex= pattern))
    # enterprises = Enterprise.nodes.filter(Q(titulaireId__regex= pattern))
    for ent in enterprises:
        rightId = ent.titulaireId.split('.')[0]
        candidateIds = [c for c in okId if rightId in c]
        dictId[ent.titulaireId] = candidateIds

        if len(candidateIds) == 0:
            # a priori pas de noeud valide déjà existant dans la base, on le teste, sinon on crée et on migre
            # d'abord on corrige le numéro
            if len(rightId) in [8, 13]:
                # manque un digit, probablement un zéro initial supprimé par une conversion au format float
                rightId = '0' + rightId  # on le rajoute
            if len(rightId) == 9:
                # on a probablement affaire en fait à un n° SIREN
                try:
                    df = search_sirene(variable = ['siren'], number= 1, pattern = [rightId], phonetic_search = False,
                                       legal=True, closed=True)
                    rightId = df.siret[0]
                except RequestException:
                    pass
        
            try:
                goodEnt = Enterprise.nodes.get(titulaireId = rightId)
            except DoesNotExist:
                goodEnt = addEnterpriseWithSiret(sirenId= rightId, typeId= 'SIRET')
                goodEnt.save()

        elif len(candidateIds) >= 1:
            # il devrait exister dans la base une seule entreprise avec le code siret correct, on rattache le noeud
            try:
                goodEnt = Enterprise.nodes.get(titulaireId = candidateIds[0])
            except DoesNotExist:
                # siret existe dans decp mais n'a pas déjà été créé dans la base, on tente de le créer
                goodEnt = addEnterpriseWithSiret(sirenId= candidateIds[0], typeId= 'SIRET')
                goodEnt.save()

        countNodes += 1
        countRelations += migrateEnterpriseNode(old= ent, new= goodEnt)

    return countNodes, countRelations


def migrateEnterpriseNode(old, new):
    """
    """
    countRelations = 0
    for contract in old.publicContract.all():
        countRelations += 1
        new.publicContract.connect(contract)
    for siege in old.siege.all():
        countRelations += 1
        new.siege.connect(siege)
    for location in old.enterpriseLocation.all():
        countRelations += 1
        new.enterpriseLocation.connect(location)
    old.publicContract.disconnect_all()
    old.siege.disconnect_all()
    old.enterpriseLocation.disconnect_all()

    new.save()
    old.delete()

    return countRelations


# In[13]:


def connectToCpv(contractNode: Contract, code: str):
    """
    Requête Cypher : getCodeInVersion =
    "MATCH(c:CPV), (v:CPV) WHERE c.code = '{}' AND v.versionCPV = '{}' AND ((c:CPV)-[:IS_IN_CATEGORY*1..5]-(v:CPV)) RETURN c"
    """
    if code is None:
        logger.trace("code None")
    else:
        # élision du code de contrôle éventuel
        code = code.split('-')[0]
    
        # suppression des INX
        toRemove = ['INX ']
        for substring in toRemove:
            if code.find(substring) != -1:
                code = code[len(substring):]
                logger.trace("Suppression INX pour code {}", code)
        
        try:
            cpvNode = None
            request = getCodeInVersion.format(code, '2008')
            result = db.cypher_query(request, resolve_objects=True)
            if len(result[0]) > 1:
                logger.trace("Multiple noeuds CPV ({}) lors de la connexion du contrat à la catégorie CPV {} en version {}",
                             len(result[0]), code, '2008')
                cpvNode = result[0][0][0]
            elif len(result[0]) == 0:
                # on recherche s'il ne s'agit pas d'un ancien code 2003
                try:
                    request = getCodeInVersion.format(code, '2003')
                    result = db.cypher_query(request, resolve_objects=True)
                    if len(result[0]) > 1:
                        logger.trace("Multiple noeuds CPV ({}) lors de la connexion du contrat à la catégorie CPV {} en version {}",
                                     len(result[0]), code, '2003')
                        cpvNode = result[0][0][0]
                    elif len(result[0]) == 0:
                        logger.trace("Catégorie CPV {} inexistante", code)
                    else:
                        cpvNode = result[0][0][0]
                    if cpvNode is not None:
                        contractNode.cpv.connect(cpvNode)
                except CypherSyntaxError:
                    logger.trace('Requête Cypher incorrecte : {}', request)
            else:
                # c'est le cas normalement attendu
                cpvNode = result[0][0][0]
            if cpvNode is not None:
                contractNode.cpv.connect(cpvNode)
        except CypherSyntaxError:
            logger.trace('Requête Cypher incorrecte : {}', request)


# In[14]:


def connectToNaf(enterprise: Enterprise, code: str):
    """
    """
    if enterprise.activitePrincipaleEtablissement is not None:
        try:
            toNafNode = NAF.nodes.get(code= enterprise.activitePrincipaleEtablissement)
            enterprise.naf.connect(toNafNode)
    
        except DoesNotExist:
            logger.trace("Code NAF inconnu : {} pour entreprise {}", enterprise.activitePrincipaleEtablissement,
                         enterprise.titulaireId)
            unknown.append(enterprise.activitePrincipaleEtablissement)
    
    return None


# In[15]:


def addInvContract(contract: pd.Series):
    """
    """
    # identification des titulaires
    idsTitulaires = {'titulaire_id_1': 'titulaire_denominationSociale_1',
                     'titulaire_id_2': 'titulaire_denominationSociale_2',
                     'titulaire_id_3': 'titulaire_denominationSociale_3'}
    id2type       = {'titulaire_id_1': 'titulaire_typeIdentifiant_1',
                     'titulaire_id_2': 'titulaire_typeIdentifiant_2',
                     'titulaire_id_3': 'titulaire_typeIdentifiant_3'}
    idsBuyer = {'acheteur.id': 'acheteur.nom'}
    
    contract = checkEnterprises(contract, idsTitulaires, id2type)
    if not contract[list(idsTitulaires.keys())].isna().all():
        # identification de l'acheteur
        contract = checkEnterprises(contract, idsBuyer)
        if not contract[list(idsBuyer.keys())].isna().all():
            return addContract(contract)
        else:
            # aucun acheteur identifiable, on ne crée pas les objets associés à cette ligne du dataset
            logger.trace("Aucun acheteur identifiable : {}, ",
                         list(contract[idsBuyer.keys()].values),
                         list(contract[idsBuyer.values()].values)
                        )
            return None
    else:
        # aucune entreprise identifiable, on ne crée pas les objets associés à cette ligne du dataset
        logger.trace("Aucune entreprise identifiable : {}",
                     list(contract[idsTitulaires.keys()].values),
                     list(contract[idsTitulaires.values()].values)
                    )
        return None

