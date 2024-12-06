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
from datetime import date
from dateutil import parser

from neo4j import GraphDatabase

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



# In[15]:


if '__file__' not in globals():
    sys.path.append(str(Path().absolute().parent))

from modules.config import *
from modules.neomodel_classes import *
from modules.location_management import getDeliveryLocation, getLocationCode
from modules.location_constructors import getCity, getRegionFromDepartement


# In[4]:


import json


# In[13]:


def addContract(contract: pd.Series):
    """
    - contract : ligne de dataframe contenant les informations relatives à un contrat. 
    """
   
    col0 = ['titulaire_id_1', 'titulaire_id_2', 'titulaire_id_3']
    col1 = ['titulaire_denominationSociale_1', 'titulaire_denominationSociale_2', 'titulaire_denominationSociale_3']
    col2 = ['titulaire_typeIdentifiant_1', 'titulaire_typeIdentifiant_2', 'titulaire_typeIdentifiant_3']

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

    titulaires = titulaires.apply(addEnterprise, axis= 1)

    # création des noeuds sièges si distincts des titulaires
    sieges = titulaires[titulaires.SIRET_SIEGE.notna()]
    if sieges.size != 0:
        sieges['SIRET'] = sieges['SIRET_SIEGE']
        sieges = sieges.apply(addEnterprise, axis= 1)
        # création des liens avec les sièges
        sieges.apply(lambda x: x.NODE.siege.connect(x.NODE_SIEGE), axis= 1)
    

    # création du contrat

    contractKey = {'year': parser.parse(contract.dateNotification, fuzzy= True).year,
                   'id': contract.id,
                   'titulaire': contract.titulaire_id_1,
                   'montant': contract.montant,
                   'objet': contract.objet}
    try:
        contractNode = Contract.nodes.get(key = contractKey)
    except DoesNotExist:
        contractNode = Contract(modelVersion = modelVersion,
                                key = contractKey,
                                idContract = contract.id,
                                year  = str(parser.parse(contract.dateNotification, fuzzy= True).year),
                                month = str(parser.parse(contract.dateNotification, fuzzy= True).month),
                                day   = str(parser.parse(contract.dateNotification, fuzzy= True).day),
                                objet = contract.objet,
                                procedure = contract.procedure,
                                montant = contract.montant
                               ).save()

    # création du noeud Partnership si plusieurs titulaires
    if titulaires.SIRET[titulaires.SIRET.notna()].count() > 1:
        try:
            groupement = Partnership.nodes.get(idContract = contractKey)
        except DoesNotExist:
            groupement = Partnership(modelVersion = modelVersion,
                                     idContract= contractKey,
                                     typeGroupementOperateurs= contract.typeGroupementOperateurs,
                                     year  = str(parser.parse(contract.dateNotification, fuzzy= True).year),
                                     month = str(parser.parse(contract.dateNotification, fuzzy= True).month),
                                     day   = str(parser.parse(contract.dateNotification, fuzzy= True).day)).save()

        # création des relations cocontractantes
        titulaires.apply(lambda x: x.NODE.coContractor.connect(groupement), axis=1)
    
        # création du lien avec le contrat
        groupement.publicContract.connect(contractNode)
    else:
        # 1 seul titulaire
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
            case 'nature'                   : contractNode.nature = contract[col]
            case 'codeCPV'                  : contractNode.codeCPV = contract[col]
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

    # création du noeud acheteur
    siretBuyer = str(contract['acheteur.id'])[0:14]

    # traitement du cas où l'identité de l'acheteur n'est pas renseignée.
    # par défaut on prendra la dénomination sociale correspondant au SIRET.
    buyerName = str(contract['acheteur.nom'])
    buyerId = str(contract['acheteur.id'])
    try:
        dfBuyer = search_sirene(variable = ['siret'], number= 1,
                                pattern = [siretBuyer], phonetic_search = False, legal=True, closed=True)
        # cette API peut renvoyer plusieurs lignes pour un même SIRET, même si number= 1.
        # on ne conserve que la première ligne retournée, en conservant néanmoins le format dataframe.
        dfBuyer = dfBuyer.iloc[0, :]
        dfBuyer = dfBuyer.to_frame().transpose()
        if contract.isna()['acheteur.nom']:
            buyerName = str(*dfBuyer.denominationUniteLegale)
        if dfBuyer.isna().denominationUniteLegale.all():
            buyerName = 'not populated in SIRENE'
        
    except RequestException:
        buyerName = 'unknown in SIRENE'
        dfBuyer = None

    if dfBuyer is not None:
        communeBuyer = dfBuyer.codeCommuneEtablissement.values.all()
    else:
        communeBuyer = ''
        
    # recherche si le noeud correspondant à la clé (siretBuyer, buyerName) existe déjà
    try:
        buyer = Buyer.nodes.get(buyerId = siretBuyer, buyerName = buyerName)
        if buyer.modelVersion != modelVersion:
            # on choisit de mettre à jour les informations du noeud
            raise DoesNotExist('buyer')

    except DoesNotExist:
        buyer = Buyer(modelVersion = modelVersion,
                      buyerId    = siretBuyer,
                      buyerSiren = siretBuyer[0:9],
                      buyerSite  =  siretBuyer[9:14],
                      buyerName  = buyerName
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
    entBuyer.apply(lambda x: buyer.buyerEnterprise.connect(x.NODE), axis= 1)
    
    # création de l'entité siège de l'acheteur 
    siegeBuyer = entBuyer[entBuyer.SIRET_SIEGE.notna()]
    if siegeBuyer.size != 0:
        siegeBuyer['TYPE_ID'] = 'SIRET'
        siegeBuyer['SIRET'] = siegeBuyer['SIRET_SIEGE']
        siegeBuyer = siegeBuyer.apply(addEnterprise, axis= 1)

        # création du lien entreprise acheteur -> siège entreprise acheteur
        siegeBuyer.apply(lambda x: x.NODE.siege.connect(x.NODE_SIEGE), axis= 1)

    # création du lien acheteur -> contrat
    buyer.managedContract.connect(contractNode)

    # création du lieu d'exécution
    codes = {'code': contract['lieuExecution.code'],
             'type': unidecode(str(contract['lieuExecution.typeCode']).lower()), 
             'nom': contract['lieuExecution.nom'],
             'communeBuyer': communeBuyer,
             'final': '', 'subtype': '', 'country': '', 'region': '', 'departement': '', 'commune': '', 'postal': '',
             'cedex': '', 'canton': '', 'arrondissement': ''}
    
    codes, deliveryLocation = getDeliveryLocation(codes)

    # création du lien contrat -> lieu d'exécution
    if deliveryLocation != None:
        contractNode.deliveryLocation.connect(deliveryLocation, {'deliveryLocationTypeId': codes['subtype']})
    return


# In[14]:


def addEnterprise(titulaire: pd.Series) -> pd.Series:
    """
    - titulaire : Series avec 6 champs : SIRET', 'DS', 'TYPE_ID', 'NODE', 'SIRET_SIEGE', 'NODE_SIEGE'

    Si le champ SIRET_SIEGE n'est pas NaN, on crée un siège (non titulaire) d'une entreprise titulaire.
    retour : Series titulaire, avec noeud existant ou créé renseigné dans la colonne 'NODE'.
    """

    siret = titulaire.SIRET

    # vérifier si le site existe déjà
    try:
        enterprise = Enterprise.nodes.get(titulaireId = siret)
        if enterprise.modelVersion != modelVersion:
            # on choisit de mettre à jour les informations du noeud
            raise DoesNotExist('enterprise')

    except DoesNotExist:
        # récupérer les informations SIRENE sur la base du SIRET
        try:
            df = search_sirene(variable = ['siret'], number= 1,
                               pattern = [siret], phonetic_search = False, legal=True, closed=True)
            # cette API peut renvoyer plusieurs lignes pour un même SIRET, même si number= 1.
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
                
            # création du noeud Enterprise avec les informations minimales
            enterprise = Enterprise(modelVersion = modelVersion,
                                    titulaireId= siret,
                                    titulaireSiren = siret[0:9],
                                    titulaireSite =  siret[9:14],
                                    titulaireTypeIdentifiant = titulaire.TYPE_ID,
                                    titulaireDenominationSociale = denominationUniteLegale,
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
                    case 'activitePrincipaleEtablissement'     : enterprise.activitePrincipaleEtablissement = str(*df[col])
                    case 'activitePrincipaleEtablissementLibelle':
                        enterprise.activitePrincipaleEtablissementLibelle = str(*df[col])
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

            enterprise.save()

            # récupération du noeud City ou création du noeud City si non existant
            if enterprise.codeCommuneEtablissement is not None:
                codes = {'code': str(enterprise.codeCommuneEtablissement), 'final': 'commune',
                         'type': 'code commune', 'subtype': '', 'country': '', 'region': '', 'departement': '',
                         'commune': str(enterprise.codeCommuneEtablissement), 'postal': '', 'cedex': ''}

                codes = getLocationCode(codes)
                city = getCity(codes)  # city peut être un noeud City ou Cedex, sous-classes de LocationNode
                enterprise.enterpriseLocation.connect(city) # création de la relation avec city

        except RequestException:
            # création du noeud avec informations minimales, le site est considéré siège par défaut
            # il n'est pas rattaché à une localisation.
            enterprise = Enterprise(modelVersion = modelVersion,
                                    titulaireId= siret,
                                    titulaireSiren = siret[0:9],
                                    titulaireSite =  siret[9:14],
                                    titulaireDenominationSociale = 'not known in SIRENE',
                                    titulaireTypeIdentifiant = titulaire.TYPE_ID,
                                    isSiege = True).save()

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

