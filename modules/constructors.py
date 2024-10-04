#!/usr/bin/env python
# coding: utf-8

# In[11]:


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



# In[2]:


if '__file__' not in globals():
    sys.path.append(str(Path().absolute().parent))

from modules.config import session
from modules.neomodel_classes import *
from modules.location_management import getDeliveryLocation, getLocationCode
from modules.location_constructors import getCity, getRegionFromDepartement


# In[5]:


import json


# In[10]:


parser.isoparse('2023-12-18').year


# In[3]:


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
                           pd.Series(index= range(0, 3)),
                           pd.Series(index= range(0, 3)),
                           pd.Series(index= range(0, 3))],
                           keys= ['SIRET', 'DS', 'TYPE_ID', 'NODE', 'SIRET_SIEGE', 'NODE_SIEGE'],
                           axis= 1)
    # création des titulaires
    titulaires = titulaires[titulaires.SIRET.notna()]   # on ne conserve que les valeurs renseignées
    titulaires = titulaires.apply(addEnterprise, axis= 1)

    # création des noeuds sièges si distincts des titulaires
    sieges = titulaires[titulaires.SIRET_SIEGE.notna()]
    sieges['SIRET'] = sieges['SIRET_SIEGE']
    sieges = sieges.apply(addEnterprise, axis= 1)
    # création des liens avec les sièges
    sieges.apply(lambda x: x.NODE.siege.connect(x.NODE_SIEGE), axis= 1)
    
    contractKey = {'year': parser.isoparse(contract.datePublicationDonnees).year,
                   'id': contract.id,
                   'titulaire': contract.titulaire_id_1,
                   'montant': contract.montant,
                   'objet': contract.objet}
    # création du contrat
    try:
        contractNode = Contract.nodes.get(key = contractKey)
    except DoesNotExist:
        contractNode = Contract(modelVersion = modelVersion,
                                key = contractKey,
                                idContract = contract.id,
                                objet = contract.objet,
                                procedure = contract.procedure,
                                montant = contract.montant
                               ).save()

    # création du noeud Partnership si plusieurs titulaires
    if titulaires.SIRET[titulaires.SIRET.notna()].count() > 1:
        try:
            groupement = Partnership.nodes.get(idContract = contract.id)
        except DoesNotExist:
            groupement = Partnership(modelVersion = modelVersion,
                                     idContract= contract.id,
                                     typeGroupementOperateurs= contract.typeGroupementOperateurs).save()
            # création des relations cocontractantes
            titulaires.apply(lambda x: x.NODE.coContractor.connect(groupement), axis=1)
    
            # création du lien avec le contrat
            groupement.publicContract.connect(contractNode)
    else:
        # 1 seul titulaire
        titulaires[titulaires.SIRET.notna()].apply(lambda x: x.NODE.publicContract.connect(contractNode),
                                                   axis= 1)

    # mise à jour du noeud contrat
    colsContractOptions = ['nature', 'codeCPV', 'procedure','dureeMois', 'id', 'formePrix', 'dateNotification', 'objet',
                       'montant', 'source', 'technique', 'modaliteExecution', 'idAccordCadre', 'marcheInnovant', 
                       'ccag', 'offresRecues', 'attributionAvance', 'typeGroupementOperateurs', 'origineUE', 
                       'origineFrance', 'sousTraitanceDeclaree', 'actesSousTraitance', 
                       'modificationsActesSousTraitance', 'TypePrix', 'tauxAvance', 'typePrix', 'booleanModification']
    contractOptions = contract[colsContractOptions]
    cols = contractOptions[contractOptions.notna()].index
    for col in cols:
        match col:
            case 'nature'                   : contractNode.nature = contract.nature
            case 'codeCPV'                  : contractNode.codeCPV = contract.codeCPV
            case 'dureeMois'                : contractNode.dureeMois = contract.dureeMois
            case 'formePrix'                : contractNode.formePrix = contract.formePrix
            case 'objet'                    : contractNode.objet = contract.objet
            case 'source'                   : contractNode.source = contract.source
            case 'technique'                : contractNode.technique = contract.technique
            case 'modaliteExecution'        : contractNode.modaliteExecution = contract.modaliteExecution
            case 'idAccordCadre'            : contractNode.idAccordCadre = contract.idAccordCadre
            case 'marcheInnovant'           : contractNode.marcheInnovant = contract.marcheInnovant
            case 'ccag'                     : contractNode.ccag = contract.ccag
            case 'offresRecues'             : contractNode.offresRecues = contract.offresRecues
            case 'attributionAvance'        : contractNode.attributionAvance = contract.attributionAvance
            case 'origineUE'                : contractNode.origineUE = contract.origineUE
            case 'origineFrance'            : contractNode.origineFrance = contract.origineFrance
            case 'sousTraitanceDeclaree'    : contractNode.sousTraitanceDeclaree = contract.sousTraitanceDeclaree
            case 'actesSousTraitance'       : contractNode.actesSousTraitance = contract.actesSousTraitance
            case 'modificationsActesSousTraitance' : 
                contractNode.modificationsActesSousTraitance = contract.modificationsActesSousTraitance
            case 'TypePrix'                 : contractNode.TypePrix = contract.TypePrix
            case 'tauxAvance'               : contractNode.tauxAvance = contract.tauxAvance
            case 'typePrix'                 : contractNode.typePrix = contract.typePrix
            case 'booleanModification'      : contractNode.booleanModification = contract.booleanModification
            case _                          : pass             
           
    # mise à jour des dates au format datetime.date, en évitant les valeurs NaN
    colsDatesContract = ['dateNotification', 'datePublicationDonnees', 'created_at', 'updated_at']
    datesContract = contract[colsDatesContract]
    cols = datesContract[datesContract.notna()].index
    for col in cols:
        match col:
            case 'dateNotification'      : contractNode.dateNotification = parser.isoparse(contract.dateNotification)
            case 'datePublicationDonnees': contractNode.datePublicationDonnees = parser.isoparse(contract.datePublicationDonnees)
            case 'created_at'            : contractNode.created_at = parser.isoparse(contract.created_at)
            case 'updated_at'            : contractNode.updated_at = parser.isoparse(contract.updated_at)
            case _: pass

    contractNode.save()

    # création du lien titulaire -> contrat
    

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


# In[4]:


def addEnterprise(titulaire: pd.Series) -> pd.Series:
    """
    - titulaire : Series avec 6 champs : SIRET', 'DS', 'TYPE_ID', 'NODE', 'SIRET_SIEGE', 'NODE_SIEGE'

    Si le champ SIRET_SIEGE n'est pas NaN, on crée un siège (non titulaire) d'une entreprise titulaire.
    retour : Series titulaire, avec noeud existant ou créé renseigné dans la colonne 'NODE'.
    """

    siret = str(int(titulaire.SIRET))
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
                    case 'dateDebut'                           : enterprise.dateDebut = parser.isoparse(*df[col])
                    case 'dateCreationEtablissement'           :
                        enterprise.dateCreationEtablissement = parser.isoparse(*df[col])
                    case 'dateCreationUniteLegale'             :
                        enterprise.dateCreationUniteLegale = parser.isoparse(*df[col])
                    case 'dateFin'                             : enterprise.dateFin = parser.isoparse(*df[col])                        ,
                    case 'denominationUniteLegale'             : enterprise.denominationUniteLegale = str(*df[col])
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
                                    isSiege = True)


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

