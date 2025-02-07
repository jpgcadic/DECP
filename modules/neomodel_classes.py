#!/usr/bin/env python
# coding: utf-8

# In[1]:


from neo4j import GraphDatabase

from neomodel import config, db, install_labels, install_all_labels
from neomodel import (StructuredNode,  RelationshipTo, RelationshipFrom, StructuredRel)
from neomodel import (StringProperty, IntegerProperty, FloatProperty, ArrayProperty, BooleanProperty, DateProperty, UniqueIdProperty)
from neomodel import AliasProperty, RegexProperty, JSONProperty, DateTimeProperty, DateTimeFormatProperty
from neomodel.exceptions import *

from neomodel.integration.pandas import to_dataframe, to_series
from neomodel.integration.numpy import to_ndarray



# In[2]:


modelVersion = '1.0.1'

class CommonRecordProperty:
    recordCreationDate = DateTimeFormatProperty(index= True, format="%Y-%m-%d %H:%M:%S")
    recordLastUpdate   = DateTimeFormatProperty(index= True, format="%Y-%m-%d %H:%M:%S")
    modelVersion       = StringProperty(required=True)

class LocationRel(StructuredRel):
    deliveryLocationTypeId = StringProperty()
    

class Enterprise(StructuredNode, CommonRecordProperty):
    titulaireId                           = StringProperty(unique_index=True, required=True)
    titulaireSiren                        = StringProperty(unique_index=False, required=True)
    titulaireSite                         = StringProperty(unique_index=False, required=True)
    titulaireDenominationSociale          = StringProperty(unique_index=False, required=True)
    titulaireDenominationOriginale        = StringProperty(unique_index=False, required=False)
    titulaireTypeIdentifiant              = StringProperty(unique_index=False, required=True)
    isSiege                               = BooleanProperty(unique_index=False, required=True)
    dateDebut                             = DateProperty(unique_index=False, required=False)
    dateCreationEtablissement             = DateProperty(unique_index=False, required=False)
    dateCreationUniteLegale               = DateProperty(unique_index=False, required=False)
    dateFin                               = DateProperty(unique_index=False, required=False)
    categorieEntreprise                   = StringProperty(unique_index=False, required=False)
    categorieJuridiqueUniteLegale         = StringProperty(unique_index=False, required=False)
    activitePrincipaleUniteLegale         = StringProperty(unique_index=False, required=False)
    activitePrincipaleEtablissement       = StringProperty(unique_index=False, required=False)
    numeroVoieEtablissement               = StringProperty(unique_index=False, required=False)
    typeVoieEtablissement                 = StringProperty(unique_index=False, required=False)
    libelleVoieEtablissement              = StringProperty(unique_index=False, required=False)
    codePostalEtablissement               = StringProperty(unique_index=False, required=False)
    libelleCommuneEtablissement           = StringProperty(unique_index=False, required=False)
    codeCommuneEtablissement              = StringProperty(unique_index=False, required=False)
    statutDiffusionEtablissement          = StringProperty(unique_index=False, required=False)
    trancheEffectifsEtablissement         = StringProperty(unique_index=False, required=False)
    anneeEffectifsEtablissement           = StringProperty(unique_index=False, required=False)
    dateDernierTraitementEtablissement    = DateProperty(unique_index=False, required=False)
    nombrePeriodesEtablissement           = IntegerProperty(unique_index=False, required=False)
    etatAdministratifUniteLegale          = StringProperty(unique_index=False, required=False)
    statutDiffusionUniteLegale            = StringProperty(unique_index=False, required=False)
    sigleUniteLegale                      = StringProperty(unique_index=False, required=False)
    nomenclatureActivitePrincipaleUniteLegale = StringProperty(unique_index=False, required=False)
    economieSocialeSolidaireUniteLegale    = StringProperty(unique_index=False, required=False)
    societeMissionUniteLegale             = StringProperty(unique_index=False, required=False)
    caractereEmployeurUniteLegale         = StringProperty(unique_index=False, required=False)
    trancheEffectifsUniteLegale           = StringProperty(unique_index=False, required=False)
    effectifsMinUniteLegale               = FloatProperty(unique_index=False, required=False)
    effectifsMaxUniteLegale               = FloatProperty(unique_index=False, required=False)
    anneeEffectifsUniteLegale             = StringProperty(unique_index=False, required=False)
    nicSiegeUniteLegale                   = StringProperty(unique_index=False, required=False)
    dateDernierTraitementUniteLegale      = DateProperty(unique_index=False, required=False)
    anneeCategorieEntreprise              = StringProperty(unique_index=False, required=False)
    nomenclatureActivitePrincipaleEtablissement = StringProperty(unique_index=False, required=False)
    caractereEmployeurEtablissement       = StringProperty(unique_index=False, required=False)
    activitePrincipaleRegistreMetiersEtablissement = StringProperty(unique_index=False, required=False)
    
    publicContract            = RelationshipTo('Contract', 'HAS_CONTRACT')
    siege                     = RelationshipTo('Enterprise', 'IS_BRANCH_OF')
    enterpriseLocation        = RelationshipTo('LocationNode', 'IS_LOCATED_IN', model= LocationRel)


class Buyer(StructuredNode, CommonRecordProperty):
    buyerId         = StringProperty(unique_index=False, required=True)
    buyerSiren      = StringProperty(unique_index=False, required=True)
    buyerSite       = StringProperty(unique_index=False, required=True)
    buyerName       = StringProperty(unique_index=False, required=False)
    buyerLegalName  = StringProperty(unique_index=False, required=False)
    buyerLegalData  = BooleanProperty()

    buyerLocation   = RelationshipTo('LocationNode', 'IS_LOCATED_IN', model= LocationRel)
    managedContract = RelationshipTo('Contract', 'MANAGES_CONTRACT')
    buyerEnterprise = RelationshipTo('Enterprise', 'IS_IN_ENTERPRISE')


class Contract(StructuredNode, CommonRecordProperty):
    key                               = JSONProperty(unique_index=True, required=True)
    contractId                        = StringProperty(unique_index=False, required=False)
    year                              = IntegerProperty(unique_index=False, required=True)
    month                             = IntegerProperty(unique_index=False, required=True)
    day                               = IntegerProperty(unique_index=False, required=True)
    procedure                         = StringProperty(unique_index=False, required=True)
    montant                           = FloatProperty(unique_index=False, required=True)
    nature                            = StringProperty(unique_index=False, required=False)
    codeCPV                           = StringProperty(unique_index=False, required=False)
    dureeMois                         = IntegerProperty(unique_index=False, required=False)
    formePrix                         = StringProperty(unique_index=False, required=False)
    dateNotification                  = DateProperty(unique_index=False, required=False)
    objet                             = StringProperty(unique_index=False, required=True)
    source                            = StringProperty(unique_index=False, required=False)
    datePublicationDonnees            = DateProperty(unique_index=False, required=False)
    created_at                        = DateProperty(unique_index=False, required=False)
    technique                         = StringProperty(unique_index=False, required=False)
    modaliteExecution                 = StringProperty(unique_index=False, required=False)
    idAccordCadre                     = StringProperty(unique_index=False, required=False)
    marcheInnovant                    = BooleanProperty(unique_index=False, required=False)
    ccag                              = StringProperty(unique_index=False, required=False)
    offresRecues                      = FloatProperty(unique_index=False, required=False)
    attributionAvance                 = BooleanProperty(unique_index=False, required=False)
    origineUE                         = FloatProperty(unique_index=False, required=False)
    origineFrance                     = FloatProperty(unique_index=False, required=False)
    sousTraitanceDeclaree             = BooleanProperty(unique_index=False, required=False)
    actesSousTraitance                = ArrayProperty(unique_index=False, required=False)
    modificationsActesSousTraitance   = ArrayProperty(unique_index=False, required=False)
    typePrix                          = StringProperty(unique_index=False, required=False)
    tauxAvance                        = FloatProperty(unique_index=False, required=False)
    updated_at                        = DateProperty(unique_index=False, required=False)
    booleanModification               = BooleanProperty(unique_index=False, required=False)

    deliveryLocation                  = RelationshipTo('LocationNode', 'IS_DELIVERED_IN', model= LocationRel)

class MultiPartyContract(Contract):
    typeGroupementOperateurs = StringProperty(unique_index=False, required=True)

    
class LocationNode(StructuredNode, CommonRecordProperty):
    codeTypes     = {'code postal'        : 'code postal',
                     'code departement'   : 'code departement',
                     'code pays'          : 'code pays',
                     'code commune'       : 'code commune',
                     'code region'        : 'code region',
                     'code arrondissement': 'code arrondissement',
                     'code canton'        : 'code canton',
                     'cedex'              : 'CEDEX'
                    }
    code          = StringProperty(unique_index=False, required=True)
    codeType      = StringProperty(unique_index=False, required=True, choices= codeTypes)
    name          = StringProperty(unique_index=False, required=False)


class Address(LocationNode):
    streetNumber = StringProperty(unique_index=False, required=False)
    streetName   = StringProperty(unique_index=False, required=False)
    city         = RelationshipTo('LocationNode', 'IS_LOCATED_IN', model= LocationRel)
    

class City(LocationNode):
    codeCommune   = StringProperty(unique_index= True, required= True)
    codeTypes     = {'code postal'        : 'code postal',
                     'code commune'       : 'code commune'
                    }
    codeType     = StringProperty(unique_index= False, required= False, default= 'code commune', choices= codeTypes)
    postCode     = StringProperty(unique_index=False, required=False)
    cityName     = StringProperty(unique_index=False, required=False)
    region       = RelationshipTo('Region', 'IS_IN_REGION', model= LocationRel)
    departement  = RelationshipTo('Departement', 'IS_IN_DEPARTEMENT', model= LocationRel)


class Cedex(LocationNode):
    codeCedex     = StringProperty(unique_index= True, required= True)
    codeCommune   = StringProperty(unique_index= False, required= False)
    codeTypes     = {'code postal'        : 'code postal',
                     'code commune'       : 'code commune',
                     'cedex'              : 'CEDEX'
                    }
    codeType     = StringProperty(unique_index= False, required= False, default= 'cedex', choices= codeTypes)
    cityName     = StringProperty(unique_index=False, required=False)
    region       = RelationshipTo('Region', 'IS_IN_REGION', model= LocationRel)
    departement  = RelationshipTo('Departement', 'IS_IN_DEPARTEMENT', model= LocationRel)


class Region(LocationNode):
    codeType     = StringProperty(unique_index= False, required= False, default= 'code region')
    regionCode   = StringProperty(unique_index=True, required=True)
    regionName   = StringProperty(unique_index=True, required=True)
    country      = RelationshipTo('Country', 'IS_IN_COUNTRY', model= LocationRel)


class Country(LocationNode):
    # on considère que le code pays est sous format ISO2
    codeFormat  = StringProperty(unique_index= False, required= False, default= 'ISO2')
    codeType    = StringProperty(unique_index= False, required= False, default= 'code pays')
    countryCode = StringProperty(unique_index=True, required=False, default= 'FR')
    countryName = StringProperty(unique_index=False, required=False, default= 'FRANCE')


class Departement(LocationNode):
    codeType          = StringProperty(unique_index= False, required= False, default= 'code departement')
    departementCode   = StringProperty(unique_index=True, required=True)
    departementName   = StringProperty(unique_index=True, required=True)
    country           = RelationshipTo('Country', 'IS_IN_COUNTRY', model= LocationRel)
    region            = RelationshipTo('Region', 'IS_IN_REGION', model= LocationRel)


class Arrondissement(LocationNode):
    codeType          = StringProperty(unique_index= False, required= False, default= 'code arrondissement')
    arrondissementCode   = StringProperty(unique_index=True, required=True)
    arrondissementName   = StringProperty(unique_index=True, required=True)
    country           = RelationshipTo('Country', 'IS_IN_COUNTRY', model= LocationRel)
    region            = RelationshipTo('Region', 'IS_IN_REGION', model= LocationRel)
    departement       = RelationshipTo('Departement', 'IS_IN_DEPARTEMENT', model= LocationRel)


class Canton(LocationNode):
    codeType          = StringProperty(unique_index= False, required= False, default= 'code canton')
    cantonCode        = StringProperty(unique_index=True, required=True)
    cantonName        = StringProperty(unique_index=True, required=True)
    country           = RelationshipTo('Country', 'IS_IN_COUNTRY', model= LocationRel)
    region            = RelationshipTo('Region', 'IS_IN_REGION', model= LocationRel)
    departement       = RelationshipTo('Departement', 'IS_IN_DEPARTEMENT', model= LocationRel)

