#!/usr/bin/env python
# coding: utf-8

# In[2]:


session = {}

# colonnes non représentées par une chaîne de caractères. format float retenu pour supporter la présence de NaN
colonnesNum = {'montant': float, 'dureeMois': float, 'offresRecues': float}

# colonnes représentant un booléen, pour traitement lors du chargement (oui/non, true/false...)
colonnesBool = ['marcheInnovant', 'attributionAvance', 'sousTraitanceDeclaree']

# liste des colonnes lues dans le dataset original, données au format v3
useColsV3 = ['titulaire_denominationSociale_1', 'titulaire_id_1', 'titulaire_typeIdentifiant_1',
             'titulaire_denominationSociale_2', 'titulaire_id_2', 'titulaire_typeIdentifiant_2',
             'titulaire_denominationSociale_3', 'titulaire_id_3', 'titulaire_typeIdentifiant_3',
             'procedure', 'nature', 'codeCPV', 'dureeMois', 'datePublicationDonnees', 'id', 
             'formePrix', 'dateNotification', 'objet', 'montant', 'acheteur.id', 'acheteur.nom', 'source',
             'lieuExecution.code', 'lieuExecution.typeCode', 'lieuExecution.nom',
             'considerationsSociales', 'considerationsEnvironnementales',
             'technique', 'modaliteExecution', 'marcheInnovant', 'ccag', 'offresRecues', 'attributionAvance',
             'typeGroupementOperateurs', 'sousTraitanceDeclaree', 'idAccordCadre',
             'TypePrix', 'typesPrix', 'typePrix', 'tauxAvance', 'origineUE', 'origineFrance']

# colonnes renommées, supprimées, ajoutées dans le dataset constitué depuis le dataset original v3
renameColsV3 = {'technique': 'techniques', 'modaliteExecution': 'modalitesExecution'}
dropColsV3   = ['TypePrix', 'typePrix']
addColsV3    = []

# liste des colonnes lues dans le dataset original, données au format 2022 et désigné ici par v4
useColsV4 = ['titulaire_id_1', 'titulaire_typeIdentifiant_1', 'titulaire_id_2', 'titulaire_typeIdentifiant_2',
            'titulaire_id_3', 'titulaire_typeIdentifiant_3', 'id', 'nature', 'objet', 'codeCPV', 'procedure','dureeMois',
            'dateNotification', 'datePublicationDonnees', 'montant', 'formePrix', 'attributionAvance', 'offresRecues',
            'marcheInnovant', 'ccag', 'sousTraitanceDeclaree', 'typeGroupementOperateurs', 'idAccordCadre', 'source',
            'acheteur.id', 'lieuExecution.code', 'lieuExecution.typeCode', 'considerationsSociales',
            'considerationsEnvironnementales', 'modalitesExecution', 'techniques', 'typesPrix', 'tauxAvance', 'origineUE',
            'origineFrance']

# colonnes renommées, supprimées, ajoutées dans le dataset constitué depuis le dataset original v4
renameColsV4 = {}
dropColsV4 = []
addColsV4 = ['titulaire_denominationSociale_1', 'titulaire_denominationSociale_2', 'titulaire_denominationSociale_3',
             'lieuExecution.nom', 'acheteur.nom']

colsConsiderationsSociales = ['Pas de considération sociale', 'Clause sociale', 'Critère social', 'Marché réservé']

colsConsiderationsEnvironnementales = ['Pas de considération environnementale',
                                       'Critère environnemental',
                                       'Clause environnementale']

colsModaliteExecution = ['Bons de commande', 'Tranches', 'Marchés subséquents', 'Mixte']

varsModaliteExecution = ['commande', 'tranche', 'subséquent', 'Mixte']

colsTechniques = ['Accord-cadre', 'Catalogue électronique', 'Concours', 'Acquisition dynamique',
                  'Système de qualification', 'Enchère électronique']

varsTechniques = ['Accord-cadre', 'Catalogue électronique', 'Concours', 'acquisition dynamique',
                  'Système de qualification', 'Enchère électronique']

valTypesPrix = ['Définitif ferme', 'Définitif révisable', 'Définitif actualisable', 'Provisoire']

colsContractBaseOptions = ['nature', 'codeCPV', 'procedure','dureeMois', 'id', 'formePrix', 'dateNotification', 'objet',
                           'montant', 'source', 'technique', 'modaliteExecution', 'idAccordCadre', 'marcheInnovant', 
                           'ccag', 'offresRecues', 'attributionAvance', 'typeGroupementOperateurs', 'origineUE', 
                           'origineFrance', 'sousTraitanceDeclaree', 'tauxAvance', 'typesPrix']


