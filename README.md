**DECP**
Données Essentielles de la Commande Publique

# Objet et motivations
Les motivations premières de cette exploitation d'un jeu de données public relèvent du test, de l'évaluation et de l'expérimentation:
* mise en oeuvre et test d'une base graphe Neo4j avec des données issues d'un environnement métier
* mise en oeuvre et évaluation d'une bibliothèque python (*neomodel*) permettant la manipulation d'une telle base de données dans un code python
* mise en oeuvre de l'API mise à disposition par l'INSEE au travers, également, d'une bibliothèque python (*pynsee*), en vue de l'enrichissement du jeu de données principal.

La structure du jeu de données principal, tabulaire, se prête bien à une modélisation relationnelle et ne milite pas particulièrement pour une exploitation sous forme graphe. Néanmoins la recherche de patterns au moyen des algorithmes dont on peut disposer sur une infrastructure de base de données graphe est un axe d'exploration additionnel. 

# Jeux de données
## Jeux de données principaux
Ces jeux de données listent les marchés attribués dans le cadre des appels d'offres publics.
Ils sont organisés en enregistrements, 1 ligne = 1 enregistrement = 1 marché, sous réserves de discussions et précisions abordées dans la partie "modélisation".
Les jeux de données principaux (DECP) sont disponibles sous [DECP](https://data.economie.gouv.fr/explore/?sort=modified&q=DECP)
Seuls les jeux de données relatifs aux marchés sont exploités, les trois jeux de données relatifs aux concessions ne sont pas exploités.
Les données présentes dans ces jeux de données sont issues de plusieurs sources, la source de chaque enregistrement est identifiée
dans un de ses champs.
La présence d'un enregistrement dans un jeu de données dépend de deux paramètres :
* L'arrêté définissant le format des données : 22/03/2019 ou 22/12/2022
* La conformité des données de l'enregistrement aux spécifications de l'arrêté.

Un jeu de données de tests est également disponible, il n'a pas encore été exploité à ce stade.

La description des éléments métiers de ces jeux de données n'est pas encore abordée dans cette introduction.

## Jeux de données auxiliaires
Ce jeu de données est enrichi avec des informations relatives à la localisation des entités (titulaires, acheteurs, lieu d'exécution). La base des codes postaux ainsi que les bases du Code Officiel Géographique de l'INSEE (*arrondissements, communes, cantons, départements, régions, collectivités, pays*), d'Eurostat (*base NUTS*) et de la nomenclature ISO des régions (*ISO-3166-2*) sont utilisées. La base Cedex, payante, n'est ici pas exploitée.
La nomenclature régionale ISO est normalement disponible en version payante, ici récupérée "manuellement" puis retraitée.
Ces jeux de données sont disponibles sous :
[nomenclatures INSEE](https://www.insee.fr/fr/information/7766585)

[nomenclature NUTS](https://ec.europa.eu/eurostat/fr/web/nuts)

[nomenclature ISO](https://www.iso.org/obp/ui/fr/#iso:code:3166:FR)


## APIs
Les données relatives aux entreprises sont extraites depuis l'API SIRENE 3.11, sur le portail antérieur au 21-08-2024.

## Licences
DECP, Sirene :  *« Licence Ouverte / Open Licence version 2.0 » conçue par Etalab*
Eurostat : *Commons Universal Public Domain Dedication deed (CC0 1.0)*

# Analyse exploratoire des données
Elle n'apparaît pas dans le code publié ici. Néanmoins ce jeu de donnée mériterait une approche et une présentation un tant soit peu structurée sur ce point.
La qualité des données est très bonne de prime abord. Dans le détail, on peut cependant attirer l'attention sur quelques points :
* Les enregistrements d'un même jeu de données proviennent de plusieurs sources distinctes. Des méthodes et contrôles d'acquisition différents peuvent prévaloir d'une source à l'autre.
* Des valeurs présentes dans certaines colonnes peuvent être, au moins selon les sources, incohérentes. C'est en particulier le cas pour le code du lieu d'exécution, souvent incohérent avec le type de code du lieu d'exécution, et qui témoigne souvent de saisies manuelles. Ce point précis rend passablement complexe la détermination du lieu d'exécution d'un contrat. 

# Chargement
La conception initiale de ce code prévoyait à terme une utilisation interactive via une interface utilisateur sous Streamlit. Le code ici publié a été d'abord utilisé pour charger progressivement une base de donnée Neo4j Aura, puis Neo4j Community on-premise pour des raisons de capacité. Cependant l'implémentation des contrôles de cohérence sous python conjointement avec l'utilisation d'API sous python (Neomodel, Pynsee / Sirene en particulier) en fait une méthode de chargement très inefficace.
Une alternative serait d'extraire les différents noeuds représentants les entités représentées dans des fichiers csv, en y effectuant les contrôles appropriés, puis de construire les relations entre eux dans un second temps. Une copie locale complète de la base SIRENE, sinon une extraction préalable par lots, est alors nécessaires pour bénéficier d'un réel gain de performances.
Compte tenu des contrôles de cohérence à effectuer, ainsi que des différents standards de modèle de données rencontrés (selon l'arrêté auquel ils sont conformes), l'utilisation d'outils tels que *dbt* serait à privilégier. C'est une des étapes suivantes de l'implémentation.
 
# Modélisation
La modélisation se fait par classes et sous-classes de *StructuredNode* dans le code python (module *neomodel_classes*), et une classe *StructuredRel*.
La modélisation se fait assez naturellement avec des objets *Enterprise*, *Buyer*, *Partnership*, *Contract*, *LocatinNode*. De cette dernière classe dérivent *Address*, *Arrondissement*, *City*, *Cedex*, *Canton*, *Departement*, *Region*, *Country*.
Des contraintes d'unicité sont définies sur les principaux identifiants, les contraintes d'unicité sont appliquées sur la base.
L'identification d'un marché (objet *Contract*) est actuellement faite sur la base d'une clé composite codée dans une structure json : {'year': 'id': contract.id, 'titulaire': contract.titulaire_id_1, 'montant': contract.montant, 'objet': contract.objet}
En effet il se trouve que l'id d'un marché n'est pas unique dans un même jeu de données. On retrouve des lignes consécutives pour lesquelles la date de notification, le titulaire, le montant et l'objet sont identiques, et où seule l'identification de l'acheteur diffère. L'interprétation faite dans la courante implémentation est qu'il s'agit d'un même marché, établi au profit d'un groupement d'acheteurs. Cette interprétation semble après coup erronée, à la lecture des arrêtés définissant le format des données devant être publiées par les entités administratives : la clé pertinente serait la combinaison identifiant de l'Acheteur + identifiant du Contrat. C'est également une étape suivante de l'implémentation.

La modélisation sera finalisée ultérieurement sous Oracle Data Modeler.

# Prochaines étapes
Interface sous Streamlit.

Méthode d'ingestion plus proche de l'état de l'art.

Evolution du modèle et rétrofit

Détection de communautés dans le jeu de données.

