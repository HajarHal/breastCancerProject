# breastCancerProject
Breast Cancer Knowledge Graph Visualization
Table of Contents
Description
Technologies
Installation
ETL Pipeline
Database Setup
Graph Visualization
Flask Application
Usage


Description
Ce projet vise à créer une application web permettant de visualiser les graphes de connaissance liés au cancer du sein. L'application prend un ensemble de données brutes et le transforme en quatre catégories principales : causes, traitements, prévention et diagnostic. Elle couvre trois types de cancer : féminin, masculin et récurrent.

Le processus commence par l'utilisation de techniques d'apprentissage machine pour classer les données en différentes catégories. Cela permet une représentation graphique plus claire et informative. Les données classées sont ensuite chargées dans une base de données PostgreSQL, facilitant ainsi leur gestion et leur accès.

En utilisant la bibliothèque PyVis, des graphes interactifs sont générés pour représenter les relations entre les différents éléments du dataset, offrant ainsi une visualisation dynamique des causes, traitements, prévention et diagnostic du cancer du sein.

Technologies
Python
Flask
PostgreSQL
Docker
PyVis
Pandas
scikit-learn

ETL Pipeline
L'ETL (Extract, Transform, Load) pipeline a été développé pour transformer le dataset initial en quatre tables distinctes : causes, traitements, prévention et diagnostic. Chaque table est spécifiquement dédiée à un type de cancer : féminin, masculin et récurrent.

Extraction : Le pipeline extrait les données à partir d'une source initiale.
Transformation : Les données sont nettoyées et classées en utilisant des algorithmes d'apprentissage machine, facilitant ainsi la catégorisation des informations.
Chargement : Les données transformées sont chargées dans une base de données PostgreSQL, garantissant une gestion efficace des données.
Database Setup
Les données sont organisées dans quatre tables distinctes dans une base de données PostgreSQL. Les tables créées sont :

causes
treats
prevents
diagnoses
Cela permet une récupération rapide et efficace des informations pour la visualisation et l'analyse.

Graph Visualization
Les graphes sont générés à l'aide de la bibliothèque PyVis, permettant de créer des visualisations interactives. Un DAG (Directed Acyclic Graph) a été mis en place pour produire des graphes pour chaque type de cancer et chaque catégorie, facilitant ainsi la compréhension des relations entre les différents éléments.

Flask Application
Une application web a été développée avec Flask pour afficher les graphes générés. L'interface utilisateur est conçue pour être intuitive et interactive, permettant aux utilisateurs d'explorer facilement les différentes visualisations.

Usage
Accédez à l'application via votre navigateur à l'adresse https://breast-cancer-app-g9za.vercel.app/ .
Explorez les graphes en fonction des différents types de cancer et des catégories.


