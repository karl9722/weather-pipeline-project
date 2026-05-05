Weather Data Pipeline avec Dagster
Équipe projet
HABRAN Karl
SEBASTIAO Luna
NGNIDJIE Alex-Claudel
Description du projet

Ce projet consiste à concevoir et déployer une pipeline de données météo de bout en bout orchestrée avec Dagster.

L’objectif est de :

Collecter des données météo multi-villes via des APIs
Stocker les données dans PostgreSQL
Transformer les données avec dbt
Produire des indicateurs analytiques avec Power BI

Le projet suit une logique ELT (Extract → Load → Transform).

APIs utilisées
Open-Meteo : données météo
Nominatim : géolocalisation des villes

Justification :

Nominatim permet d’obtenir les coordonnées à partir d’une ville
Open-Meteo utilise ces coordonnées pour fournir les données météo

Cela permet de construire une pipeline cohérente de la localisation jusqu’à l’analyse.

Données manipulées

Entités :

Villes géolocalisées
Données météo

Champs principaux :

Ville, département
Latitude, longitude
Température, humidité, précipitations, vent
Stack technique
Python 3.11
Dagster
PostgreSQL
dbt
Docker / Docker Compose
Power BI
Lancement du projet

1. Lancer PostgreSQL

docker-compose up -d

2. Lancer Dagster

dagster dev
Pipeline de données

Extract

Récupération des villes
Géolocalisation via Nominatim
Appel API Open-Meteo

Load

Insertion des données brutes dans PostgreSQL

Transform

Nettoyage des données
Standardisation des formats
Création de métriques
Orchestration avec Dagster

Assets

get_coordinates
get_raw_meteo_data
clean_meteo_data
meteo_metrics

Jobs

Collecte des données
Transformation
Nettoyage des données anciennes

Schedules

Toutes les heures : ingestion
Quotidien : calcul des métriques

Partitions

Partition par date
dbt – Transformation des données

Modèles :

Staging : données nettoyées
Mart : données analytiques

Exemples de métriques :

Température moyenne
Précipitations totales
Min / Max par ville
Dashboard Power BI

Indicateurs :

Pluie élevée
Vent fort
Variation de température
Monitoring et tests

Trois niveaux :

Dagster : suivi du pipeline
dbt tests : qualité des données
PostgreSQL : audit et logs
Risques

Nominatim

Limite de requêtes
Données crowdsourcées

Open-Meteo

Temps de réponse variable
Incohérences possibles
Objectif final

Construire une solution complète permettant :

Analyse météo multi-villes
Aide à la décision
