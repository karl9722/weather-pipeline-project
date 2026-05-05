## Projet : pipeline de donnees meteo avec Dagster

## Fait par

HABRAN Karl - SEBASTIAO Luna - NGNIDJIE Alex-Claudel

## Description du projet

Ce projet consiste a concevoir et deployer une pipeline de donnees meteo de bout en bout en utilisant Dagster comme orchestrateur.

L'objectif est de collecter des donnees meteo multi-villes via des APIs externes, de les stocker dans une base PostgreSQL, de les transformer avec dbt, puis de les exploiter dans un dashboard Power BI afin de produire des indicateurs analytiques et decisionnels.

API choisie :

- Open-Meteo
- Nominatim

## Donnees manipulees

### Entites

- Villes geolocalisees
- Donnees meteo

### Champs principaux

- Ville, departement
- Latitude, longitude
- Temperature
- Humidite
- Precipitations
- Vent

---

## Stack technique

- Python 3.11
- Dagster
- PostgreSQL
- dbt
- Docker / Docker Compose
- Power BI

---

## Installation et lancement

### 1. Lancer PostgreSQL avec Docker

```bash
docker-compose up -d
```

### 2. Lancer Dagster

```bash
dagster dev
```

Avant le lancement, configurer la variable d'environnement `DATABASE_URL`.

---

## Pipeline de donnees

### Extract

- Recuperation des villes
- Geolocalisation via Nominatim
- Appel de l'API Open-Meteo

### Load

- Insertion des donnees brutes dans PostgreSQL

### Transform

- Nettoyage des donnees
- Standardisation des formats
- Creation de metriques

---

## Orchestration avec Dagster

### Assets

- `get_coordinates`
- `get_raw_meteo_data`
- `clean_meteo_data`
- `meteo_metrics`

### Jobs

- Collecte des donnees meteo
- Transformation des donnees
- Nettoyage des donnees anciennes

### Schedules

- Toutes les heures : ingestion des donnees meteo
- Tous les jours : calcul des metriques

### Partitions

- Partition par date pour historiser les donnees

---

## dbt - Transformation des donnees

### Modeles

- Staging : donnees nettoyees
- Mart : donnees analytiques

### Exemples de metriques

- Temperature moyenne
- Precipitations totales
- Minimum / maximum par ville

---

## Dashboard Power BI

### Indicateurs suivis

- Pluie elevee
- Vent fort
- Variation de temperature

---

## Monitoring et tests

Architecture en trois couches :

- Dagster : suivi du pipeline
- dbt tests : qualite des donnees
- PostgreSQL : audit et logs

---

## Risques connus

### Nominatim

- Limite : 1 requete par seconde
- Donnees crowdsourcees

### Open-Meteo

- Temps de reponse variable
- Incoherences possibles entre sources

### Plan B

- Changer d'API meteo si necessaire
- Ajouter un systeme de cache ou un stockage intermediaire

---

## Repartition des roles

| Membre | Responsabilites |
| --- | --- |
| Karl | Power BI + PostgreSQL |
| Luna | Dagster |
| Alex-Claudel | dbt |
| Tous | README + soutenance |
