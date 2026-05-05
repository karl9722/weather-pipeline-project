## Project Pipeline de Données avec Dagster

## Fait par:

HABRAN Karl  -  SEBASTIAO Luna – NGNIDJIE Alex-Claudel  

## 📌 Description du projet

Ce projet consiste à concevoir et déployer une pipeline de données météo de bout en bout en utilisant Dagster comme orchestrateur.

L’objectif est de collecter des données météo multi-villes via des APIs externes, de les stocker dans une base PostgreSQL, de les transformer avec dbt, puis de les exploiter dans un dashboard Power BI afin de produire des indicateurs analytiques et décisionnels.

API choisie  

les API Open-Meteo + Nominatim 

## Données manipulées

### Entités
- Villes géolocalisées  
- Données météo  

### Champs principaux
- Ville, département  
- Latitude, longitude  
- Température  
- Humidité  
- Précipitations  
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
