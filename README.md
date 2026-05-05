🌦️ Weather Data Pipeline avec Dagster

👥 Équipe projet
HABRAN Karl
SEBASTIAO Luna
NGNIDJIE Alex-Claudel
📌 Description du projet

Ce projet consiste à concevoir et déployer une pipeline de données météo de bout en bout orchestrée avec Dagster.

L’objectif est de :

Collecter des données météo multi-villes via des APIs
Stocker les données dans une base PostgreSQL
Transformer les données avec dbt
Produire des indicateurs analytiques via Power BI

👉 Le projet suit une logique ELT (Extract → Load → Transform)

🌍 APIs utilisées
Open-Meteo → données météo
Nominatim → géolocalisation des villes

💡 Pourquoi ce choix ?

Nominatim → transforme une ville en coordonnées
Open-Meteo → utilise ces coordonnées pour fournir la météo

➡️ Pipeline cohérente de la localisation à l’analyse

🏗️ Architecture globale
        ┌────────────┐
        │  Nominatim │
        └─────┬──────┘
              │
              ▼
        ┌────────────┐
        │ Open-Meteo │
        └─────┬──────┘
              │
              ▼
        ┌────────────┐
        │ PostgreSQL │
        └─────┬──────┘
              │
              ▼
        ┌────────────┐
        │    dbt     │
        └─────┬──────┘
              │
              ▼
        ┌────────────┐
        │ Power BI   │
        └────────────┘

📊 Données manipulées
Entités
Villes géolocalisées
Données météo
Champs principaux
Ville, département
Latitude / Longitude
Température
Humidité
Précipitations
Vent


⚙️ Stack technique
Python 3.11
Dagster
PostgreSQL
dbt
Docker / Docker Compose
Power BI

📦 Dépendances principales :
Dagster, pandas, requests, psycopg, SQLAlchemy, dbt

🐳 Lancement du projet
1️⃣ Lancer PostgreSQL avec Docker
cd weather-pipeline-project
docker-compose up -d

➡️ PostgreSQL exposé sur le port 55432

2️⃣ Lancer Dagster
cd ./src/projet_final_4DATA

# définir la variable d'environnement
$env:DATABASE_URL="votre_url"

dagster dev
🚀 Exécution du pipeline

Dans l’interface Dagster :

▶️ Ordre d’exécution des assets
get_coordinates
get_raw_meteo_data
clean_meteo_data
meteo_metrics
🔄 Pipeline de données

📥 Extract
Récupération des villes
Géolocalisation via Nominatim

Appel API Open-Meteo
📦 Load
Insertion des données brutes dans PostgreSQL

🔧 Transform
Nettoyage des données
Standardisation des formats
Création de métriques

🧠 Orchestration avec Dagster

📌 Assets
get_coordinates
get_raw_meteo_data
clean_meteo_data
meteo_metrics

⚙️ Jobs
Collecte des données météo
Transformation des données
Nettoyage des anciennes données

⏰ Schedules
Toutes les heures → ingestion météo
Quotidien → calcul des métriques
Nettoyage périodique

🚨 Sensors
Détection météo extrême (optionnel)

🧩 Partitions
Partition par date pour historisation

👉 Architecture conforme aux bonnes pratiques Dagster

🧪 dbt – Transformation des données
Modèles
Staging → données nettoyées
Mart → données analytiques
Exemples de métriques
Température moyenne
Précipitations totales
Min / Max par ville

📈 Dashboard Power BI
KPIs
🌧️ Pluie élevée
🌬️ Vent fort
🌡️ Variation de température


🔍 Monitoring & Tests

🧱 3 couches de monitoring
Dagster → pipeline
dbt tests → qualité des données
PostgreSQL → audit & logs

📁 Structure du projet (exemple)
weather-pipeline-project/
│
├── src/
│   └── projet_final_4DATA/
│
├── dbt/
├── sql/
├── docker-compose.yml
├── requirements.txt
├── README.md
└── .env

⚠️ Risques connus
Nominatim
Limite : 1 requête/seconde
Données crowdsourcées
Open-Meteo
Temps de réponse variable
Sources multiples → incohérences possibles
🛠️ Plan B
Changer d’API météo si besoin
Ajouter cache ou stockage intermédiaire
🧑‍💻 Répartition des rôles
Membre	Responsabilités
Karl	Power BI + PostgreSQL
Luna	Dagster
Alex-Claudel	dbt
Tous	README + Soutenance
🎯 Objectif final

Créer une solution complète permettant :

📊 Analyse météo multi-villes
📈 Aide à la décision
