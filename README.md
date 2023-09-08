# PROJET DE GESTION DE DONNÉES CINÉMATOGRAPHIQUES

Ce projet consiste en un pipe-line de données de bout en bout qui extrait, transforme, charge, indexe des données et
présente des visualisations des données.

## TABLE DES MATIÈRES

1. [INTRODUCTION](#introduction)
2. [CONFIGURATION & INSTALLATION](#configuration-et-installation)
3. [SOURCES DES FICHIERS SQL](#sources-des-fichiers-sql)

## INTRODUCTION

Ce projet a été développé dans le cadre d'un projet d'école d'ingénieurs et vise à fournir une solution complète pour
traiter des données cinématographiques en utilisant des technologies modernes telles que Docker, Elasticsearch, Kibana
et Cassandra.

## CONFIGURATION & INSTALLATION

Pour exécuter ce projet, suivez ces étapes :

1. **Prérequis :** assurez-vous d'avoir Docker et Docker Compose installés sur votre système.
2. **Clôner le dépôt :** clonez ce dépôt sur votre machine et accédez au répertoire du projet.
   ```shell
   git clone https://github.com/nate-ledrich/movie-pipeline.git
   cd movie-pipeline
   ```
3. **Configurez les variables d'environnement :** copier le fichier `.env` vers un fichier `.env` à la racine du projet
   et ajuster les valeurs des variables selon votre environnement.
   ```shell
   cp .env.example .env
   ```
4. **Installez les dépendances :** Assurez-vous d'installer les dépendances du fichier `requirements.txt`.
5. **Exécutez avec Docker Compose :** ouvrez un terminal et exécutez la commande suivante afin de démarrer les
   conteneurs :
   ```shell
   docker-compose up -d
   ```
   Attendez une dizaine de secondes pour que les services se mettent en place. Cela lancera les conteneurs MySQL,
   Cassandra, Elasticsearch et Kibana, défini dans le fichier `docker-compose.yaml`.
6. **Création des tables et keyspace :** les scripts SQL (`/mysql-ini-scripts`) et CQL (`/cassandra-init-scripts`) sont
   gérés de manière automatique. D'une part, les scripts SQL sont exécutés lors du démarrage du conteneur MySQL. D'autre
   part, les scripts CQL sont exécutés durant le processus d'ETL (Extract, Transform and Load).
7. **Importez le dashboard Kibana :** Ouvrez votre navigateur et accédez à http://localhost:5601 pour accéder à
   l'interface Kibana. Allez dans Data Management / Saved Objects et importez le fichier de configuration
   Kibana `kibana_config/export.ndjon`. Ce fichier défini le dashboard contenant les visualisations seront
   importés dans Kibana.

## SOURCES DES FICHIERS SQL

Les fichiers SQL utilisés pour construire la base de données ont été obtenus à
l'adresse https://github.com/bbrumm/databasestar/tree/main/sample_databases/sample_db_movies/mysql.