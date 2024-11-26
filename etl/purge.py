"""
Ce fichier est utilisé pour mettre en place la purge de la base de données des tremblements de terre grâce à Apache Airflow
"""

# Import des librairies nécessaires
from airflow.decorators import dag, task  # noqa
from airflow.exceptions import AirflowException  # noqa
from airflow.utils.dates import days_ago  # noqa
import pymongo  # noqa
import pymongo.collection  # noqa
import requests  # noqa
from datetime import datetime, timezone  # noqa

# DAG de base
default_args = {
    "owner": "airflow",
    "retries": 0,
}


# Définition des fonctions de DAG
@dag(
    schedule="*/1 * * * *",  # Exécution toutes les minutes
    default_args=default_args,
    start_date=days_ago(1),
    max_active_runs=1,  # Ici on définit ce paramètre à 1 pour empêcher les doublons d'exécution de ce DAG
    catchup=False,
    tags=["purge_earthquake_db"],
)
def purge_earthquake_db():

    @task
    def cleanse(
        client: str = "mongodb://localhost:27017/", db: str = "earthquake_db", collection: str = "earthquakes"
    ) -> None:

        # Connexion à la base de données MongoDB
        client = pymongo.MongoClient(client)
        db = client[db]
        collection = db[collection]

        pass
