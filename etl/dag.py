"""
Ce fichier est utilisé pour mettre en place l'ETL grâce à Apache Airflow
"""

# Import des librairies nécessaires
from airflow.decorators import dag, task
import pendulum  # noqa
import pymongo  # noqa
import pandas as pd  # noqa
import os  # noqa
import requests  # noqa


# Définition des fonctions de DAG
@dag(schedule="@once", start_date=pendulum.datetime(2021, 1, 1, tz="UTC"), catchup=False, tags=["earthquake_dag"])
def superheroes_etl():
    """DAG global d'import des données des tremblement de terre depuis le fichier csv des données brutes vers la base de données MongoDB"""

    @task
    def connect_mongo(
        mongo_client: str = "mongodb://localhost:27017/",
        mongo_db: str = "earthquake_db",
        mongo_collection: str = "earthquakes",
    ) -> tuple:
        """_summary_

        Parameters
        ----------
        mongo_client : _type_, optional
            url de connexion à la base de données MongoDB, by default "mongodb://localhost:27017/"
        mongo_db : str, optional
            _description_, by default "earthquake_db"
        mongo_collection : str, optional
            _description_, by default "earthquakes"

        Returns
        -------
        tuple
            Tuple contenant le client mongo, la db et la collection pour requêter la base de données
        """

        client = pymongo.MongoClient(mongo_client)
        db = client["earthquake_db"]
        collection = db["earthquakes"]

        return client, db, collection

    @task
    def extract(
        client: str,
        db: str,
        collection: str,
        request: str = "https://earthquake.usgs.gov/fdsnws/event/1/query?",
        format: str = "geojson",
        starttime: str = "2024-11-01",
    ) -> pd.DataFrame:
        """Tâche d'extraction de la donnée depuis l'API

        Parameters
        ----------
        request : _type_, optional
            corps de la requête à l'API, by default "https://earthquake.usgs.gov/fdsnws/event/1/query?"
        format : str, optional
            format de la réponse attendu, by default "geojson"
        starttime : str, optional
            paramètre de la requête, by default "2024-11-01"
        client : str
            client de la base de données, issu de la fonction connect_mongo()
        db : str
            nom de la base de données, issu de la fonction connect_mongo()
        collection : str
            nom de la collection, issu de la fonction connect_mongo()

        Returns
        -------
        pd.DataFrame
            DataFrame contenant les résultats de la requête API
        """

        # -------------------------------------------------------------------------------------------------------------------------------------------#
        # Phase de récupération de la dernière date de requête

        mongo_request = [
            {"$project": {"generated": "$metadata.generated"}},
            {"$group": {"_id": None, "maxGenerated": {"$max": "$generated"}}},
        ]

        res = list(collection.aggregate(mongo_request))

        starttime = res[0]["maxGenerated"]

        # -------------------------------------------------------------------------------------------------------------------------------------------#
        # Requête API
        final_api_request = f"{request}format={format}&starttime={starttime}"

        return requests.get(final_api_request).json()

    @task
    def transform(raw_df: pd.DataFrame) -> pd.DataFrame:
        """Tâche de transformation du DataFrame brut

        Parameters
        ----------
        raw_df : pd.DataFrame
            DataFrame issu de la tâche d'extraction de la données

        Returns
        -------
        pd.DataFrame
            DataFrame ayant subi les transformations nécessaires avant son stockage
        """
        pass

    @task
    def load(transformed_df: pd.DataFrame) -> None:
        """Tâche de chargement des données dans la base de données MongoDB

        Parameters
        ----------
        transformed_df : pd.DataFrame
            DataFrame issu de la phase de transformation de la donnée
        """
        pass
