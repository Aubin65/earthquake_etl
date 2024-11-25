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
    def extract(request: str) -> pd.DataFrame:
        """Tâche d'extraction de la donnée depuis l'API

        Parameters
        ----------
        request : str
            Corps de la requête à effectuer

        Returns
        -------
        pd.DataFrame
            DataFrame comprenant les données brutes extraites de l'API
        """
        pass

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
