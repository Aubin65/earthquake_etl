"""
Ce fichier est utilisé pour mettre en place la purge de la base de données des tremblements de terre grâce à Apache Airflow
"""

# Import des librairies nécessaires
from airflow.decorators import dag, task
from datetime import datetime
import happybase
import pendulum
from earthquake_etl_airflow.DAGs.hbase.useful_functions.encoding_functions import bytes_to_var

# DAG de base
default_args = {
    "owner": "airflow",
    "retries": 0,
}


# Définition des fonctions de DAG
@dag(
    schedule="*/1 * * * *",  # Exécution toutes les minutes
    default_args=default_args,
    start_date=pendulum.today("UTC").add(days=-1),
    max_active_runs=1,  # Ici on définit ce paramètre à 1 pour empêcher les doublons d'exécution de ce DAG
    catchup=False,
    tags=["purge_earthquake_db"],
)
def purge_earthquake_db():

    @task
    def purge(
        host: str = "localhost",
        port: int = 9090,
        table: str = "earthquakes",
    ) -> None:
        """Fonction de purge de la base de données earthquake pour les données plus anciennes qu'un jour

        Parameters
        ----------
        client : _type_, optional
            adresse de la base MongoDB, by default "mongodb://localhost:27017/"
        db : str, optional
            nom de la base de données MongoDB, by default "earthquake_db"
        collection : str, optional
            nom de la collection dans la base de données MongoDB, by default "earthquakes"
        """

        # Connexion à la base de données HBase
        connection = happybase.Connection(host=host, port=port)

        # Vérification de la connexion
        connection.open()

        # Connexion à la table :
        table = connection.table(table)

        # Récupération de la date de la veille
        yesterday = pendulum.now("UTC").add(days=-1)

        for row_key, data in table.scan():

            date = datetime.strptime(bytes_to_var(data[b"general:date"]), "%Y-%m-%dT%H:%M:%S")

            if date < yesterday:

                table.delete(row_key)

        # Fermeture du client HBase
        connection.close()

    purge()


purge_earthquake_db()
