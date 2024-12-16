"""
Ce fichier est utilisé pour mettre en place l'ETL grâce à Apache Airflow
"""

# Import des librairies nécessaires
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
import pendulum
import happybase  # noqa
import requests
from datetime import datetime, timezone
from geopy.distance import geodesic
import struct

# DAG de base
default_args = {"owner": "airflow", "retries": 0}


# Définition des fonctions de DAG
@dag(
    schedule="*/1 * * * *",  # Exécution toutes les minutes
    default_args=default_args,
    start_date=pendulum.today("UTC").add(days=-1),
    max_active_runs=1,  # Ici on définit ce paramètre à 1 pour empêcher les doublons d'exécution de ce DAG
    catchup=False,
    tags=["earthquake_dag_hbase"],
)
def earthquake_etl_hbase():
    """DAG global d'import des données des tremblement de terre depuis le fichier csv des données brutes vers la base de données HBase"""

    @task
    def extract(
        host: str = "localhost",
        port: int = 9090,
        table: str = "earthquakes",
        request: str = "https://earthquake.usgs.gov/fdsnws/event/1/query?",
        format: str = "geojson",
        starttime: pendulum.datetime = pendulum.now("UTC").add(days=-1).strftime("%Y-%m-%dT%H:%M:%S"),
    ) -> dict:
        """Tâche d'extraction de la donnée depuis l'API

        Parameters
        ----------
        request : _type_, optional
            corps de la requête à l'API, by default "https://earthquake.usgs.gov/fdsnws/event/1/query?"
        format : str, optional
            format de la réponse attendu, by default "geojson"
        starttime : str, optional
            paramètre de la requête, by default "2024-11-01"
        host : str
            host de la base de données, by default "localhost"
        port : int
            port utilisé pour la connexion à la base de données, by default 9090
        table : str
            table utilisée pour stocker les données, by default "earthquakes"

        Returns
        -------
        dict
            dictionnaire contenant les résultats de la requête API
        """

        # -------------------------------------------------------------------------------------------------------------------------------------------#
        # Connexion à la base de données HBase
        connection = happybase.Connection(host=host, port=port)

        # Vérification de la connexion
        connection.open()

        # Connexion à la table :
        table = connection.table(table)

        # -------------------------------------------------------------------------------------------------------------------------------------------#

        # Initialisation du nom de la colonne dans laquelle la date est stockée
        date_column = b"general_info:date"

        # Initialisation de la liste des dates du tableau
        dates_list = []  # noqa

        # Itération sur la table
        for key, data in table.scan():
            if date_column in data:

                # Récupération de la date
                date = data[date_column].decode("utf-8")  # noqa
                pass  # TODO: Compléter cela
        # -------------------------------------------------------------------------------------------------------------------------------------------#
        # Requête API
        final_api_request = f"{request}format={format}&starttime={starttime}"

        raw_data = requests.get(final_api_request, timeout=15).json()

        # -------------------------------------------------------------------------------------------------------------------------------------------#
        # Fermeture du client HBase
        connection.close()

        # -------------------------------------------------------------------------------------------------------------------------------------------#
        # Vérification de l'existence de features

        if len(raw_data["features"]) > 0:
            return raw_data
        else:
            raise AirflowException("pas de nouveaux features, arrêt du DAG.")

    @task
    def transform(raw_data: dict, own_position: tuple = (43.46602, -0.75166)) -> list[dict]:
        """Tâche de transformation du DataFrame brut

        Parameters
        ----------
        raw_data : dict
            dictionnaire issu de la requête get
        own_position : tuple, optional
            position à laquelle on se trouve, by default (43.46602, -0.75166) (position d'Octime)

        Returns
        -------
        list
            liste des éléments à stocker au bon format
        """

        # Initialisation de la liste contenant les séismes
        earthquakes_list = []

        for feature in raw_data["features"]:

            # Ajout des coordonnées du séisme dans un tuple pour calculer la distance
            point = (feature["geometry"]["coordinates"][1], feature["geometry"]["coordinates"][0])

            # Initialisation de la key row pour insérer dans HBase
            key = str((1 / (feature["properties"]["time"] / 1000).timestamp()) * 1e12).split(".")[-1].encode("utf-8")

            # Création du dictionnaire temporaire
            # On convertit les int et float en byte au format compact
            temp_dict = {
                b"stats:magType": {feature["properties"]["magType"]}.encode("utf-8"),
                b"stats:mag": struct.pack("f", feature["properties"]["mag"]),
                b"general:place": feature["properties"]["place"].encode("utf-8"),
                b"general:date": datetime.fromtimestamp(feature["properties"]["time"] / 1000, timezone.utc)
                .strftime("%Y-%m-%dT%H:%M:%S")
                .encode("utf-8"),
                b"general:type": feature["properties"]["type"].encode("utf-8"),
                b"stats:nst": struct.pack(">i", feature["properties"]["nst"]),
                b"stats:dmin": struct.pack("f", feature["properties"]["dmin"]),
                b"stats:sig": struct.pack(">i", feature["properties"]["sig"]),
                b"coordinates:geometryType": feature["geometry"]["type"].encode("utf-8"),
                b"coordinates:longitude": struct.pack("f", point[1]),
                b"coordinates:latitude": struct.pack("f", point[0]),
                b"coordinates:depth": struct.pack("f", feature["geometry"]["coordinates"][2]),
                b"stats:distance_from_us_km": struct.pack("f", round(geodesic(point, own_position).kilometers, 2)),
            }

            # Ajout à la liste finale
            earthquakes_list.append((key, temp_dict))

        return earthquakes_list

    @task
    def load(
        earthquakes_list: list,
        host: str = "localhost",
        port: int = 9090,
        table: str = "earthquakes",
        batch_size: int = 1000,
        split_batch: bool = False,
    ) -> None:
        """Tâche de chargement des données dans la base de données MongoDB

        Parameters
        ----------
        earthquakes_list : list
            liste des dictionnaires correspondant à chaque enregistrement de tremblement de terre
        """

        # client = pymongo.MongoClient(client)
        # Connexion à la base de données HBase
        connection = happybase.Connection(host=host, port=port)

        # Vérification de la connexion
        connection.open()

        # Connexion à la table :
        table = connection.table(table)

        # Condition sur la taille du batch
        if split_batch:
            # Initialisation du batch
            with table.batch(batch_size=batch_size) as batch:
                # Itération sur chaque enregistrement
                for record in earthquakes_list:

                    # Récupération de la row key et de la donnée correspondante
                    key, data = record
                    # Insertion des données
                    batch.put(key, data)

        else:
            # Initialisation du batch
            with table.batch() as batch:
                # Itération sur chaque enregistrement
                for record in earthquakes_list:

                    # Récupération de la row key et de la donnée correspondante
                    key, data = record
                    # Insertion des données
                    batch.put(key, data)

        print("Données chargées avec succès")

    # Extraction des données brutes
    raw_dict = extract()

    # Transformation des données brutes pour ne garder que ce qui nous intéresse
    transformed_dicts = transform(raw_dict)

    # Chargement des données
    load(earthquakes_list=transformed_dicts)


# Lancement du DAG général
earthquake_etl_hbase()
