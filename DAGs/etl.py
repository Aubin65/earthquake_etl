"""
Ce fichier est utilisé pour mettre en place l'ETL grâce à Apache Airflow
"""

# Import des librairies nécessaires
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
import pendulum
import pymongo
import pymongo.collection
import requests
from datetime import datetime, timezone
from geopy.distance import geodesic

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
    tags=["earthquake_dag"],
)
def earthquake_etl():
    """DAG global d'import des données des tremblement de terre depuis le fichier csv des données brutes vers la base de données MongoDB"""

    @task
    def extract(
        client: str = "mongodb://localhost:27017/",
        db: str = "earthquake_db",
        collection: str = "earthquakes",
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
        client : str
            client de la base de données, issu de la fonction connect_mongo()
        db : str
            nom de la base de données, issu de la fonction connect_mongo()
        collection : str
            nom de la collection, issu de la fonction connect_mongo()

        Returns
        -------
        dict
            dictionnaire contenant les résultats de la requête API
        """

        # -------------------------------------------------------------------------------------------------------------------------------------------#
        # Connexion à la base de données MongoDB
        client = pymongo.MongoClient(client)
        db = client[db]
        collection = db[collection]

        # -------------------------------------------------------------------------------------------------------------------------------------------#
        # Phase de récupération de la dernière date de requête

        res = collection.aggregate([{"$group": {"_id": None, "maxDate": {"$max": "$date"}}}])

        # Si il n'y a aucun document, starttime est égal à celui prédéfini, sinon au max trouvé dans la collection MongoDB
        result = next(res, None)
        starttime = result["maxDate"] if result else starttime

        # -------------------------------------------------------------------------------------------------------------------------------------------#
        # Requête API
        final_api_request = f"{request}format={format}&starttime={starttime}"

        raw_data = requests.get(final_api_request, timeout=15).json()

        # -------------------------------------------------------------------------------------------------------------------------------------------#
        # Fermeture du client MongoDB
        client.close()

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

        earthquakes_list = []

        for feature in raw_data["features"]:

            point = (feature["geometry"]["coordinates"][1], feature["geometry"]["coordinates"][0])

            # Création du dictionnaire temporaire
            temp_dict = {
                "mag": feature["properties"]["mag"],
                "place": feature["properties"]["place"],
                "date": datetime.fromtimestamp(feature["properties"]["time"] / 1000, timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%S"
                ),
                # "time": feature["properties"]["time"],
                "type": feature["properties"]["type"],
                "nst": feature["properties"]["nst"],
                "dmin": feature["properties"]["dmin"],
                "sig": feature["properties"]["sig"],
                "magType": feature["properties"]["magType"],
                "geometryType": feature["geometry"]["type"],
                # "coordinates": feature["geometry"]["coordinates"],
                "longitude": point[1],
                "latitude": point[0],
                "depth": feature["geometry"]["coordinates"][2],
                "distance_from_us_km": round(geodesic(point, own_position).kilometers, 2),
            }

            # Ajout à la liste finale
            earthquakes_list.append(temp_dict)

        return earthquakes_list

    @task
    def load(
        earthquakes_list: list,
        client: str = "mongodb://localhost:27017/",
        db: str = "earthquake_db",
        collection: str = "earthquakes",
    ) -> None:
        """Tâche de chargement des données dans la base de données MongoDB

        Parameters
        ----------
        earthquakes_list : list
            liste des dictionnaires correspondant à chaque enregistrement de tremblement de terre
        """

        client = pymongo.MongoClient(client)
        db = client[db]
        collection = db[collection]

        collection.insert_many(earthquakes_list)

        client.close()

    # Extraction des données brutes
    raw_dict = extract()

    # Transformation des données brutes pour ne garder que ce qui nous intéresse
    transformed_dicts = transform(raw_dict)

    # Chargement des données
    load(earthquakes_list=transformed_dicts)


# Lancement du DAG général
earthquake_etl()
