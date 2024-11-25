"""
Ce fichier est utilisé pour mettre en place l'ETL grâce à Apache Airflow
"""

# Import des librairies nécessaires
from airflow.decorators import dag, task
import pendulum
import pymongo
import requests


# Définition des fonctions de DAG
@dag(schedule="15 * * * *", start_date=pendulum.datetime(2021, 1, 1, tz="UTC"), catchup=False, tags=["earthquake_dag"])
def earthquake_etl():
    """DAG global d'import des données des tremblement de terre depuis le fichier csv des données brutes vers la base de données MongoDB"""

    @task
    def connect_mongo(
        mongo_client: str = "mongodb://localhost:27017/",
        mongo_db: str = "earthquake_db",
        mongo_collection: str = "earthquakes",
    ) -> tuple:
        """Connexion à la base de données MongoDB

        Parameters
        ----------
        mongo_client : _type_, optional
            url de connexion à la base de données MongoDB, by default "mongodb://localhost:27017/"
        mongo_db : str, optional
            nom de la base de données, by default "earthquake_db"
        mongo_collection : str, optional
            nom de la collection, by default "earthquakes"

        Returns
        -------
        tuple
            tuple contenant le client mongo, la db et la collection pour requêter la base de données
        """

        client = pymongo.MongoClient(mongo_client)
        db = client[mongo_db]
        collection = db[mongo_collection]

        return client, db, collection

    @task
    def disconnect_mongo(client: pymongo.MongoClient) -> None:
        """Déconnexion du client MongoDB

        Parameters
        ----------
        client : pymongo.MongoClient
            client MongoDB à déconnecter
        """
        client.close()

    @task
    def extract(
        client: str,
        db: str,
        collection: str,
        request: str = "https://earthquake.usgs.gov/fdsnws/event/1/query?",
        format: str = "geojson",
        starttime: str = "2024-11-01",
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
        # Phase de récupération de la dernière date de requête

        res = collection.aggregate([{"$group": {"_id": None, "maxTime": {"$max": "$time"}}}])

        # Si il n'y a aucun document, starttime est égal à celui prédéfini, sinon au max trouvé dans la collection MongoDB
        result = next(res, None)
        starttime = result["maxTime"] if result else starttime

        # -------------------------------------------------------------------------------------------------------------------------------------------#
        # Requête API
        final_api_request = f"{request}format={format}&starttime={starttime}"

        return requests.get(final_api_request).json()

    @task
    def transform(raw_data: dict) -> list[dict]:
        """Tâche de transformation du DataFrame brut

        Parameters
        ----------
        raw_data : dict
            dictionnaire issu de la requête get

        Returns
        -------
        list
            liste des éléments à stocker au bon format
        """

        earthquakes_list = []

        for feature in raw_data["features"]:

            # Création du dictionnaire temporaire
            temp_dict = {
                "mag": feature["properties"]["mag"],
                "place": feature["properties"]["place"],
                "time": feature["properties"]["time"],
                "type": feature["properties"]["type"],
                "nst": feature["properties"]["nst"],
                "dmin": feature["properties"]["dmin"],
                "sig": feature["properties"]["sig"],
                "magType": feature["properties"]["magType"],
                "geometryType": feature["geometry"]["type"],
                "coordinates": feature["geometry"]["coordinates"],
            }

            # Ajout à la liste finale
            earthquakes_list.append(temp_dict)

        return earthquakes_list

    @task
    def load(earthquakes_list: list, collection: pymongo.collection.Collection) -> None:
        """Tâche de chargement des données dans la base de données MongoDB

        Parameters
        ----------
        earthquakes_list : list
            liste des dictionnaires correspondant à chaque enregistrement de tremblement de terre
        """

        collection.insert_many(earthquakes_list)

    # Extraction du client, de la db et de la collection concernée
    client, db, collection = connect_mongo()

    # Extraction des données brutes
    raw_dict = extract(client=client, db=db, collection=collection)

    # Transformation des données brutes pour ne garder que ce qui nous intéresse
    transformed_dicts = transform(raw_dict)

    # Chargement des données
    load(earthquakes_list=transformed_dicts, collection=collection)

    # Déconnexion du client
    disconnect_mongo(client)


# Lancement du DAG général
earthquake_etl()
