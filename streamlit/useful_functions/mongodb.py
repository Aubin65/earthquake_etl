"""
Ce fichier est utilisé pour mettre en place les fonctions de connexion à la base de données
"""

# Import des librairies nécessaires
import pymongo


def connect_mongo(
    mongo_client: str = "mongodb://localhost:27017/",
    mongo_db: str = "earthquake_db",
    mongo_collection: str = "earthquakes",
) -> tuple:
    """connexion à la base de données mongodb

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
        Tuple contenant le client mongo, la db et la collection pour requêter la base de données
    """

    client = pymongo.MongoClient(mongo_client)
    db = client[mongo_db]
    collection = db[mongo_collection]

    return client, db, collection


def disconnect_mongo(client: pymongo.MongoClient = connect_mongo()[0]) -> None:
    """déconnexion

    Parameters
    ----------
    client : pymongo.MongoClient
        url de connexion à la base de données MongoDB
    """

    client.close()
