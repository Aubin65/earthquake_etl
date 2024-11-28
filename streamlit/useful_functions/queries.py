"""
Ce fichier est utilisé pour mettre en place les fonctions de récupération de données brutes
"""

# Import des librairies nécessaires
import pandas as pd
import pymongo.collection


def get_raw_data(collection: pymongo.collection, mag_min: int = 0, mag_max: int = 12) -> pd.DataFrame:
    """fonction qui crée un dataframe en fonction d'une fenêtre de magnitude donnée

    Parameters
    ----------
    collection : pymongo.collection
        collection de la bdd mongodb
    mag_min : int, optional
        magnitude minimale, by default 0
    mag_max : int, optional
        magnitude maximale, by default 12

    Returns
    -------
    pd.DataFrame
        dataframe contenant les données souhaitées
    """

    # Requête mongodb avec filtre sur l'attribut "mag"
    query = {"mag": {"$gte": mag_min, "$lte": mag_max}}

    # Extraction des données (longitudes et latitudes)
    cursor = collection.find(query, {"_id": 0})

    return pd.DataFrame(data=list(cursor))


def get_closer_earthquakes(collection: pymongo.collection, n: int = 5) -> pd.DataFrame:
    """fonction qui renvoie un dataframe comprenant les cinq tremblements les plus proches

    Parameters
    ----------
    collection : pymongo.collection
        collection de la base de données mongodb
    n : int, optional
        nombre de tremblements à prendre en compte, by default 5

    Returns
    -------
    pd.DataFrame
        dataframe comprenant les 5 enregistrements les plus proches de notre position (la position prise en compte est définie lors du déclenchement de l'ETL)
    """

    # Extraction des n lignes comprenant les tremblements les plus proches
    cursor = (
        collection.find(
            {},
            {
                "_id": 0,
                "type": 0,
                "nst": 0,
                "dmin": 0,
                "sig": 0,
                "magType": 0,
                "geometryType": 0,
                "depth": 0,
            },
        )
        .sort("distance_from_us_km", pymongo.ASCENDING)
        .limit(n)
    )

    return pd.DataFrame(data=list(cursor))
