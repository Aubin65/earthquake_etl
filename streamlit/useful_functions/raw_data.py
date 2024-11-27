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
