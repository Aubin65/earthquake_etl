"""
Ce fichier est utilisé pour mettre en place les fonctions de visualisation des données
"""

# Import des librairies nécessaires
import plotly.express as px
import pandas as pd


def plot_earthquake_locations(collection, mag_min: int = 0, mag_max: int = 12) -> None:
    """Fonction de plot de la carte du monde des tremblements de terre datant de la veille

    Parameters
    ----------
    collection : pymongo.collection
        collection utilisée pour se connecter à la base mongodb
    """

    # Extraction des données (longitudes et latitudes)
    cursor = collection.find({}, {"latitude": 1, "longitude": 1, "place": 1, "mag": 1, "_id": 0})
    raw_data = list(cursor)

    data = {
        "latitude": [elem["latitude"] for elem in raw_data if (elem["mag"] < mag_max and elem["mag"] > mag_min)],
        "longitude": [elem["longitude"] for elem in raw_data if (elem["mag"] < mag_max and elem["mag"] > mag_min)],
        "place": [elem["place"] for elem in raw_data if (elem["mag"] < mag_max and elem["mag"] > mag_min)],
        "magnitude": [elem["mag"] for elem in raw_data if (elem["mag"] < mag_max and elem["mag"] > mag_min)],
    }

    df = pd.DataFrame(data)

    # Création de la carte avec Plotly
    fig = px.scatter_geo(
        df,
        lat="latitude",
        lon="longitude",
        hover_name=data["place"],
        hover_data={"magnitude": True},
        size="magnitude",
        size_max=10,
        title="Localisation des Tremblements de Terre",
    )

    # Configuration des styles de la carte
    fig.update_geos(showcountries=True, showcoastlines=True, showland=True, landcolor="rgb(240, 240, 240)")

    fig.update_layout(
        geo=dict(
            scope="world",  # Vous pouvez changer cela en 'europe' ou 'usa' selon vos besoins
            projection_type="mercator",
            showland=True,
            landcolor="lightgray",
            showlakes=True,
            lakecolor="white",
        ),
        # Augmenter la taille de la carte
        width=1200,
        height=800,
    )

    return fig
