"""
Ce fichier est utilisé pour mettre en place les fonctions de visualisation des données
"""

# Import des librairies nécessaires
import plotly.express as px
from plotly.graph_objects import Figure, Scattergeo
import pandas as pd
import pymongo.collection


def plot_earthquake_locations(collection: pymongo.collection, mag_min: int = 0, mag_max: int = 12) -> Figure:
    """Fonction de plot de la carte du monde des tremblements de terre datant de la veille

    Parameters
    ----------
    collection : pymongo.collection
        collection utilisée pour se connecter à la base mongodb
    mag_min : int, optional
        magnitude minimale, by default 0
    mag_max : int, optional
        magnitude maximale, by default 12

    Returns
    -------
    Figure
        objet plotly.graph_objects utilisé pour l'affichage du graphique
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
        title="Localisation des Tremblements de Terre dans le monde depuis hier",
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


def plot_closer_earthquake_locations(df: pd.DataFrame, home_point: tuple = (43.46602, -0.75166)) -> Figure:
    """Fonction de plot de la carte du monde des tremblements de terre datant de la veille

    Parameters
    ----------
    df : pd.DataFrame
        dataframe des n plus proches tremblements de terre

    Returns
    -------
    Figure
        objet plotly.graph_objects utilisé pour l'affichage du graphique
    """

    # Création de la carte avec Plotly
    fig = px.scatter_geo(
        df,
        lat="latitude",
        lon="longitude",
        hover_name="place",
        hover_data="mag",
        size="mag",
        size_max=10,
        title=f"Localisation des {len(df)} Tremblements de Terre les plus proches d'Octime depuis hier",
    )

    for _, point in df.iterrows():

        fig.add_trace(
            Scattergeo(
                lat=[home_point[0]],
                lon=[home_point[1]],
                mode="markers",
                marker=dict(size=10, color="green", symbol="circle"),
                name="Position Octime",
                showlegend=False,
            )
        )

        fig.add_trace(
            Scattergeo(
                lat=[point.latitude, home_point[0]],
                lon=[point.longitude, home_point[1]],
                mode="lines",
                line=dict(width=1, color="red"),
                name=f"Distance Octime - {point.place}",
            )
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
        # Position de la légende sous la carte
        legend=dict(
            y=-0.1,
            yanchor="top",
            x=0.5,
            xanchor="center",
            orientation="h",
        ),
    )

    return fig
