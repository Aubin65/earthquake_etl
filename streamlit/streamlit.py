"""
Ce fichier est utilisé pour mettre en place l'interface de visualisation des données
"""

# Import des librairies nécessaires
import streamlit as st
from useful_functions.plot import plot_earthquake_locations
from useful_functions.mongodb import connect_mongo, disconnect_mongo  # noqa

client, db, collection = connect_mongo()

st.title("A la découverte des tremblements de terre récents")

st.write(
    "Vous vous êtes toujours intéressés aux tremblements de terre ? Voici une cartographie de ceux ayant eu lieu durant la dernière journée :"
)

mag_min, mag_max = st.slider("Sélectionner l'amplitude de magnitude", 0, 12)

st.plotly_chart(plot_earthquake_locations(collection=collection, mag_min=mag_min, mag_max=mag_max))

disconnect_mongo()
