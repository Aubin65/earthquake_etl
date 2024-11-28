"""
Ce fichier est utilisé pour mettre en place l'interface de visualisation des données
"""

# Import des librairies nécessaires
import streamlit as st
from useful_functions.plot import plot_earthquake_locations, plot_closer_earthquake_locations
from useful_functions.mongodb import connect_mongo, disconnect_mongo
from useful_functions.queries import get_raw_data, get_closer_earthquakes

client, db, collection = connect_mongo()

st.title("Tremblements de terre récents")

st.header("Graphique")

st.write(
    "Vous vous êtes toujours intéressés aux tremblements de terre ? Voici une cartographie de ceux ayant eu lieu durant la dernière journée :"
)

mag_min, mag_max = st.slider("Sélectionner l'amplitude de magnitude", 0.00, 12.00, (0.00, 12.00))

st.plotly_chart(plot_earthquake_locations(collection=collection, mag_min=mag_min, mag_max=mag_max))

st.header("Données brutes")

st.dataframe(get_raw_data(collection=collection, mag_min=mag_min, mag_max=mag_max))

st.header("Affichage des n plus proches tremblements")

n = st.slider("Sélectionner le nombre d'enregistrements", 1, 20, 5)

df = get_closer_earthquakes(collection=collection, n=n)
df_to_print = df.drop(columns=["longitude", "latitude"])

st.dataframe(df_to_print)

st.plotly_chart(plot_closer_earthquake_locations(df=df))

disconnect_mongo()
