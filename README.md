# earthquake_etl

Pour ce projet, j'ai décidé de requêter l'[API](https://earthquake.usgs.gov/fdsnws/event/1/) du gouverment américain afin de tester l'aspect planification d'Apache Airflow. En effet, l'ETL viendra requêter l'API toutes les 15 minutes et stockera les informations qui n'ont pas encore été stockées dans la base de données MongoDB.