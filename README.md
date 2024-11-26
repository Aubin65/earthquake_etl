# earthquake_etl

Pour ce projet, j'ai décidé de requêter l'[API](https://earthquake.usgs.gov/fdsnws/event/1/) du gouverment américain afin de tester l'aspect planification d'Apache Airflow. En effet, l'ETL viendra requêter l'API toutes les 15 minutes et stockera les informations qui n'ont pas encore été stockées dans la base de données MongoDB.

La forme de chaque document est la suivante : 

```json
{
  "_id": "ObjectId('674597b629fb05cd930292bf')",
  "mag": 1.32,
  "place": "9 km NW of The Geysers, CA",
  "date": "2024-11-26T09:21:32",
  "type": "earthquake",
  "nst": 27,
  "dmin": 0.007061,
  "sig": 27,
  "magType": "md",
  "geometryType": "Point",
  "longitude": -122.842666625977,
  "latitude": 38.8193321228027,
  "depth": 2.46000003814697
}
```

Pour cela, j'ai décidé de récupérer les coordonnées de manière séparée pour pouvoir les utiliser directement dans des visuels de carte.

