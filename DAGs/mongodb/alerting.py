"""
Ce script est utilisé pour alerter un utilisateur lorsqu'un tremblement de terre a été détecté plus proche d'une certaine distance définie dans la fonction
"""

# Import des librairies nécessaires
from airflow.decorators import dag, task
import pendulum
import pymongo
import pymongo.collection
from dotenv import load_dotenv  # noqa
from email.mime.text import MIMEText
import smtplib
import os  # noqa

# DAG de base
default_args = {"owner": "airflow", "retries": 0}


# Définition des fonctions de DAG
@dag(
    schedule="@daily",  # Exécution quotidienne
    default_args=default_args,
    start_date=pendulum.today("UTC").add(days=-1),
    max_active_runs=2,
    catchup=False,
    tags=["alerting_dag"],
)
def alerting_dag():
    """
    Ce Dag est divisé en trois étapes :
        * Une étape de récupération des tremblements de terre les plus proches selon une distance minimale
        * Une étape de récupération des variables d'environnement nécessaires à l'envoi du mail
        * Une étape d'alerting en fonction du résultat de l'étape précédente
    """

    @task
    def get_close_earthquakes(
        client: str = "mongodb://localhost:27017/",
        db: str = "earthquake_db",
        collection: str = "earthquakes",
        dist_min: int = 5000,
    ) -> list[dict]:
        """Fonction d'extraction des données sur les tremblements de terre

        Parameters
        ----------
        client : str
            client de la base de données, by default "mongodb://localhost:27017/"
        db : str
            nom de la base de données, by default "earthquake_db"
        collection : str
            nom de la collection, by default "earthquakes"
        dist_min : int, optional
            distance minimale pour l'alerting, by default 5000

        Returns
        -------
        list[dict]
            liste comprenant les enregistrements suffisamment proches pour devoir alerter par mail
        """

        # Initialisation de la liste finale des enregistrements
        res = []

        # Connexion à la base de données MongoDB
        client = pymongo.MongoClient(client)
        db = client[db]
        collection = db[collection]

        # Requête permettant de récupérer les enregistrements plus proches que la distance minimale
        query = {"distance_from_us_km": {"$lte": dist_min}}
        projection = {"_id": 0}

        cursor = collection.find(query, projection)

        for record in cursor:
            res.append(record)

        # Fermeture du client
        client.close()

        # On retourne la liste des résultats
        return res

    @task
    def load_env_var(
        var_list: list[str] = ["SMTP_HOST", "SMTP_USER", "SMTP_PASSWORD", "SMTP_MAIL_FROM", "SMTP_RECIPIENTS"]
    ) -> dict:
        """Fonction de chargement des variables d'environnement nécessaires à l'envoi des mails

        Il faut définir ces variables dans un fichier .env à la racine du projet (son chemin est en input de la fonction)

        ```
        smtp_host = ${SMTP_HOST}
        smtp_user = ${SMTP_USER}
        smtp_password = ${SMTP_PASSWORD}
        smtp_mail_from = ${SMTP_MAIL_FROM}
        ```

        Parameters
        ----------
        var_list : list[str], optional
            liste des variables d'environnement à récupérer, by default ["SMTP_HOST", "SMTP_USER", "SMTP_PASSWORD", "SMTP_MAIL_FROM", "SMTP_RECIPIENTS"]

        Returns
        -------
        dict
            dictionnaire des variables d'environnement
        """

        # Clear le cache si les variables ont changé
        for var in var_list:
            os.environ.pop(var, None)

        # Chargement des variables d'environnement du fichier .env
        load_dotenv()

        # On retourne les variables dont on a besoin

        return {var: os.getenv(var) for var in var_list}

    @task
    def alert(close_earthquakes: list[dict], smtp_config: dict, port: int = 587) -> None:
        """Fonction d'envoi du mail d'alerte si un séisme a été détecté proche d'Orthez

        Parameters
        ----------
        close_earthquakes : list[dict]
            liste des séismes proches, issu de la tâche get_close_earthquakes()
        host : str
            paramètre host pour l'envoi du mail, issu de la fonction load_env_var()
        usr : str
            paramètre usr pour l'envoi du mail, issu de la fonction load_env_var()
        pwd : str
            paramètre pwd pour l'envoi du mail, issu de la fonction load_env_var()
        mail_from : str
            paramètre mail_from pour l'envoi du mail, issu de la fonction load_env_var()
        recipients : str
            paramètre recipients pour l'envoi du mail, issu de la fonction load_env_var()
        port : int
            paramètre port pour l'envoi du mail, by default 587
        """

        if len(close_earthquakes) > 1:

            earthquake_details = "\n".join(
                [f"- magnitude {eq['mag']}, {eq['place']} ({eq['date']})" for eq in close_earthquakes]
            )

            # Créer un email texte brut
            subject = "ALERTE : TREMBLEMENT DE TERRE PROCHE"
            to_email = "habitants@ville_orthez.com"
            body = f"Bonjour, ceci est un email envoyé automatiquement à l'aide de Python.\n\
Il est écrit pour vous prévenir que les tremblements de terre suivants ont été détectés proche d'Orthez :\n\n\
{earthquake_details}\n\n\
Bien cordialement,\n\
Aubin Morlas"

            msg = MIMEText(body, "plain")
            msg["Subject"] = subject
            msg["From"] = smtp_config["SMTP_MAIL_FROM"]
            msg["To"] = to_email

            # Envoyer l'email
            with smtplib.SMTP(smtp_config["SMTP_HOST"], port) as server:
                server.starttls()
                server.login(smtp_config["SMTP_USER"], smtp_config["SMTP_PASSWORD"])
                server.sendmail(
                    smtp_config["SMTP_MAIL_FROM"], smtp_config["SMTP_RECIPIENTS"].split(","), msg.as_string()
                )

        elif len(close_earthquakes) == 1:

            earthquake_details = "\n".join(
                [f"- magnitude {eq['mag']}, {eq['place']} ({eq['date']})" for eq in close_earthquakes]
            )

            # Créer un email texte brut
            subject = "ALERTE : TREMBLEMENT DE TERRE PROCHE"
            to_email = "habitants@ville_orthez.com"
            body = f"Bonjour, ceci est un email envoyé automatiquement à l'aide de Python.\n\
Il est écrit pour vous prévenir que le tremblement de terre suivant a été détecté proche d'Orthez :\n\n\
{earthquake_details}\n\n\
Bien cordialement,\n\
Aubin Morlas"

            msg = MIMEText(body, "plain")
            msg["Subject"] = subject
            msg["From"] = smtp_config["SMTP_MAIL_FROM"]
            msg["To"] = to_email

            # Envoyer l'email
            with smtplib.SMTP(smtp_config["SMTP_HOST"], port) as server:
                server.starttls()
                server.login(smtp_config["SMTP_USER"], smtp_config["SMTP_PASSWORD"])
                server.sendmail(
                    smtp_config["SMTP_MAIL_FROM"], smtp_config["SMTP_RECIPIENTS"].split(","), msg.as_string()
                )

        else:
            # Créer un email texte brut
            subject = "MAJ Situation tremblements de terre"
            to_email = "habitants@ville_orthez.com"
            body = "Bonjour, ceci est un email envoyé automatiquement à l'aide de Python.\n\
Il est écrit pour vous prévenir qu'aucun tremblement de terre n'a été recensé proche d'Orthez depuis hier.\n\
Bien cordialement,\n\
Aubin Morlas"

            msg = MIMEText(body, "plain")
            msg["Subject"] = subject
            msg["From"] = smtp_config["SMTP_MAIL_FROM"]
            msg["To"] = to_email

            # Envoyer l'email
            with smtplib.SMTP(smtp_config["SMTP_HOST"], port) as server:
                server.starttls()
                server.login(smtp_config["SMTP_USER"], smtp_config["SMTP_PASSWORD"])
                server.sendmail(
                    smtp_config["SMTP_MAIL_FROM"], smtp_config["SMTP_RECIPIENTS"].split(","), msg.as_string()
                )

    # Récupération des enregistrements proches
    close_earthquakes = get_close_earthquakes(dist_min=6650)

    # Chargement des données d'envoi des mails
    smtp_config = load_env_var()

    # Envoi de l'alerte
    alert(close_earthquakes=close_earthquakes, smtp_config=smtp_config)


alerting_dag()
