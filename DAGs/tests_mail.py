from airflow.decorators import dag, task
from datetime import datetime, timedelta
from dotenv import load_dotenv  # noqa
import os  # noqa
import random as rd
from email.mime.text import MIMEText
import smtplib


# Définir les arguments par défaut
default_args = {
    "owner": "airflow",
    "email": ["a.morlas@octime.com"],
    "email_on_failure": True,  # Activer les emails en cas d'échec
    "email_on_retry": True,  # Activer les emails en cas de relance
    "retries": 0,  # Nombre de tentatives en cas d'échec
    "retry_delay": timedelta(minutes=10),  # Intervalle entre les tentatives
    "start_date": datetime(2024, 12, 1),
}


# Utiliser ces paramètres dans un DAG
@dag(
    default_args=default_args,
    schedule_interval="*/1 * * * *",
    catchup=False,
    description="DAG avec configuration d'email",
    max_active_runs=1,
)
def email_notification_dag():

    @task
    def load_variables(relative_env_path: str = ".env") -> None:
        """
        Cette fonction est utilisée pour charger les variables d'environnement utilisées par le fichier airflow.cfg.
        Il faut au préalable avoir défini les variables suivantes de la manière suivante dans le fichier cité précédemment :

        ```
        smtp_host = ${SMTP_HOST}
        smtp_user = ${SMTP_USER}
        smtp_password = ${SMTP_PASSWORD}
        smtp_mail_from = ${SMTP_MAIL_FROM}
        ```

        Il faut aussi définir ces variables dans un fichier .env à la racine du projet (son chemin est en input de la fonction)

        Parameters
        ----------
        env_path : str, optional
            chemin vers le fichier .env, by default ".env"
        """

        env_path = os.path.join(os.getcwd(), relative_env_path)

        print(f"CECI EST LE CHEMIN DE STOCKAGE DU FICHIER .env : {env_path}")

        try:
            # Vérification de l'existence du fichier
            if not os.path.exists(env_path):
                raise FileNotFoundError(f"Le fichier .env n'a pas été trouvé : {env_path}")

            # Chargement du fichier de variables d'environnement
            load_dotenv(dotenv_path=env_path)

        except FileNotFoundError as e:
            print(e)
        except Exception as e:
            print(f"Une erreur est survenue : {e}")

    @task
    def failing_task():

        number = rd.randint(1, 2)

        if number == 2:
            raise Exception("Cette tâche échoue intentionnellement pour tester les notifications email.")
        else:
            # Configuration SMTP
            smtp_host = os.getenv("SMTP_HOST")
            smtp_port = 587
            smtp_user = os.getenv("SMTP_USER")
            smtp_password = os.getenv("SMTP_PASSWORD")
            smtp_mail_from = os.getenv("SMTP_MAIL_FROM")

            # Créer un email texte brut
            subject = "Ceci est un mail de réussite du process"
            to_email = "a.morlas@octime.com"
            body = f"Bonjour, ceci est un email envoyé via Python. Il atteste de la réussite du DAG Airflow, il a été envoyé par {smtp_user}"

            msg = MIMEText(body, "plain")
            msg["Subject"] = subject
            msg["From"] = smtp_mail_from
            msg["To"] = to_email

            # Envoyer l'email
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.starttls()
                server.login(smtp_user, smtp_password)
                server.sendmail(smtp_mail_from, [to_email], msg.as_string())
                print("Email envoyé avec succès.")

    load_variables()
    failing_task()


# Lancer le DAG
email_notification_dag()
