"""
Ce script est utilisé pour charger les variables d'environnement utilisées par le fichier airflow.cfg.
Il faut au préalable avoir défini les variables suivantes de la manière suivante dans le fichier cité précédemment :

```
smtp_host = ${SMTP_HOST}
smtp_user = ${SMTP_USER}
smtp_password = ${SMTP_PASSWORD}
smtp_mail_from = ${SMTP_MAIL_FROM}
```

Il faut aussi définir ces variables dans un fichier .env à la racine du projet
"""

from dotenv import load_dotenv
import os


def load_variables(env_path: str = "../.env") -> None:
    """Fonction de chargement des variables d'environnement

    Parameters
    ----------
    env_path : str, optional
        chemin vers le fichier .env, by default "../.env"
    """

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
