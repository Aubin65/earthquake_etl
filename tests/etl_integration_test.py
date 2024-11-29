"""
Ce script python est utilisé pour tester les DAGs et fonctions mises en place dans ce projet
"""

# Import des librairies nécessaires
from airflow.models import DagBag
from airflow.utils.state import State
import pendulum


def test_dag_loading():
    """
    Fonction de test sur le chargement du DAGs
    Source : https://www.restack.io/docs/airflow-knowledge-apache-unit-testing
    """

    # Récupération du DAG
    dag_bag = DagBag()

    # On s'assure que le DAG est bien présent dans la liste des DAGs
    assert "earthquake_etl" in dag_bag.dags, "DAG earthquake_etl inexistant"

    # Récupération du DAG earthquake_etl
    dag = dag_bag.get_dag("earthquake_etl")

    # Vérifier le nombre de tâches du DAG
    assert len(dag.tasks) == 3

    # Vérifier les tâches attendues du DAG
    assert dag.task_ids == ["extract", "transform", "load"], "Tâches non conformes"


def test_task_dependencies():
    """
    Fonction de test des dépendances dans le DAG earthquake_etl
    Source : https://www.restack.io/docs/airflow-knowledge-apache-unit-testing
    """

    # Récupération du DAG
    dag = DagBag().get_dag("earthquake_etl")

    # Récupérer ces tâches
    tasks = dag.tasks

    # Initialisation des dépendances attendues
    dependencies = {
        "extract": {"downstream": ["transform"], "upstream": []},
        "transform": {"downstream": ["load"], "upstream": ["extract"]},
        "load": {"downstream": [], "upstream": ["transform"]},
    }

    # Tests sur les dépendances
    for task in tasks:
        assert task.downstream_task_ids == set(
            dependencies[task.task_id]["downstream"]
        ), "Dépendances downstream non correspondantes"
        assert task.upstream_task_ids == set(
            dependencies[task.task_id]["upstream"]
        ), "Dépendances upstream non correspondantes"


def test_full_dag_execution():
    """
    Fonction de test (end-to-end) du run complet du DAG earthquake_etl
    NB : Pour le moment, ce test n'est pas fonctionnel. Il run complètement le DAG en prod (sans mock) mais certaines instances ne fonctionnent pas
    """

    # Récupération du DAG
    dag = DagBag().get_dag("earthquake_etl")

    # Nettoyage des exécutions passées
    dag.clear(start_date=pendulum.now("UTC").subtract(hours=3))

    # Lancement du DAG
    dag.run()

    # Tests des états des différentes tâches
    assert all(
        task_instance.state == State.SUCCESS for task_instance in dag.get_task_instances()
    ), "Toutes les tâches ne sont pas un succès"
