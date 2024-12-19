"""
Ce fichier est utilisé pour mettre en place les fonctions d'encodage et de décodage pour la manipulation de données avec HBase
"""

# Import des librairies nécessaires
import struct


# Définition de la fonction encode qui permettre d'encoder plus facilement les données :
def var_to_bytes(var) -> bytes | None:
    """Permet d'encoder une variable du type str, int ou float

    Parameters
    ----------
    var : _type_
        variable d'entrée de la fonction

    Returns
    -------
    bytes | None
        variable encodée

    Raises
    ------
    TypeError
        TypeError si la variable n'est pas du type attendu
    """

    # Cas ou aucune variable n'est donnée en entrée
    if not var:
        return None

    # Cas d'une chaîne de caractères
    if isinstance(var, str):
        return var.encode("utf-8", errors="replace")

    # Cas d'un entier
    if isinstance(var, int):
        return struct.pack(">i", var)

    # Cas d'un nombre décimal
    if isinstance(var, float):
        return struct.pack("f", var)

    # Gestion des autres cas
    else:
        print(f"La variable qui n'a pas pu être encodée est la suivante : {var}")
        raise TypeError("Le type de la variable fourni n'est pas attendu")


def try_decode_float(data: bytes) -> float | None:
    """Fonction de décodage d'un nombre décimal

    Parameters
    ----------
    data : _type_
        variable d'entrée

    Returns
    -------
    float | None
        retourne un flottant si possible, sinon None
    """

    # Cas où on peut renvoyer un flottant
    try:
        return struct.unpack(">f", data)[0]

    # Cas échéant
    except struct.error:
        return None


def try_decode_int(data: bytes) -> int | None:
    """Fonction de décodage d'un nombre entier

    Parameters
    ----------
    data : bytes
        variable d'entrée

    Returns
    -------
    int | None
        retourne un entier si possible, sinon None
    """

    # Cas où on peut renvoyer un entier
    try:
        return struct.unpack(">i", data)[0]

    # Cas échéant
    except struct.error:
        return None


def try_decode_str(data: bytes) -> str | None:
    """Fonction de décodage d'une chaîne de caractères

    Parameters
    ----------
    data : bytes
        variable d'entrée

    Returns
    -------
    str | None
        retourne une chaîne de caractères si possible, sinon None
    """

    # Cas où on peut renvoyer une chaîne de caractères
    try:
        return data.decode("utf-8")

    # Cas échéant
    except (UnicodeDecodeError, TypeError):
        return None


def bytes_to_var(var: bytes) -> float | int | str | None:
    """Fonction de décodage d'une variable de type float, int ou str

    Parameters
    ----------
    var : bytes
        variable d'entrée

    Returns
    -------
    float | int | str | None
        retourne le type de données possible, sinon None
    """

    # Cas d'un flottant
    decoded_float = try_decode_float(var)
    if decoded_float is not None:
        return decoded_float

    # Cas d'un entier
    decoded_int = try_decode_int(var)
    if decoded_int is not None:
        return decoded_int

    # Cas d'une chaîne de caractères
    decoded_str = try_decode_str(var)
    if decoded_str is not None:
        return decoded_str

    # Cas échéant
    else:
        return None
