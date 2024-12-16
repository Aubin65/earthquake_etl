"""
Ce fichier est utilisé pour mettre en place les fonctions d'encodage et de décodage pour la manipulation de données avec HBase
"""

# Import des librairies nécessaires
import struct


# Définition de la fonction encode qui permettre d'encoder plus facilement les données :
def var_to_bytes(var) -> bytes:
    """Permet d'encoder une variable du type str, int ou float

    Parameters
    ----------
    var : _type_
        variable d'entrée de la fonction

    Returns
    -------
    bytes
        variable encodée
    """

    if isinstance(var, str):
        return var.encode("utf-8")

    if isinstance(var, int):
        return struct.pack(">I", var)

    if isinstance(var, float):
        return struct.pack("f", var)


def try_decode_float(data):
    try:
        return struct.unpack(">f", data)[0]
    except struct.error:
        return None


def try_decode_int(data):
    try:
        return struct.unpack(">I", data)[0]
    except struct.error:
        return None


def try_decode_str(data):
    try:
        data.decode("utf-8")
    except (UnicodeDecodeError, TypeError):
        return None


def bytes_to_var(var: bytes):

    decoded_float = try_decode_float(var)
    if decoded_float is not None:
        return decoded_float

    decoded_int = try_decode_int(var)
    if decoded_int is not None:
        return decoded_int

    decoded_str = try_decode_str(var)
    if decoded_str is not None:
        return decoded_str
