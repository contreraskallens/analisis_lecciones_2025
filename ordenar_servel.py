import json
from typing import TypeAlias, cast

import pandas as pd
from isal import igzip
from rich import print

PRIMERA_VUELTA: str = "primera_vuelta"
SEGUNDA_VUELTA: str = "segunda_vuelta"

KEEP_VARS: list[str] = [
    "id_region",
    "id_cirsen",
    "id_distrito",
    "id_provincia",
    "orden_comuna",
    "id_comuna",
    "id_mesa",
    "mesa",
    "id_local",
    "orden_local",
    "blancos",
    "nulos",
    "total_general",
    "electores",
]
KEY_CANDIDATOS: dict[str, str] = {
    "1": "Parisi",
    "2": "Jara",
    "3": "MEO",
    "4": "Kaiser",
    "5": "Kast",
    "6": "Artes",
    "7": "Matthei",
    "8": "HMN",
}

mesa: TypeAlias = dict[str, str | int | None | list[dict[str, int | None]]]


def get_json_eleccion(nombre_archivo: str) -> list[mesa]:
    with igzip.open(  # pyright: ignore[reportUnknownMemberType]
        nombre_archivo + ".json.gz", mode="rt", encoding="utf-8"
    ) as gzip_content:
        json_eleccion = cast(list[mesa], json.loads(gzip_content.read()))

    return json_eleccion


def get_info_mesa(dict_mesa: mesa) -> pd.Series:
    parsed: pd.Series = pd.Series(dict_mesa)
    info_mesa = cast(pd.Series, parsed[KEEP_VARS])
    dict_votos: dict[str, int] = {}
    votos_candidatos = cast(
        list[dict[str, int]], dict_mesa["candidatos"]
    )  # También puede ser None porque electo es 3 o None, pero bueh
    candidatos: list[dict[str, int]] = sorted(
        votos_candidatos,
        key=lambda x: x["orden_voto"],
    )
    for candidato in candidatos:
        candidato_nombre: str = KEY_CANDIDATOS[str(candidato["orden_voto"])]
        candidato_votos: int = candidato["votos"]
        dict_votos[candidato_nombre] = candidato_votos
    votos = pd.Series(dict_votos)
    votos_mesa: pd.Series = pd.concat([info_mesa, votos])
    return votos_mesa


def get_eleccion_procesada(todas_mesas: list[mesa]) -> list[pd.Series]:
    mesas_procesadas: list[pd.Series] = []
    for esta_mesa in todas_mesas:
        mesas_procesadas.append(get_info_mesa(esta_mesa))
    return mesas_procesadas


def save_mesas_juntas(info_mesas: list[pd.Series], nombre_archivo: str) -> None:
    all_votos = pd.concat(info_mesas, axis=1).T
    all_votos.to_csv(nombre_archivo)  # pyright: ignore[reportUnknownMemberType]


if __name__ == "__main__":
    primera_vuelta: list[mesa] = get_json_eleccion(PRIMERA_VUELTA)
    primera_vuelta_procesada: list[pd.Series] = get_eleccion_procesada(primera_vuelta)
    save_mesas_juntas(primera_vuelta_procesada, "primera_vuelta.csv")

    segunda_vuelta: list[mesa] = get_json_eleccion(SEGUNDA_VUELTA)
    segunda_vuelta_procesada: list[pd.Series] = get_eleccion_procesada(segunda_vuelta)
    save_mesas_juntas(segunda_vuelta_procesada, "segunda_vuelta.csv")
