from __future__ import annotations

import requests
import pandas as pd


def fetch_open_meteo_hourly_to_df(
    latitude: float,
    longitude: float,
    timezone: str = "Europe/Paris",
    timeout_seconds: int = 10,
) -> pd.DataFrame:
    """
    Appelle l'API Open-Meteo et retourne un DataFrame "propre" à partir des données hourly.

    Le DataFrame final aura typiquement :
      - une colonne datetime (timezone incluse)
      - une colonne temperature_2m
      - un index sur datetime (pratique pour analyses time-series)
    """

    # 1) Endpoint
    url = "https://api.open-meteo.com/v1/forecast"

    # 2) Paramètres : on demande une série horaire "temperature_2m"
    #    Tu peux en demander plusieurs en les séparant par des virgules :
    #      "temperature_2m,relative_humidity_2m,precipitation"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "temperature_2m",
        "timezone": timezone,
    }

    headers = {
        "Accept": "application/json",
        "User-Agent": "audit-data-demo/1.0",
    }

    # 3) Requête HTTP
    response = requests.get(url, params=params, headers=headers, timeout=timeout_seconds)

    # 4) Gestion d'erreurs HTTP
    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(
            f"HTTP error {response.status_code} calling Open-Meteo: {response.text[:300]}"
        ) from e

    # 5) JSON -> dict Python
    data = response.json()

    # 6) Validation rapide de structure
    hourly = data.get("hourly")
    if not hourly or "time" not in hourly:
        raise ValueError(
            "Unexpected JSON structure: missing hourly/time. "
            f"Root keys: {list(data.keys())}"
        )

    # 7) Construction du DataFrame
    #    hourly ressemble à :
    #    {
    #       "time": ["2026-01-11T00:00", ...],
    #       "temperature_2m": [2.1, 2.0, ...]
    #    }
    #
    #    pd.DataFrame(hourly) crée une table avec colonnes = clés
    df = pd.DataFrame(hourly)

    # 8) Nettoyage typique (la "database propre")
    # 8.a) convertir "time" en datetime (timezone déjà cohérente car renvoyée par API selon timezone param)
    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    # 8.b) convertir les colonnes numériques (au cas où l'API renvoie du texte / None)
    #      (Ici uniquement temperature_2m car on ne demande que ça)
    if "temperature_2m" in df.columns:
        df["temperature_2m"] = pd.to_numeric(df["temperature_2m"], errors="coerce")

    # 8.c) supprimer les lignes invalides (pas de time)
    df = df.dropna(subset=["time"])

    # 8.d) supprimer doublons (par prudence)
    df = df.drop_duplicates(subset=["time"], keep="last")

    # 8.e) trier par date
    df = df.sort_values("time")

    # 8.f) index time-series (optionnel mais très pratique)
    df = df.set_index("time")

    # 8.g) option : renommer colonnes (plus “BI”)
    df = df.rename(columns={"temperature_2m": "temp_2m_celsius"})

    return df


if __name__ == "__main__":
    # Exemple : Paris
    df_hourly = fetch_open_meteo_hourly_to_df(latitude=45.13573722040256, longitude= 5.714254381300856)

    # Aperçu
    """
    print(df_hourly.head(10))
    print("\nInfos colonnes:")
    print(df_hourly.dtypes)"""

    # Exemple d'usage : garder seulement les 24 prochaines heures
    print("\n24 prochaines heures:")
    print(df_hourly.head(24))
    
    """
    # Exemple d'usage : stats rapides
    print("\nStats:")
    print(df_hourly["temp_2m_celsius"].describe())"""

