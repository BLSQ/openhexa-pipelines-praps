import datetime
import os
from typing import List

import geopandas as gpd
import pandas as pd
import requests
from sqlalchemy import create_engine
from openhexa.sdk import current_run, parameter, pipeline, workspace
from shapely.geometry import Point


SURVEYS = {
    "FICHE_EVALUATION_ATELIER": "aKP9ER5mZNa3YdcUYowQxz",
    "FICHE_COGES_PARTCIPATION_ FEMMES": "a4XfXz5WgQz9fAq5eP6Mke",
    "FICHE_ CDR _INDICATEURS_REGIONAL_13_04_23-2": "aACjCqdUVNnJAjTmzUFrYH",
    "FICHE_ KBT_INDICATEURS_CDR_ PAYS_VF_13_04_23": "aC5j6nfQURbW9CZ4CJC9xK",
    "FICHE POINT D'EAU": "aNgKpfspFR4x9zJoCzkhAU",
    "FICHE MARCHES A BETAIL": "aF2qn7wtg5yfqsR9HKYzU2",
    "FICHE UNITE VETERINAIRE": "aCDoMSd2nyHBcDjhoyANYU",
    "FICHE PARC DE VACCINATION": "a367o5CCnKDHHqUq6yVmD4",
    "FICHE SOUS PROJETS INNOVANTS": "aNsHRV2CSLjstRHDMRnK76",
    "FICHE MODULE SAISIE REGISTRE_PRAPS_2": "a7a23KSWs4AAHJeGBaPSxt",
    "FICHE PRATIQUES GESTION DURABLE PAYSAGE": "aDtBCJLNdCrHdEpwtrVqWc",
    "FICHE FOURRAGE CULTIVE URC": "aBPLmmNrQBHwqVbUPEGY8T",
    "FICHE_MODULE_FORMATION_DEF": "a9iGL3zLt9ZcswF5XBvUGo",
    "FICHE SOUS PROJET AGR": "aPqUaKpDueEkgzLg6r3cWt",
}


class Field:
    def __init__(self, meta: dict):
        self.meta = meta

    @property
    def uid(self) -> str:
        return self.meta.get("$kuid")

    @property
    def name(self) -> str:
        return self.meta.get("name", "")

    @property
    def label(self) -> str:
        if "label" in self.meta:
            return self.meta["label"][0]
        else:
            return None

    @property
    def type(self) -> str:
        return self.meta.get("type")

    @property
    def list_name(self) -> str:
        return self.meta.get("select_from_list_name")

    @property
    def condition(self) -> str:
        if self.meta.get("relevant"):
            return self.parse_condition(self.meta.get("relevant"))
        else:
            return None

    @staticmethod
    def parse_condition(expression: str) -> dict:
        """Transform the conditionnal expression string into a string that can be
        evaluated by Python.

        Assumes that the variable name that contain record data is named `record`.
        """
        expression = expression.replace("selected(${", "(record.get('")
        expression = expression.replace("${", "record.get('")
        expression = expression.replace("}", "')")
        expression = expression.replace(", ", " == ")
        return expression


class Survey:
    def __init__(self, meta: dict):
        self.meta = meta
        if "content" in meta:
            self.fields = self.parse_fields()
            self.choices = self.parse_choices()

    def __repr__(self) -> str:
        return f'Survey("{self.name}")'

    @property
    def uid(self) -> str:
        return self.meta["uid"]

    @property
    def name(self) -> str:
        return self.meta.get("name")

    @property
    def description(self) -> str:
        return self.meta["settings"].get("description")

    @property
    def country(self) -> str:
        return self.meta["settings"].get("country")

    def parse_fields(self) -> List[Field]:
        return [Field(f) for f in self.meta["content"]["survey"]]

    def parse_choices(self) -> dict:
        choice_lists = {}
        if "choices" not in self.meta["content"]:
            return None
        for choice in self.meta["content"]["choices"]:
            if "list_name" in choice:
                list_name = choice["list_name"]
                if list_name not in choice_lists:
                    choice_lists[list_name] = []
                choice_lists[list_name].append(choice)
        return choice_lists

    def get_field(self, name: str) -> Field:
        return [f for f in self.fields if f.name.lower() == name.lower()][0]


class AuthenticationError(Exception):
    pass


class Api:
    def __init__(self):
        self.url = os.getenv("KOBO_API_URL").rstrip("/")
        self.session = requests.Session()

    def authenticate(self, token: str):
        self.session.headers["Authorization"] = f"Token {token}"

    def check_authentication(self):
        if "Authorization" not in self.session.headers:
            raise AuthenticationError("Not authenticated")

    def list_surveys(self) -> List[dict]:
        """List UID and names of available surveys."""
        surveys = []
        r = self.session.get(f"{self.url}/assets.json")
        assets = r.json()["results"]
        for asset in assets:
            if asset.get("asset_type") == "survey":
                surveys.append({"uid": asset.get("uid"), "name": asset.get("name")})
        return surveys

    def get_survey(self, uid: str) -> dict:
        """Get full survey metadata."""
        r = self.session.get(f"{self.url}/assets/{uid}.json")
        r.raise_for_status()
        return Survey(r.json())

    def get_data(self, survey: Survey) -> dict:
        r = self.session.get(survey.meta["data"])
        return r.json().get("results")


def get_fields_metadata(api: Api, survey_uid: str) -> pd.DataFrame:
    """Build a dataframe with fields UIDs and labels."""
    survey = api.get_survey(survey_uid)
    records = []
    for f in survey.fields:
        if not f.name:
            continue
        if f.name.startswith("group"):
            continue
        record = {"name": f.name, "label": f.label, "type": f.type}
        records.append(record)
    return pd.DataFrame(records)


def get_survey_data(api: Api, survey_uid: str) -> pd.DataFrame:
    """Get survey data as a dataframe."""
    survey = api.get_survey(survey_uid)
    data = api.get_data(survey)
    df = pd.DataFrame(data)

    rename = {}
    columns = []

    for column in df.columns:
        if "group" in column:
            simplified_name = column.split("/")[-1]
            rename[column] = simplified_name
            columns.append(simplified_name)

    columns += ["_status", "_geolocation", "_attachments"]
    df = df.rename(columns=rename)
    df = df[columns]

    def _lat(x):
        if isinstance(x, list):
            if len(x) == 2:
                return x[0]
        return None

    def _lon(x):
        if isinstance(x, list):
            if len(x) == 2:
                return x[1]
        return None

    df["LATITUDE"] = df["_geolocation"].apply(_lat)
    df["LONGITUDE"] = df["_geolocation"].apply(_lon)

    df = df.drop(columns=["_geolocation", "_attachments"])

    return df


def get_survey_geodata(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Get survey data as a geodataframe."""
    geoms = []
    for _, row in df.iterrows():
        if row.LATITUDE and row.LONGITUDE:
            geoms.append(Point(row.LONGITUDE, row.LATITUDE))
        else:
            geoms.append(None)
    geodf = gpd.GeoDataFrame(df, geometry=geoms)
    geodf.crs = "EPSG:4326"
    return geodf


@pipeline("update-geonode", name="Update Geonode")
@parameter(
    "survey_name",
    name="Survey",
    type=str,
    choices=[
        "FICHE_EVALUATION_ATELIER",
        "FICHE_COGES_PARTCIPATION_ FEMMES",
        "FICHE_ CDR _INDICATEURS_REGIONAL_13_04_23-2",
        "FICHE_ KBT_INDICATEURS_CDR_ PAYS_VF_13_04_23",
        "FICHE POINT D'EAU",
        "FICHE MARCHES A BETAIL",
        "FICHE UNITE VETERINAIRE",
        "FICHE PARC DE VACCINATION",
        "FICHE SOUS PROJETS INNOVANTS",
        "FICHE MODULE SAISIE REGISTRE_PRAPS_2",
        "FICHE PRATIQUES GESTION DURABLE PAYSAGE",
        "FICHE FOURRAGE CULTIVE URC",
        "FICHE_MODULE_FORMATION_DEF",
        "FICHE SOUS PROJET AGR",
    ],
    required=True,
)
@parameter(
    "postgres_table",
    name="Database table",
    help="Target table in the workspace database",
    type=str,
    required=False,
)
@parameter(
    "postgis_table",
    name="Database geo-enabled table",
    help="Target PostGIS table in the workspace",
    type=str,
    required=False,
)
@parameter(
    "anonymize",
    name="Anonymize data",
    help="Do not extract personal information",
    type=bool,
    default=True,
)
def update_geonode(
    survey_name: str, postgres_table: str, postgis_table: str, anonymize: bool
):
    """Update Geonode source data for a given survey."""
    api = Api()
    current_run.log_info(
        f"Connecting to KoboToolbox instance {os.getenv('KOBO_API_URL')}..."
    )
    api.authenticate(os.getenv("KOBO_API_TOKEN"))
    survey_uid = SURVEYS[survey_name]

    timestring = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%f")
    survey_fname = survey_name.lower().replace(" ", "_").replace("'", "")
    output_dir = f"{workspace.files_path}/data/extracts/{survey_fname}/{timestring}"
    os.makedirs(output_dir, exist_ok=True)

    extract_fields_metadata(api, survey_name, survey_uid, output_dir)
    data = extract_data(api, survey_name, survey_uid, output_dir)

    if anonymize:
        columns = [col for col in data.columns if not col.startswith("ID")]
        data = data[columns]

    geodata = extract_geodata(data, output_dir, survey_name)

    if postgres_table and postgis_table:
        push_to_database(data, geodata, postgres_table, postgis_table, overwrite=True)


@update_geonode.task
def extract_fields_metadata(
    api: Api, survey_name: str, survey_uid: str, output_dir: str
):
    current_run.log_info(f"Extracting fields metadata for survey {survey_name}...")
    fields = get_fields_metadata(api, survey_uid)
    fpath = os.path.join(output_dir, "fields_metadata.csv")
    fields.to_csv(fpath, index=False)
    current_run.add_file_output(fpath)
    current_run.log_info(f"Written fields metadata into {fpath}")
    return


@update_geonode.task
def extract_data(api: Api, survey_name: str, survey_uid: str, output_dir: str):
    current_run.log_info(f"Extracting data for survey {survey_name}")
    data = get_survey_data(api, survey_uid)
    current_run.log_error(f"No data found in survey {survey_name}")
    fpath = os.path.join(output_dir, "survey_data.csv")
    data.to_csv(fpath, index=False)
    current_run.add_file_output(fpath)
    current_run.log_info(f"Written survey data into {fpath}")
    return data


@update_geonode.task
def extract_geodata(data: pd.DataFrame, output_dir: str, survey_name: str):
    current_run.log_info(f"Extracting data for survey {survey_name}")
    geodata = get_survey_geodata(data)
    fpath = os.path.join(output_dir, "survey_data.gpkg")
    geodata.to_file(fpath, driver="GPKG")
    current_run.add_file_output(fpath)
    current_run.log_info(f"Written survey geodata into {fpath}")
    return geodata


@update_geonode.task
def push_to_database(
    df: pd.DataFrame,
    geodf: gpd.GeoDataFrame,
    postgres_table: str,
    postgis_table: str,
    overwrite: bool,
):
    engine = create_engine(workspace.database_url)
    if_exists = "replace" if overwrite else "fail"
    df.to_sql(postgres_table, engine, if_exists=if_exists)
    current_run.log_info(f"Pushed data to database table {postgres_table}")
    geodf.to_postgis(postgis_table, engine, if_exists=if_exists)
    current_run.log_info(f"Pushed geodata to database table {postgis_table}")


if __name__ == "__main__":
    update_geonode()
