import logging
import os

import pandas as pd
from dotenv import load_dotenv

from afip_service import AFIPService
from utils import INSCRIPTION_ERROR_KEYS, filter_dictionary, save_report_json

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Configure logging to save logs in 'error_report.log' with UTF-8 encoding
logging.basicConfig(
    level=logging.INFO,
    filename="monotributo.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
)
logger = logging.getLogger(__name__)


def process_person_data(person_data: dict) -> dict | None:
    """
    Procesa la información de una persona y devuelve un diccionario con los datos formateados.
    Se retornará None si no hay datos de monotributo ni de régimen general.

    Args:
        person_data (dict): Diccionario que puede contener:
            - datosGenerales
            - datosMonotributo
            - datosRegimenGeneral

    Returns:
        dict | None: Diccionario con los datos procesados o None si no hay datos relevantes.
    """
    generales = person_data.get("datosGenerales", {})
    datos_monotributo = person_data.get("datosMonotributo")
    datos_regimen_general = person_data.get("datosRegimenGeneral")

    # Retornar None si no se tienen datos importantes
    if not (datos_monotributo or datos_regimen_general):
        return None

    processed_data = {"es_monotributista": bool(datos_monotributo)}

    if generales:
        # Se extrae y asigna el tipo de persona
        tipo_persona = generales.get("tipoPersona")
        processed_data["tipo_persona"] = tipo_persona

        if tipo_persona == "FISICA":
            if nombre := generales.get("nombre"):
                processed_data["nombre"] = str(nombre).strip().capitalize()
            if apellido := generales.get("apellido"):
                processed_data["apellido"] = str(apellido).strip().capitalize()
        else:
            if razon_social := generales.get("razonSocial"):
                processed_data["razon_social"] = str(razon_social).strip().upper()

    if datos_monotributo is not None:
        processed_data["datos_monotributo"] = datos_monotributo

    if datos_regimen_general is not None:
        processed_data["datos_regimen_general"] = datos_regimen_general

    return processed_data


def process_all_data(data: dict) -> dict:
    """
    Procesa un diccionario de personas aplicando process_person_data a cada registro.
    Solo se incluirán aquellos registros que tengan datos de monotributo o de régimen general.

    Args:
        data (dict): Diccionario donde cada clave es un identificador (ej. CUIT) y cada valor es la
                     información de la persona.

    Returns:
        dict: Diccionario con el mismo identificador y los datos procesados.
    """
    return {
        person_id: processed
        for person_id, person_data in data.items()
        if (processed := process_person_data(person_data)) is not None
    }


def main():
    try:
        # Read parameters from environment variables (or use default values if
        # needed)
        username = os.getenv("AFIP_USERNAME")
        password = os.getenv("AFIP_PASSWORD")
        base_url = os.getenv("AFIP_BASE_URL")
        chunk_size = int(os.getenv("AFIP_CHUNK_SIZE"))
        max_calls = int(os.getenv("AFIP_MAX_CALLS"))
        pause_duration = int(os.getenv("AFIP_PAUSE_DURATION"))
        max_retries = int(os.getenv("AFIP_MAX_RETRIES"))
        retry_delay = int(os.getenv("AFIP_RETRY_DELAY"))
        # For 'AFIP_SERVICES_AVAILABLE', assume it's a comma-separated list in
        # the .env file
        services_available = os.getenv("AFIP_SERVICES_AVAILABLE", "").split(",")

        # Load the Excel file with NITs to consult.
        # It is assumed that the Excel file has a column named 'nro_nit' with
        # the NITs.
        excel_file = os.getenv("EXCEL_FILE_PATH")
        df = pd.read_excel(excel_file)
        # Ensure there are no missing values and convert the column to integers
        df["nro_nit"] = df["nro_nit"].fillna(0).astype(int)
        nit_list = df["nro_nit"].dropna().unique().tolist()
        # Log the total number of NITs to consult
        logger.info("Total NITs to consult: %d", len(nit_list))
    except Exception as e:
        logger.error("Error reading configuration or Excel file: %s", str(e))
        return

    # Create an instance of AFIPService with the provided parameters
    service = AFIPService(
        username=username,
        password=password,
        base_url=base_url,
        chunk_size=chunk_size,
        max_calls=max_calls,
        pause_duration=pause_duration,
        max_retries=max_retries,
        retry_delay=retry_delay,
        services_available=services_available,
    )

    # Fetch data from the AFIP service
    fetched_data = service.fetch_data_service("inscription", nit_list)
    logger.info("Data fetched: %d records", len(fetched_data))

    # Accumulate errors found in the fetched data
    data_with_error = service.accumulate_errors_in_data(
        fetched_data, INSCRIPTION_ERROR_KEYS
    )
    logger.info("Records with errors: %d", len(data_with_error))
    logger.debug("Error record keys: %s", data_with_error.keys())
    logger.debug("Fetched data keys: %s", fetched_data.keys())

    # Filter out the records with errors from the fetched data
    fetched_data = filter_dictionary(fetched_data, data_with_error)

    # Process data for maping info
    fetched_data = process_all_data(fetched_data)

    # Save JSON reports
    save_report_json(len(nit_list), "personas_con_info", fetched_data)


if __name__ == "__main__":
    main()
