import logging
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import requests

logger = logging.getLogger(__name__)


class AFIPService:
    def __init__(
        self,
        username: str,
        password: str,
        base_url: str,
        chunk_size: int,
        max_calls: int,
        pause_duration: int,
        max_retries: int,
        retry_delay: int,
        services_available: Optional[List[str]] = None,
    ) -> None:
        """
        Initializes the AFIPService instance with credentials and configuration parameters.

        Parameters:
            username (str): Username for authentication.
            password (str): Password.
            base_url (str): Base URL of the API.
            chunk_size (int): Size of chunks for splitting the requests.
            max_calls (int): Maximum number of consecutive calls before pausing.
            pause_duration (int): Pause time in seconds after reaching max_calls.
            max_retries (int): Maximum number of retry attempts in case of failure.
            retry_delay (int): Base wait time in seconds between retries (will be multiplied exponentially).
            services_available (List[str]): List of available services. Defaults to ["inscription", "padron"] if None.
        """
        self.username = username
        self.password = password
        self.base_url = base_url
        self.chunk_size = chunk_size
        self.max_calls = max_calls
        self.pause_duration = pause_duration
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.services_available = (
            services_available
            if services_available is not None
            else ["inscription", "padron"]
        )

        # Use a persistent session for all requests
        self.session = requests.Session()

        # Retrieve the authentication token
        self.token = self._get_token()

    def _get_token(self) -> Optional[str]:
        """
        Retrieves the authentication token from the API.

        Returns:
            Optional[str]: JWT access token if successful, None otherwise.
        """
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {"username": self.username, "password": self.password}
        token_url = f"{self.base_url}/token"
        try:
            response = self.session.post(token_url, data=data, headers=headers)
            if response.status_code == 200:
                token = response.json().get("access_token")
                logger.info("Token acquired successfully.")
                return token
            else:
                logger.error("Error acquiring token: %s", response.text)
                return None
        except Exception as e:
            logger.error("Exception during token acquisition: %s", e)
            return None

    def _refresh_token(self) -> bool:
        """
        Refreshes the authentication token.

        Returns:
            bool: True if the token was refreshed successfully, False otherwise.
        """
        logger.info("Refreshing authentication token...")
        new_token = self._get_token()
        if new_token:
            self.token = new_token
            return True
        return False

    def _check_instance(self, service_name: str) -> bool:
        """
        Checks if the requested service is available and if a valid token exists.

        Parameters:
            service_name (str): The service to check.

        Returns:
            bool: True if the service is available and the token is valid, False otherwise.
        """
        if service_name not in self.services_available:
            logger.error("Unknown service: %s", service_name)
            return False
        if not self.token:
            logger.error("No valid token available.")
            return False
        return True

    def _query_service(
        self, service_name: str, person_ids: List[Any]
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Queries the specified service by sending a JSON payload with a list of person IDs.

        This method first verifies that the service instance is valid and then sends a POST
        request to the service URL with the given person IDs. If the access token has expired
        (HTTP 401), it attempts to refresh the token and retries the request once.

        Parameters:
            service_name (str): The service to query (e.g., "inscription" or "padron").
            person_ids (List[Any]): List of person IDs to query.

        Returns:
            Optional[List[Dict[str, Any]]]: A list of cleaned response dictionaries if the
            request is successful; None if any error occurs.
        """
        # Validate if the service instance is available and the token is valid
        if not self._check_instance(service_name):
            logger.error("Invalid service instance for '%s'.", service_name)
            return None

        url = f"{self.base_url}/{service_name}"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        payload = {"persona_ids": person_ids}

        try:
            response = self.session.post(url, json=payload, headers=headers)
            # If unauthorized, try refreshing the token once
            if response.status_code == 401:
                logger.warning(
                    "Received 401 Unauthorized for service '%s'. Attempting token refresh...",
                    service_name,
                )
                if self._refresh_token():
                    # Update header with the new token
                    headers["Authorization"] = f"Bearer {self.token}"
                    response = self.session.post(url, json=payload, headers=headers)
                else:
                    logger.error("Token refresh failed for service '%s'.", service_name)
                    return None

            if response.ok:
                # Extract the 'data' field from the JSON response (default to
                # empty list if missing)
                response_data = response.json().get("data", [])
                # Clean each dictionary in the response data using the static
                # method clean_dict
                cleaned_data = [AFIPService.clean_dict(item) for item in response_data]
                return cleaned_data
            else:
                logger.error(
                    "Error querying service '%s': %s", service_name, response.text
                )
                return None

        except requests.exceptions.RequestException as req_err:
            logger.error(
                "Request error while querying service '%s': %s", service_name, req_err
            )
            return None

    def _request_with_retry(
        self, service_name: str, fragment: List[Any]
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Attempts to query the specified service with retries using exponential backoff.

        The method calls the query_service function for the provided chunk (fragment) of IDs.
        If the response is empty or invalid, it waits for an exponentially increasing delay
        before retrying, up to the maximum number of retries.

        Parameters:
            service_name (str): The service to query.
            fragment (List[Any]): A chunk of person IDs to query.

        Returns:
            Optional[List[Dict[str, Any]]]: The service response if successful; None if all retries fail.
        """
        for attempt in range(1, self.max_retries + 1):
            response = self._query_service(service_name, fragment)
            if response:
                return response
            else:
                logger.warning(
                    "Attempt %d for service '%s' failed: empty or invalid response.",
                    attempt,
                    service_name,
                )
                if attempt < self.max_retries:
                    # Calculate the delay using exponential backoff
                    backoff_delay = self.retry_delay * (2 ** (attempt - 1))
                    logger.info("Retrying in %d seconds...", backoff_delay)
                    time.sleep(backoff_delay)
        logger.error(
            "All %d retry attempts failed for service '%s'.",
            self.max_retries,
            service_name,
        )
        return None

    def fetch_data_service(
        self, service_name: str, person_ids: List[Any]
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Fetches and aggregates data from the specified service by splitting person IDs into chunks,
        applying retries with exponential backoff, and pausing after a set number of consecutive calls.

        This method first checks if the service instance and token are valid. Then, if the list of
        person IDs exceeds the defined chunk size, it divides them into smaller chunks. For each chunk,
        it uses the retry mechanism to query the service and aggregates the cleaned responses.

        Parameters:
            service_name (str): The service to query.
            person_ids (List[Any]): Complete list of person IDs to process.

        Returns:
            Optional[List[Dict[str, Any]]]: A list of aggregated cleaned response data from all chunks,
            or None if an error occurs.
        """
        # Verify if the service is available and the token is valid
        if not self._check_instance(service_name):
            logger.error("Service instance check failed for '%s'.", service_name)
            return None

        # Split person IDs into chunks if they exceed the predefined chunk
        # size.
        if len(person_ids) > self.chunk_size:
            person_ids = [
                person_ids[i : i + self.chunk_size]
                for i in range(0, len(person_ids), self.chunk_size)
            ]
            logger.info("Split person IDs into %d chunks.", len(person_ids))

        total_data: List[Dict[str, Any]] = []

        for index, chunk in enumerate(person_ids):
            # Pause execution after reaching the maximum allowed consecutive
            # calls
            if index == self.max_calls:
                logger.info(
                    "Reached maximum consecutive calls (%d). Pausing for %d seconds...",
                    self.max_calls,
                    self.pause_duration,
                )
                time.sleep(self.pause_duration)
                logger.debug(f"List of nits for request: {chunk}")
            response = self._request_with_retry(service_name, chunk)
            if response:
                logger.debug(response)
                total_data.extend(response)
            else:
                logger.error(
                    "Failed to retrieve data for chunk %d of service '%s'.",
                    index + 1,
                    service_name,
                )

        return AFIPService.format_response(total_data)

    def check_health(self, service_name: str) -> Optional[Tuple[int, str]]:
        """
        Checks the health status of the specified service.

        Parameters:
            service_name (str): The service to check.

        Returns:
            Optional[Tuple[int, str]]: (HTTP status code, response text) or None in case of errors.
        """
        if not self._check_instance(service_name):
            return None

        url = f"{self.base_url}/{service_name}/health"
        headers = {"Authorization": f"Bearer {self.token}"}
        try:
            response = self.session.get(url, headers=headers)
            return response.status_code, response.text
        except Exception as e:
            logger.error(
                "Exception checking health for service '%s': %s", service_name, e
            )
            return None

    def get_services_available(self) -> List[str]:
        """
        Returns the list of available services.
        """
        return self.services_available

    @staticmethod
    def clean_dict(
        data: Union[Dict[Any, Any], List[Any]],
    ) -> Union[Dict[Any, Any], List[Any]]:
        """
        Recursively removes keys with None values or empty lists from a dictionary.
        Also cleans nested dictionaries and lists.

        Parameters:
            data (Union[dict, list]): Data to be cleaned.

        Returns:
            Union[dict, list]: Cleaned data.
        """
        if isinstance(data, dict):
            return {
                key: AFIPService.clean_dict(value)
                for key, value in data.items()
                if value not in [None, []]
            }
        elif isinstance(data, list):
            return [AFIPService.clean_dict(item) for item in data]
        else:
            return data

    # Esto hasta que se mejore o cambie el formato de respuesta
    @staticmethod
    def format_response(data: List[Dict[Any, Any]]) -> Dict[Any, Any]:
        """
        Transforma la respuesta original del servicio (lista de diccionarios)
        en un único diccionario con la siguiente estructura:

        {
            "27296094884": { ... },
            "27283935650": { ... },
            ...
        }

        Parameters:
            data (List[Dict[Any, Any]]): Lista de diccionarios que contienen la información.

        Returns:
            Dict[Any, Any]: Diccionario formateado con el id de persona como clave.
        """
        formatted = {}
        for item in data:
            # Cada 'item' es un diccionario con un único par clave-valor.
            formatted.update(item)
        return formatted

    @staticmethod
    def extract_errors(record: Dict[str, Any], error_keys: List[str]) -> List[Any]:
        """
        Iterates over specified error keys in a record and accumulates any found errors.

        For each key in error_keys:
        - If the key is present:
            - If its value is a dict and contains the key 'error':
                - If the 'error' value is a list, extend the errors list with its items.
                - Otherwise, append the single error.
            - If the value is not a dict, add it directly.

        Args:
            record (Dict[str, Any]): A dictionary that may contain error information.
            error_keys (List[str]): List of keys to check in the record.

        Returns:
            List[Any]: A list of errors extracted from the record.
        """
        errors: List[Any] = []
        logger.debug("Starting error extraction for record: %s", record)

        for key in error_keys:
            if key in record:
                error_info = record[key]
                logger.debug("Found key '%s' with value: %s", key, error_info)

                if isinstance(error_info, dict):
                    if "error" in error_info:
                        extracted_error = error_info["error"]
                        if isinstance(extracted_error, list):
                            logger.debug(
                                "The 'error' value for key '%s' is a list: %s",
                                key,
                                extracted_error,
                            )
                            errors.extend(extracted_error)
                        else:
                            logger.debug(
                                "The 'error' value for key '%s' is not a list: %s",
                                key,
                                extracted_error,
                            )
                            errors.append(extracted_error)
                else:
                    logger.debug(
                        "The value for key '%s' is not a dict; adding it directly", key
                    )
                    errors.append(error_info)

        logger.debug("Error extraction complete. Errors found: %s", errors)
        return errors

    @staticmethod
    def accumulate_errors_in_data(
        data: Dict[str, Any], error_keys: List[str]
    ) -> Dict[str, Any]:
        """
        Processes each record in the data dictionary to accumulate errors found.

        For each record:
        - Uses extract_errors() to retrieve errors.
        - If only one error is found, stores it directly; if multiple errors are found, stores them as a list.

        Args:
            data (Dict[str, Any]): A dictionary where each key is an identifier and its value is the record.
            error_keys (List[str]): List of keys indicating possible errors.

        Returns:
            Dict[str, Any]: A mapping from identifiers to their respective error(s).
        """
        error_accumulator: Dict[str, Any] = {}
        logger.debug("Starting accumulation of errors from the data dictionary.")

        for identifier, record in data.items():
            logger.debug(
                "Processing record with identifier '%s': %s", identifier, record
            )
            errors = AFIPService.extract_errors(record, error_keys)
            if errors:
                # If there's only one error, store it directly; otherwise, store
                # the list of errors
                error_value = errors[0] if len(errors) == 1 else errors
                error_accumulator[identifier] = error_value
                logger.debug(
                    "Accumulated errors for identifier '%s': %s",
                    identifier,
                    error_value,
                )

        logger.debug("Error accumulation complete: %s", error_accumulator)
        return error_accumulator
