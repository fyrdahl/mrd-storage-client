import pickle
from typing import Any, Optional, Dict, List, Iterator, Union

import requests
from requests.adapters import HTTPAdapter
from requests_toolbelt.sessions import BaseUrlSession
from urllib3.util.retry import Retry

DEFAULT_TIMEOUT = 3

# API documentation: https://github.com/ismrmrd/mrd-storage-server/blob/main/README.md


class StorageException(Exception):
    """Base exception for Storage"""


class ConnectionStorageException(StorageException):
    """Raised when a connection error occured"""


class SerializeStorageException(StorageException):
    """Raised when a (de-)serialization error occured"""


class HealthcheckStorageException(StorageException):
    """Raised when an healthcheck failed"""


class Blob:
    def __init__(self, **kwargs) -> None:
        """Initializes a Blob instance, dynamically assigning the given keyword arguments 
        as attributes to the instance.
        """
        self.__dict__.update(**kwargs)

    def get_data(self) -> bytes:
        """Fetch data from blob"""
        response = requests.get(self.data)
        return response.content
    
    def get(self, key: str, default=None):
        """Provides a dict-like get method for the Blob instance."""
        return getattr(self, key, default)


class Storage:
    def __init__(self, address: str, port: int, subject: str ="$null", device: Optional[str] = None, session: Optional[str] = None):
        self.address = address
        self.port = port

        base_url = f"http://{address}:{port}/"
        self.http = init_http(base_url)

        # Mandatory tags
        self.base_params = {"subject": subject}

        # Optional tags
        if device:
            self.base_params["device"] = device

        if session:
            self.base_params["session"] = session

        self.subject = subject
        self.device = device
        self.session = session

    def healthcheck(self) -> None:
        """Healthcheck can be used to verify that that the server is functioning.
        Raises HealthcheckStorageException if healthcheck failed or
        ConnectionStorageException if the connection failed.

        Example:
            >>> storage = Storage("localhost", 3333)
            >>> storage.healthcheck()
        """
        try:
            response = self.http.get("healthcheck")
            if not response.ok:
                raise HealthcheckStorageException
        except requests.exceptions.ConnectionError:
            raise ConnectionStorageException

    def store(self, obj, name=None, ttl=None, custom_tags=None):
        """Serialize and store an object as a blob.
        Raises SerializeStorageException if the serialization failed.

        Example:
            >>> import numpy as np
            >>> storage = Storage("localhost", 3333)
            >>> obj = {"key": "value"}
            >>> storage.store(obj)

        """
        if custom_tags is None:
            custom_tags = {}
        try:
            payload = pickle.dumps(obj)
        except pickle.PicklingError as e:
            raise SerializeStorageException(e) from e

        new_params = dict(self.base_params)

        if name:
            new_params["name"] = name
        if ttl:
            new_params["_ttl"] = ttl

        new_params = {**new_params, **custom_tags}
        self.http.post("v1/blobs/data", payload, params=new_params)

    def fetch(self, name=None, at=None, custom_tags=None):
        """Return a list containing the data from all matching blobs

        Example:
            >>> storage = Storage("localhost", 3333)
            >>> data = storage.fetch()

        """
        if custom_tags is None:
            custom_tags = {}

        blobs = self._search(name, at, custom_tags)
        return [self._load_object(blob.get_data()) for blob in blobs]

    def fetch_blobs(self, name=None, at=None, custom_tags=None):
        """Yield iterator for all matching blob objects

        Example:
            >>> storage = Storage("localhost", 3333)
            >>> for blob in storage.fetch_blobs():
            >>>     data = blob.get_data()

        """
        if custom_tags is None:
            custom_tags = {}

        yield from self._search(name, at, custom_tags)

    def fetch_latest(self, name=None, at=None, custom_tags=None):
        """Shortcut to get the data from the latest blob matching a search query

        Example:
            >>> storage = Storage("localhost", 3333)
            >>> data = storage.fetch_latest()

        """
        if custom_tags is None:
            custom_tags = {}

        new_params = dict(self.base_params)

        if name:
            new_params["name"] = name
        if at:
            new_params["_at"] = at

        new_params = {**new_params, **custom_tags}
        response = self.http.get("v1/blobs/data/latest", params=new_params)
        return self._load_object(response.content)

    def _search(self, name=None, at=None, custom_tags=None):
        if custom_tags is None:
            custom_tags = {}

        new_params = dict(self.base_params)

        if name:
            new_params["name"] = name
        if at:
            new_params["_at"] = at

        new_params = {**new_params, **custom_tags}

        response = self.http.get("v1/blobs", params=new_params)
        return self._create_blob_obj(response.json())

    def _load_object(self, data):
        try:
            return pickle.loads(data)
        except pickle.UnpicklingError as e:
            raise SerializeStorageException(e) from e

    def _create_blob_obj(self, json_obj):
        yield from [Blob(**item) for item in json_obj.get("items", [])]
        if json_obj.get("nextLink"):
            response = self.http.get(json_obj["nextLink"])
            yield from self._create_blob_obj(response.json())


def init_http(base_url):
    class TimeoutHTTPAdapter(HTTPAdapter):
        def __init__(self, *args, **kwargs):
            self.timeout = DEFAULT_TIMEOUT
            if "timeout" in kwargs:
                self.timeout = kwargs["timeout"]
                del kwargs["timeout"]
            super().__init__(*args, **kwargs)

        def send(self, request, **kwargs):
            timeout = kwargs.get("timeout")
            if timeout is None:
                kwargs["timeout"] = self.timeout
            try:
                return super().send(request, **kwargs)
            except requests.exceptions.ConnectionError as e:
                raise ConnectionStorageException(e)

    def assert_raise_for_status(response, *args, **kwargs):
        try:
            response.raise_for_status()
        except requests.exceptions.ConnectionError:
            raise ConnectionStorageException

    http = BaseUrlSession(base_url=base_url)
    http.hooks["response"] = [assert_raise_for_status]

    retries = Retry(
        total=3,
        backoff_factor=1,
        allowed_methods=["HEAD", "GET", "OPTIONS"],
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = TimeoutHTTPAdapter(max_retries=retries)
    http.mount("http://", adapter)

    return http
