import difflib
import logging
import os
from importlib.metadata import PackageNotFoundError, version
from typing import Any, List, Optional, Union
from urllib.parse import urlparse
from xml.etree.ElementTree import Element

import orjson
from defusedxml.ElementTree import fromstring
from requests import Response, Session
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from requests.packages.urllib3.util.retry import Retry

from fitrequest.method_generator import RequestMethod, _MethodsGenerator

logger = logging.getLogger(__name__)


class FitRequest(metaclass=_MethodsGenerator):
    _default_log_level: int = logging.INFO
    _docstring_template: Optional[str] = None
    _methods_binding: Optional[List[dict]] = None
    _retry: Optional[Retry] = None
    _session: Optional[Session] = None
    base_client_name: str
    base_url: Optional[str] = None

    def __init__(self, username=None, password=None, *args, **kwargs):
        self.base_url = self.base_url or os.environ['CLIENT_BASE_URL']
        logger.setLevel(self._default_log_level)
        if not all((username, password)):
            username = os.environ.get(f'{self.base_client_name.upper()}_USERNAME', '')
            password = os.environ.get(f'{self.base_client_name.upper()}_PASSWORD', '')
        self._authenticate(username=username, password=password)

    def __getattr__(self, name: str):
        closest_match = difflib.get_close_matches(
            name, (method for method in dir(self) if callable(getattr(self, method))), n=1
        )
        message = f"'{type(self).__name__}' object has no attribute/method '{name}'."
        if closest_match:
            message += f" Did you mean '{closest_match[0]}'?"
        raise AttributeError(message)

    def _authenticate(self, username: str, password: str) -> None:
        self._auth = HTTPBasicAuth(username, password)

    @property
    def session(self) -> Session:
        if not self._session:
            self._session = Session()
            try:
                package_version = version(self.base_client_name)
            except PackageNotFoundError:
                logger.warning(
                    f'Cannot retrieve package version, either your package is not named {self.base_client_name} '
                    '(as your base_client_name attribute), or it is not installed.'
                )
                package_version = '{version}'
            self._session.headers = {'User-Agent': f'fitrequest.{self.base_client_name}.{package_version}'}
            if self._retry:
                adapter = HTTPAdapter(max_retries=self._retry)
                self._session.mount('http://', adapter)
                self._session.mount('https://', adapter)
            self._session.auth = self._auth
        return self._session

    def _build_final_url(self, endpoint: str) -> str:
        url = f'{self.base_url}/{endpoint.lstrip("/")}'
        return url if self._is_url_valid(url) else ValueError(f'Invalid URL: {url}')

    def _handle_response(self, response: Response, raise_for_status: bool = True) -> Union[bytes, dict, str, Element]:
        if raise_for_status:
            response.raise_for_status()

        content_type = response.headers.get('Content-Type', '')
        if content_type.endswith('json'):
            return response.json()
        elif content_type.endswith(('plain', 'html')):
            return response.text
        elif content_type.endswith('xml'):
            return fromstring(response.content)
        else:
            return response.content

    @staticmethod
    def _is_url_valid(url: Optional[str]) -> bool:
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except (AttributeError, ValueError):
            return False

    def _request(
        self,
        method: RequestMethod,
        endpoint: str,
        data: Optional[Any] = None,
        params: Optional[dict] = None,
        raise_for_status: bool = True,
        resource_id: Optional[str] = None,
        response_key: Optional[str] = None,
        timeout: Optional[int] = None,
        **kwargs,
    ) -> Union[bytes, dict, str, Element]:
        url = self._build_final_url(endpoint.format(resource_id))
        logger.info(f'Sending {method} request to: {url}')
        response = self._handle_response(
            self.session.request(
                method=method,
                url=url,
                data=data,
                params=self._transform_params(params) if params else None,
                timeout=timeout,
            ),
            raise_for_status=raise_for_status,
        )

        if kwargs.get('response_log_level'):
            logger.log(
                kwargs.get('response_log_level'),
                f'Response from {self.base_client_name}',
                extra={'url': url, 'client': self.base_client_name, 'response': response},
            )

        return response[response_key] if isinstance(response, dict) and response_key else response

    def _request_and_save(
        self,
        method: RequestMethod,
        endpoint: str,
        filepath: str,
        data: Optional[Any] = None,
        params: Optional[dict] = None,
        raise_for_status: bool = True,
        resource_id: Optional[str] = None,
        response_key: Optional[str] = None,
        timeout: Optional[int] = None,
        **kwargs,
    ) -> None:
        data = self._request(
            method=method,
            endpoint=endpoint,
            data=data,
            params=params,
            raise_for_status=raise_for_status,
            resource_id=resource_id,
            response_key=response_key,
            timeout=timeout,
            **kwargs,
        )
        self._save_data(filepath=filepath, data=data)

    def _save_data(self, filepath: str, data: Union[List[dict], str, bytes], mode: str = 'xb'):
        logger.info(f'Saving data to file: {filepath}')
        with open(filepath, mode) as file:
            if isinstance(data, bytes):
                file.write(data)
            else:
                file.write(orjson.dumps(data, option=orjson.OPT_INDENT_2))

    @staticmethod
    def _transform_params(params: dict) -> dict:
        return {k: (','.join([str(x) for x in v]) if isinstance(v, list) else v) for k, v in params.items()}
