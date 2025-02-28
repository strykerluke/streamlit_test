import logging
from typing import List, Optional, Union
from xml.etree.ElementTree import Element

import orjson
from fitrequest.client import FitRequest
from fitrequest.method_generator import RequestMethod

logger = logging.getLogger(__name__)

BASE_URL = 'https://skillcorner.com'
BASE_CLIENT_NAME = 'skillcorner'

METHOD_DOCSTRING = (
    'Retrieve response from {endpoint} GET request. '
    'To learn more about it go to: https://skillcorner.com/api/docs/#{docs_url_anchor}.'
)

METHODS_BINDING = [
    {
        'name': 'get_competition_editions',
        'endpoint': '/api/competition_editions/',
        'docs_url_anchor': '/competition_editions_list',
    },
    {
        'name': 'get_competitions',
        'endpoint': '/api/competitions/',
        'docs_url_anchor': '/competitions/competitions_list',
    },
    {
        'name': 'get_competition_competition_editions',
        'endpoint': '/api/competitions/{}/editions/',
        'docs_url_anchor': '/competitions/competitions_editions_list',
        'resource_name': 'competition_id',
    },
    {
        'name': 'get_competition_rounds',
        'endpoint': '/api/competitions/{}/rounds/',
        'docs_url_anchor': '/competitions/competitions_rounds_list',
        'resource_name': 'competition_id',
    },
    {
        'name': 'get_in_possession_off_ball_runs',
        'endpoint': '/api/in_possession/off_ball_runs/',
        'docs_url_anchor': '/in_possession/in_possession_off_ball_runs_list',
    },
    {
        'name': 'get_in_possession_on_ball_pressures',
        'endpoint': '/api/in_possession/on_ball_pressures/',
        'docs_url_anchor': '/in_possession/in_possession_on_ball_pressures_list',
    },
    {
        'name': 'get_in_possession_passes',
        'endpoint': '/api/in_possession/passes/',
        'docs_url_anchor': '/in_possession/in_possession_passes_list',
    },
    {
        'name': 'get_matches',
        'endpoint': '/api/matches/',
        'docs_url_anchor': '/matches/matches_list',
    },
    {
        'name': 'get_match',
        'endpoint': '/api/match/{}/',
        'docs_url_anchor': '/match/match_read',
        'resource_name': 'match_id',
    },
    {
        'name': 'get_match_data_collection',
        'endpoint': '/api/match/{}/data_collection/',
        'docs_url_anchor': '/match/match_data_collection_read',
        'resource_name': 'match_id',
    },
    {
        'name': 'get_match_tracking_data',
        'endpoint': '/api/match/{}/tracking/',
        'docs_url_anchor': '/match/match_tracking_list',
        'resource_name': 'match_id',
        'exec_method': '_get_tracking_data',
    },
    {
        'name': 'get_physical',
        'endpoint': '/api/physical/',
        'docs_url_anchor': '/physical/physical_list',
    },
    {
        'name': 'get_players',
        'endpoint': '/api/players/',
        'docs_url_anchor': '/players/players_list',
    },
    {
        'name': 'get_player',
        'endpoint': '/api/players/{}/',
        'docs_url_anchor': '/players/players_read',
        'resource_name': 'player_id',
    },
    {
        'name': 'get_seasons',
        'endpoint': '/api/seasons/',
        'docs_url_anchor': '/seasons/seasons_list',
    },
    {
        'name': 'get_teams',
        'endpoint': '/api/teams/',
        'docs_url_anchor': '/teams/teams_list',
    },
    {
        'name': 'get_team',
        'endpoint': '/api/teams/{}/',
        'docs_url_anchor': '/teams/teams_read',
        'resource_name': 'team_id',
    },
]


class SkillcornerClient(FitRequest):
    base_url = BASE_URL
    base_client_name = BASE_CLIENT_NAME
    _docstring_template = METHOD_DOCSTRING
    _methods_binding = METHODS_BINDING

    def _is_response_complete(self, response: Union[bytes, dict, str, Element]) -> bool:
        return not isinstance(response, dict) or 'next' not in response

    def _paginate_and_return(
        self, method: RequestMethod, response: dict, default_raise_for_status: bool = True, **kwargs
    ) -> dict:
        results = []
        while True:
            results.extend(response['results'])
            if not response['next']:
                break
            response = super()._request(
                method=method,
                endpoint=response['next'].split(self.base_url)[1],
                params=None,
                default_raise_for_status=default_raise_for_status,
                **kwargs,
            )
        return results

    def _request(
        self,
        method: RequestMethod,
        endpoint: str,
        params: Optional[dict] = None,
        default_raise_for_status: bool = True,
        **kwargs,
    ) -> Union[bytes, dict, str, Element]:
        response = super()._request(
            method=method, endpoint=endpoint, params=params, default_raise_for_status=default_raise_for_status, **kwargs
        )
        if self._is_response_complete(response):
            return response
        return self._paginate_and_return(method, response, default_raise_for_status, **kwargs)

    def _get_tracking_data(
        self,
        method: RequestMethod,
        endpoint: str,
        resource_id: str,
        params: Optional[dict] = None,
        **kwargs,
    ) -> Union[List[dict], str, bytes]:
        valid_formats = ['jsonl', 'fifa-data', 'fifa-xml']
        if params and params.get('file_format', 'jsonl') not in valid_formats:
            raise ValueError(f'Unknown file format `{params["file_format"]}`, valid formats are: {valid_formats}')
        data = self._request(method=method, endpoint=endpoint, resource_id=resource_id, params=params, **kwargs)
        if not params or params.get('file_format', 'jsonl') == 'jsonl':
            return [orjson.loads(line) for line in data.splitlines()]
        elif params.get('file_format') in ['fifa-data', 'fifa-xml']:
            return data

    def _get_tracking_data_and_save(
        self,
        method: RequestMethod,
        endpoint: str,
        filepath: str,
        resource_id: str,
        params: Optional[dict] = None,
        **kwargs,
    ) -> None:
        data = self._get_tracking_data(method=method, endpoint=endpoint, resource_id=resource_id, params=params)
        self._save_data(filepath, data)
