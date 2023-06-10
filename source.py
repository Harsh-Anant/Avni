import re
from abc import ABC
from datetime import datetime
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from urllib.parse import parse_qs

import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import IncrementalMixin, Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from flatten_json import flatten


class Avni(HttpStream):
    url_base = "https://app.avniproject.org/"

    # Set this as a noop.
    primary_key = None

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()
        self.lastmodifieddatetime = config['lastmodifieddatetime']
        self.api_key = config['api_key']
        if config['subjecttype'] is not None:
            self.subjecttype = config['subjecttype']
        else:
            self.subjecttype = None
        if config['concepts'] is not None:
            self.concepts = congfig['concepts']
        else:
            self.concepts = None
        if config['locationids'] is not None::
            self.locationids =config['locationids']
        else:
            self.locationids = None


    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        # The API does not offer pagination, so we return None to indicate there are no more pages in the response
        return None

    def path(
        self, 
        stream_state: Mapping[str, Any] = None, 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "api/subjects"

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        # The api requires that we include apikey as a header so we do that in this method
        return {'auth-token': self.api_key}

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        return None  # TODO

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        # The api requires that we include the base currency as a query param so we do that in this method
        return {'lastModifiedDateTime': self.lastmodifieddatetime, 'concepts':self.concepts, 'subjectType': self.subjecttype,'locationids':self.locationids}
    
    def parse_response(
            self,
            response: requests.Response,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        # The response is a simple JSON whose schema matches our stream's schema exactly, 
        # so we just return a list containing the response
        return [response.json()]

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        # The API does not offer pagination, 
        # so we return None to indicate there are no more pages in the response
        return None

class SourceAvniConnector(AbstractSource):

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            authenticator = TokenAuthenticator(config["api_key"], "ApiKey")
            stream = Avni(authenticator=authenticator, config=config)
            return True, "200 OK"
        except requests.exceptions.RequestException as e:
            return False, "Connection Failed."

    def streams(self, config: Mapping[str, Any]) -> List[Stream]: 
        auth = TokenAuthenticator(config["api_key"], auth_method="ApiKey")
        return [Avni(authenticator=auth,config=config)]
