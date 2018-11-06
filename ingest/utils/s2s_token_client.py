from .dcp_auth_client import DCPAuthClient
import os
import json


class S2STokenClient:
    def __init__(self, dcp_auth_client: 'DCPAuthClient'=None):
        self.dcp_auth_client = dcp_auth_client

    def setup_from_env_var(self, env_var_name, google_project):
        key_dict = json.loads(os.environ.get(env_var_name))
        self.dcp_auth_client = DCPAuthClient(google_project, key_dict)

    def retrieve_token(self) -> str:
        return self.dcp_auth_client.token


