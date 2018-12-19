from .dcp_auth_client import DCPAuthClient
import os
import json


class S2STokenClient:
    def __init__(self, dcp_auth_client: 'DCPAuthClient'=None):
        self.dcp_auth_client = dcp_auth_client

    def setup_from_env_var(self, env_var_name):
        key_dict = json.loads(os.environ.get(env_var_name))
        self.dcp_auth_client = DCPAuthClient(key_dict["project_id"], key_dict)

    def setup_from_file(self, file_path):
        with open(file_path) as fh:
            service_credentials = json.load(fh)
        self.dcp_auth_client = DCPAuthClient(service_credentials["project_id"], service_credentials)

    def retrieve_token(self) -> str:
        return self.dcp_auth_client.token
