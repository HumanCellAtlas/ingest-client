#!/usr/bin/env python
"""
desc goes here
"""
import json
import logging
import os
import time
from urllib.parse import urljoin, quote

import requests

from ingest.api.requests_utils import create_session_with_retry


class IngestApi:
    def __init__(self, url=None, token_manager=None):
        self.logger = logging.getLogger(__name__)
        self.session = create_session_with_retry()
        self.token_manager = token_manager

        if not url and 'INGEST_API' in os.environ:
            url = os.environ['INGEST_API']
            # expand interpolated env vars
            url = os.path.expandvars(url)
        self.url = url if url else "http://localhost:8080"
        self.headers = {'Content-type': 'application/json'}
        self.token = None
        self._submission_links = {}
        self.logger.info(f"using {self.url} for ingest API")

        self._ingest_links = self._get_ingest_links()

    def get(self, url, **kwargs):
        if 'headers' not in kwargs:
            kwargs['headers'] = {'Content-type': 'application/json'}
        return self.session.get(url, **kwargs)

    def patch(self, url, patch):
        r = self.session.patch(url, json=patch, headers=self.get_headers())
        r.raise_for_status()
        return r

    def put(self, url, data):
        r = self.session.put(url, json=data, headers=self.get_headers())
        return r

    def set_token(self, token):
        self.token = token
        self.headers['Authorization'] = self.token
        self.logger.debug(f'Token set!')

        return self.headers

    def get_headers(self):
        # refresh token
        if self.token_manager:
            self.set_token(f'Bearer {self.token_manager.get_token()}')
            self.logger.debug(f'Token refreshed!')

        return self.headers

    def _get_ingest_links(self):
        r = self.get(self.url, headers=self.get_headers())
        r.raise_for_status()
        return r.json()["_links"]

    def get_link_from_resource_url(self, resource_url, link_name):
        r = self.get(resource_url, headers=self.get_headers())
        r.raise_for_status()
        links = r.json().get('_links', {})
        return links.get(link_name, {}).get('href')

    def get_link_from_resource(self, resource, link_name):
        links = resource.get('_links', {})
        return links.get(link_name, {}).get('href')

    def get_schemas(self, latest_only=True, high_level_entity=None, domain_entity=None, concrete_entity=None):
        schema_url = self.get_schemas_url()
        all_schemas = []

        if latest_only:
            search_url = self.get_link_from_resource_url(schema_url, "search")
            r = self.get(search_url, headers=self.get_headers())
            if r.status_code == requests.codes.ok:
                response_j = json.loads(r.text)
                all_schemas = list(self.get_related_entities("latestSchemas", response_j, "schemas"))
        else:
            all_schemas = list(self.get_entities(schema_url, "schemas"))

        if high_level_entity:
            all_schemas = list(filter(lambda schema: schema.get('highLevelEntity') == high_level_entity, all_schemas))

        if domain_entity:
            all_schemas = list(filter(lambda schema: schema.get('domainEntity') == domain_entity, all_schemas))

        if concrete_entity:
            all_schemas = list(filter(lambda schema: schema.get('concreteEntity') == concrete_entity, all_schemas))

        return all_schemas

    def get_schemas_url(self):
        if "schemas" in self._ingest_links:
            return self._ingest_links["schemas"]["href"].rsplit("{")[0]
        return None

    def getSubmissions(self):
        params = {'sort': 'submissionDate,desc'}
        r = self.get(self._ingest_links["submissionEnvelopes"]["href"].rsplit("{")[0], params=params,
                     headers=self.get_headers())
        if r.status_code == requests.codes.ok:
            return json.loads(r.text)["_embedded"]["submissionEnvelopes"]

    def getProjects(self, id):
        submissionUrl = self.url + '/submissionEnvelopes/' + id + '/projects'
        r = self.get(submissionUrl, headers=self.get_headers())
        projects = []
        if r.status_code == requests.codes.ok:
            projects = json.loads(r.text)
        return projects

    def get_project_by_id(self, id):
        submission_url = self.url + '/projects/' + id
        r = self.get(submission_url, headers=self.get_headers())
        if r.status_code == requests.codes.ok:
            project = json.loads(r.text)
            return project
        else:
            raise ValueError("Project " + id + " could not be retrieved")

    def get_project_by_uuid(self, uuid):
        return self.get_entity_by_uuid('projects', uuid)

    def get_entity_by_uuid(self, entity_type, uuid):
        url = self.url + f'/{entity_type}/search/findByUuid?uuid=' + uuid

        # TODO make the endpoint consistent
        if entity_type == 'submissionEnvelopes':
            url = self.url + f'/{entity_type}/search/findByUuidUuid?uuid=' + uuid

        r = self.get(url, headers=self.get_headers())
        r.raise_for_status()
        return r.json()

    def get_entity_by_callback_link(self, callback_link):
        url = f'{self.url}{callback_link}'
        r = self.get(url, headers=self.get_headers())
        r.raise_for_status()
        return r.json()

    def get_file_by_submission_url_and_filename(self, submission_url, filename):
        search_url = self.get_link_from_resource_url(self.url + '/files/search',
                                                     'findBySubmissionEnvelopesInAndFileName')
        search_url = search_url.replace('{?submissionEnvelope,fileName}', '')
        r = self.get(search_url, params={'submissionEnvelope': submission_url, 'fileName': filename})
        if r.status_code == requests.codes.ok:
            return r.json()
        return None

    def get_submission(self, submission_url):
        r = self.get(submission_url, headers=self.get_headers())
        r.raise_for_status()
        return r.json()

    def get_submission_by_uuid(self, submission_uuid):
        search_link = self.get_link_from_resource_url(self.url + '/submissionEnvelopes/search', 'findByUuid')
        search_link = search_link.replace('{?uuid}', '')  # TODO: use a REST traverser instead of requests?
        r = self.get(search_link, params={'uuid': submission_uuid})
        r.raise_for_status()
        return r.json()

    def get_files(self, id):
        submission_url = self.url + '/submissionEnvelopes/' + id + '/files'
        r = self.get(submission_url, headers=self.get_headers())
        files = []
        if r.status_code == requests.codes.ok:
            files = json.loads(r.text)
        return files

    def get_bundle_manifests(self, id):
        submission_url = self.url + '/submissionEnvelopes/' + id
        return self.get_entities(submission_url, "bundleManifests", 500)

    def create_submission(self, update_submission=False):
        try:
            create_submission_url = self._ingest_links["submissionEnvelopes"]["href"].rsplit("{")[0]

            if update_submission:
                create_submission_url = f'{create_submission_url}/updateSubmissions'

            r = self.session.post(create_submission_url, data="{}",
                                  headers=self.get_headers())
            r.raise_for_status()
            submission = r.json()
            submission_url = submission["_links"]["self"]["href"].rsplit("{")[0]
            self._submission_links[submission_url] = submission["_links"]
            return submission
        except requests.exceptions.RequestException as err:
            self.logger.error("Request failed: " + str(err))
            raise

    def get_submission_links(self, submission_url):
        if not self._submission_links.get(submission_url):
            r = self.get(submission_url, headers=self.get_headers())
            r.raise_for_status()
            self._submission_links[submission_url] = r.json()["_links"]

        return self._submission_links.get(submission_url)

    def get_link_in_submission(self, submission_url, link_name):
        links = self.get_submission_links(submission_url)
        if link_name in links:
            link_obj = links.get(link_name)
            link = link_obj['href'].rsplit("{")[0]
            return link

        raise ValueError(f"{link_name} is not in submission resource links")

    def update_submission_state(self, submission_id, state):
        state_url = self.get_submission_state_url(submission_id, state)
        r = self.session.put(state_url, headers=self.get_headers())
        r.raise_for_status()
        return r.json()

    def get_submission_state_url(self, submission_id, state):
        submission_url = self.get_submission_url(submission_id)
        return self.get_link_in_submission(submission_url, state)

    def getSubmissionUri(self, submission_id):
        return self._ingest_links["submissionEnvelopes"]["href"].rsplit("{")[0] + "/" + submission_id

    def get_submission_url(self, submission_id):
        return self._ingest_links["submissionEnvelopes"]["href"].rsplit("{")[0] + "/" + submission_id

    def get_full_url(self, callback_link):
        return urljoin(self.url, callback_link)

    def get_process(self, process_url):
        r = self.get(process_url, headers=self.get_headers())
        r.raise_for_status()
        return r.json()

    def get_entities(self, submission_url, entity_type):
        r = self.get(submission_url, headers=self.get_headers())
        if r.status_code == requests.codes.ok:
            if entity_type in json.loads(r.text)["_links"]:
                yield from self.get_all(json.loads(r.text)["_links"][entity_type]["href"], entity_type)

    def get_all(self, url, entity_type):
        r = self.get(url, headers=self.get_headers())
        r.raise_for_status()
        result = r.json()

        entities = result["_embedded"][entity_type] if '_embedded' in result else []
        yield from entities

        while "next" in result["_links"]:
            next_url = result["_links"]["next"]["href"]
            r = self.get(next_url, headers=self.get_headers())
            r.raise_for_status()
            result = r.json()
            entities = result["_embedded"][entity_type]
            yield from entities
            self.logger.info(f"GET {entity_type} {json.dumps(result['page'])}")

    def get_related_entities(self, relation, entity, entity_type):
        # get the self link from entity
        if relation in entity["_links"]:
            entity_uri = entity["_links"][relation]["href"]
            for entity in self.get_all(entity_uri, entity_type):
                yield entity

    def get_related_entities_count(self, relation, entity, entity_type):
        if relation in entity["_links"]:
            entity_uri = entity["_links"][relation]["href"]
            r = self.get(entity_uri)
            r.raise_for_status()
            result = r.json()
            page = result.get('page')
            if page:
                return page.get('totalElements')
            return len(result["_embedded"][entity_type])

    def create_project(self, submission_url, content, uuid=None):
        return self.create_entity(submission_url, content, "projects", uuid)

    def create_biomaterial(self, submission_url, content, uuid=None):
        return self.create_entity(submission_url, content, "biomaterials", uuid)

    def create_process(self, submission_url, json_object, uuid=None):
        return self.create_entity(submission_url, json_object, "processes", uuid)

    def create_protocol(self, submission_url, content, uuid=None):
        return self.create_entity(submission_url, content, "protocols", uuid)

    def create_file(self, submission_url, filename, content, uuid=None):
        submission_files_url = self.get_link_in_submission(submission_url, 'files')

        submission_files_url = submission_files_url + "/" + quote(filename)

        file_to_create_object = {
            "fileName": filename,
            "content": content
        }

        params = {}
        if uuid:
            params["updatingUuid"] = uuid

        time.sleep(0.001)

        # Do not use session retries here as it will throw requests.exceptions.RetryError for http 409 error
        # We want to retry that error when creating files by doing a subsequent PATCH requests to update the metadata
        r = requests.post(submission_files_url, json=file_to_create_object,
                          headers=self.get_headers(), params=params)

        # TODO Investigate why core is returning internal server error
        if r.status_code == requests.codes.conflict or r.status_code == requests.codes.internal_server_error:
            search_files = self.get_file_by_submission_url_and_filename(submission_url, filename)

            if search_files and search_files.get('_embedded') and search_files['_embedded'].get('files'):
                file_in_ingest = search_files['_embedded'].get('files')[0]
                existing_content = file_in_ingest.get('content')
                new_content = existing_content

                if existing_content:
                    new_content.update(content)
                else:
                    new_content = content

                file_url = file_in_ingest['_links']['self']['href']
                time.sleep(0.001)
                r = self.session.patch(file_url, json={'content': new_content}, headers=self.get_headers())
                self.logger.debug(f'Updating existing content of file {file_url}.')

        r.raise_for_status()

        return r.json()

    def create_submission_manifest(self, submission_url, data):
        return self.create_entity(submission_url, data, 'submissionManifest')

    def create_submission_error(self, submission_url, data):
        return self.create_entity(submission_url, data, 'submissionErrors')

    def create_entity(self, submission_url, content, entity_type, uuid=None):
        params = {}
        if uuid:
            params["updatingUuid"] = uuid

        submission_url = self.get_link_in_submission(submission_url, entity_type)
        self.logger.debug(f"POST {submission_url} {json.dumps(content)}")

        r = self.session.post(submission_url, json=content, headers=self.get_headers(), params=params)
        r.raise_for_status()
        return r.json()

    def get_object_uuid(self, entity_uri):
        r = self.get(entity_uri, headers=self.get_headers())
        if r.status_code == requests.codes.ok:
            return json.loads(r.text)["uuid"]["uuid"]

    def link_entity(self, from_entity, to_entity, relationship):
        if not from_entity:
            raise ValueError("Error: from_entity is None")

        if not to_entity:
            raise ValueError("Error: to_entity is None")

        if not relationship:
            raise ValueError("Error: relationship is None")

        # check each dict in turn for non-None-ness

        from_entity_links = from_entity["_links"] if "_links" in from_entity else None
        if not from_entity_links:
            raise ValueError("Error: from_entity has no _links")

        from_entity_links_relationship = from_entity_links[relationship] if relationship in from_entity_links else None
        if not from_entity_links_relationship:
            raise ValueError("Error: from_entity_links has no {0} relationship".format(relationship))

        from_entity_links_relationship_href = from_entity_links_relationship[
            "href"] if "href" in from_entity_links_relationship else None
        if not from_entity_links_relationship_href:
            raise ValueError(
                "Error: from_entity_links_relationship for relationship {0} has no href".format(relationship))

        from_uri = from_entity["_links"][relationship]["href"]
        to_uri = self.get_link_from_resource(to_entity, 'self')

        self.logger.info('fromUri ' + from_uri + ' toUri:' + to_uri)

        headers = {
            'Content-type': 'text/uri-list',
            'Authorization': self.get_headers()['Authorization']
        }
        r = self.session.post(from_uri.rsplit("{")[0],
                              data=to_uri.rsplit("{")[0], headers=headers)
        r.raise_for_status()

        return r

    def create_bundle_manifest(self, bundleManifest):
        url = self._ingest_links["bundleManifests"]["href"].rsplit("{")[0]
        r = self.session.post(url, json=bundleManifest.__dict__, headers=self.get_headers())
        r.raise_for_status()
        return r.json()

    def update_staging_details(self, submission_url, uuid, staging_area_location):
        staging_details = {
            "stagingDetails": {
                "stagingAreaUuid": {
                    "uuid": uuid
                },
                "stagingAreaLocation": {
                    "value": staging_area_location
                }
            }
        }
        r = self.session.patch(submission_url, data=json.dumps(staging_details))
        r.raise_for_status()
