#!/usr/bin/env python
"""
desc goes here
"""
import time
from requests import HTTPError

__author__ = "jupp"
__license__ = "Apache 2.0"

import json, os, requests, logging, uuid


class IngestApi:
    def __init__(self, url=None):
        formatter = logging.Formatter(
            '[%(filename)s:%(lineno)s - %(funcName)20s() ] %(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logging.basicConfig(formatter=formatter)
        logging.getLogger("requests").setLevel(logging.WARNING)
        self.logger = logging.getLogger(__name__)

        if not url and 'INGEST_API' in os.environ:
            url = os.environ['INGEST_API']
            # expand interpolated env vars
            url = os.path.expandvars(url)
            self.logger.info("using " + url + " for ingest API")
        self.url = url if url else "http://localhost:8080"

        self.ingest_api = None
        self.headers = {'Content-type': 'application/json'}

        self.submission_links = {}
        self.load_root()

    def load_root(self):
        if not self.ingest_api:
            reply = requests.get(self.url, headers=self.headers)
            self.ingest_api = reply.json()["_links"]

    def getSubmissions(self):
        params = {'sort': 'submissionDate,desc'}
        r = requests.get(self.ingest_api["submissionEnvelopes"]["href"].rsplit("{")[0], params=params,
                         headers=self.headers)
        if r.status_code == requests.codes.ok:
            return json.loads(r.text)["_embedded"]["submissionEnvelopes"]

    def getSubmissionIfModifiedSince(self, submissionId, datetimeUTC):
        submissionUrl = self.getSubmissionUri(submissionId)
        headers = self.headers

        if datetimeUTC:
            headers = {'If-Modified-Since': datetimeUTC}

        self.logger.info('headers:' + str(headers))
        r = requests.get(submissionUrl, headers=headers)

        if r.status_code == requests.codes.ok:
            submission = json.loads(r.text)
            return submission
        else:
            self.logger.error(str(r))

    def getProjects(self, id):
        submissionUrl = self.url + '/submissionEnvelopes/' + id + '/projects'
        r = requests.get(submissionUrl, headers=self.headers)
        projects = []
        if r.status_code == requests.codes.ok:
            projects = json.loads(r.text)
        return projects

    def getProjectById(self, id):
        submissionUrl = self.url + '/projects/' + id
        r = requests.get(submissionUrl, headers=self.headers)
        if r.status_code == requests.codes.ok:
            project = json.loads(r.text)
            return project
        else:
            raise ValueError("Project " + id + " could not be retrieved")

    def getProjectByUuid(self, uuid):
        url =  self.url + '/projects/search/findByUuid?uuid=' + uuid
        r = requests.get(url, headers=self.headers)
        r.raise_for_status()
        return r.json()

    def getSubmissionEnvelope(self, submissionUrl):
        r = requests.get(submissionUrl, headers=self.headers)
        if r.status_code == requests.codes.ok:
            submissionEnvelope = json.loads(r.text)
            return submissionEnvelope
        else:
            raise ValueError("Submission Envelope " + submissionUrl + " could not be retrieved")

    def getFiles(self, id):
        submissionUrl = self.url + '/submissionEnvelopes/' + id + '/files'
        r = requests.get(submissionUrl, headers=self.headers)
        files = []
        if r.status_code == requests.codes.ok:
            files = json.loads(r.text)
        return files

    def getBundleManifests(self, id):
        submissionUrl = self.url + '/submissionEnvelopes/' + id + '/bundleManifests'
        r = requests.get(submissionUrl, headers=self.headers)
        bundleManifests = []

        if r.status_code == requests.codes.ok:
            bundleManifests = json.loads(r.text)
        return bundleManifests

    def createSubmission(self, token):
        auth_headers = {'Content-type': 'application/json',
                        'Authorization': token
                        }
        try:
            r = requests.post(self.ingest_api["submissionEnvelopes"]["href"].rsplit("{")[0], data="{}",
                              headers=auth_headers)
            r.raise_for_status()
            submissionUrl = json.loads(r.text)["_links"]["self"]["href"].rsplit("{")[0]
            self.submission_links[submissionUrl] = json.loads(r.text)["_links"]
            return submissionUrl
        except requests.exceptions.RequestException as err:
            self.logger.error("Request failed: ", err)
            raise

    def finishSubmission(self, submissionUrl):
        r = requests.put(submissionUrl, headers=self.headers)
        if r.status_code == requests.codes.update:
            self.logger.info("Submission complete!")
            return r.text

    def updateSubmissionState(self, submissionId, state):
        state_url = self.getSubmissionStateUrl(submissionId, state)

        if state_url:
            r = requests.put(state_url, headers=self.headers)

        return self.handleResponse(r)

    def getSubmissionStateUrl(self, submissionId, state):
        submissionUrl = self.getSubmissionUri(submissionId)
        response = requests.get(submissionUrl, headers=self.headers)
        submission = self.handleResponse(response)

        if submission and state in submission['_links']:
            return submission['_links'][state]["href"].rsplit("{")[0]

        return None

    def handleResponse(self, response):
        if response.ok:
            return json.loads(response.text)
        else:
            self.logger.error('Response:' + response.text)
            return None

    def getSubmissionUri(self, submissionId):
        return self.ingest_api["submissionEnvelopes"]["href"].rsplit("{")[0] + "/" + submissionId

    def getAssayUrl(self, assayCallbackLink):
        # TODO check if callback link already has a leading slash
        return self.url + "/" + assayCallbackLink

    def getAssay(self, assayUrl):
        r = requests.get(assayUrl, headers=self.headers)
        if r.status_code == requests.codes.ok:
            return r.json()


    def getAnalyses(self, submissionUrl):
        return self.getEntities(submissionUrl, "analyses")

    def getEntities(self, submissionUrl, entityType):
        r = requests.get(submissionUrl, headers=self.headers)
        if r.status_code == requests.codes.ok:
            if entityType in json.loads(r.text)["_links"]:
                # r2 = requests.get(, headers=self.headers)
                for entity in self._getAllObjectsFromSet(json.loads(r.text)["_links"][entityType]["href"], entityType):
                    yield entity

    def _getAllObjectsFromSet(self, url, entityType, pageSize=None):
        params = dict()
        if pageSize:
            params = {"size": pageSize}

        r = requests.get(url, headers=self.headers, params=params)
        if r.status_code == requests.codes.ok:
            if "_embedded" in json.loads(r.text):
                for entity in json.loads(r.text)["_embedded"][entityType]:
                    yield entity
                if "next" in json.loads(r.text)["_links"]:
                    for entity2 in self._getAllObjectsFromSet(json.loads(r.text)["_links"]["next"]["href"], entityType):
                        yield entity2

    def getRelatedEntities(self, relation, entity, entityType):
        # get the self link from entity
        if relation in entity["_links"]:
            entityUri = entity["_links"][relation]["href"]
            for entity in self._getAllObjectsFromSet(entityUri, entityType):
                yield entity

    def _updateStatusToPending(self, submissionUrl):
        r = requests.patch(submissionUrl, data="{\"submissionStatus\" : \"Pending\"}", headers=self.headers)

    def createProject(self, submissionUrl, jsonObject, token):
        return self.createEntity(submissionUrl, jsonObject, "projects", token)

    def createBiomaterial(self, submissionUrl, jsonObject):
        return self.createEntity(submissionUrl, jsonObject, "biomaterials")

    def createProcess(self, submissionUrl, jsonObject):
        return self.createEntity(submissionUrl, jsonObject, "processes")

    # def createDonor(self, submissionUrl, jsonObject):
    #     return self.createBiomaterial(submissionUrl, jsonObject)

    def createProtocol(self, submissionUrl, jsonObject):
        return self.createEntity(submissionUrl, jsonObject, "protocols")

    # def createAnalysis(self, submissionUrl, jsonObject):
    #     return self.createEntity(submissionUrl, jsonObject, "analyses")

    def createFile(self, submissionUrl, fileName, jsonObject):
        submissionUrl = self.submission_links[submissionUrl]["files"]['href'].rsplit("{")[0]
        self.logger.debug("posting " + submissionUrl)
        fileToCreateObject = {
            "fileName": fileName,
            "content": json.loads(jsonObject)
        }
        r = requests.post(submissionUrl, data=json.dumps(fileToCreateObject),
                          headers=self.headers)
        if r.status_code == requests.codes.created or r.status_code == requests.codes.accepted:
            return json.loads(r.text)
        raise ValueError('Create file failed: File ' + fileName + " - " + r.text)

    def createEntity(self, submissionUrl, jsonObject, entityType, token=None):
        self.logger.debug(".", )
        auth_headers = {'Content-type': 'application/json',
                        'Authorization': token
                        }
        submissionUrl = self.submission_links[submissionUrl][entityType]['href'].rsplit("{")[0]

        self.logger.debug("posting " + submissionUrl)
        r = requests.post(submissionUrl, data=jsonObject,
                          headers=auth_headers)
        if r.status_code == requests.codes.created or r.status_code == requests.codes.accepted:
            return json.loads(r.text)

    # given a HCA object return the URI for the object from ingest
    def getObjectId(self, entity):
        if "_links" in entity:
            entityUrl = entity["_links"]["self"]["href"].rsplit("{")[0]
            return entityUrl
        raise ValueError('Can\'t get id for ' + json.dumps(entity) + ' is it a HCA entity?')

    def getObjectUuid(self, entityUri):
        r = requests.get(entityUri,
                         headers=self.headers)
        if r.status_code == requests.codes.ok:
            return json.loads(r.text)["uuid"]["uuid"]

    def linkEntity(self, fromEntity, toEntity, relationship):
        if not fromEntity:
            raise ValueError("Error: fromEntity is None")

        if not toEntity:
            raise ValueError("Error: toEntity is None")

        if not relationship:
            raise ValueError("Error: relationship is None")

        # check each dict in turn for non-None-ness

        fromEntityLinks = fromEntity["_links"] if "_links" in fromEntity else None
        if not fromEntityLinks:
            raise ValueError("Error: fromEntity has no _links")

        fromEntityLinksRelationship = fromEntityLinks[relationship] if relationship in fromEntityLinks else None
        if not fromEntityLinksRelationship:
            raise ValueError("Error: fromEntityLinks has no {0} relationship".format(relationship))

        fromEntityLinksRelationshipHref = fromEntityLinksRelationship["href"] if "href" in fromEntityLinksRelationship else None
        if not fromEntityLinksRelationshipHref:
            raise ValueError("Error: fromEntityLinksRelationship for relationship {0} has no href".format(relationship))

        fromUri = fromEntity["_links"][relationship]["href"]
        toUri = self.getObjectId(toEntity)

        self._retry_when_http_error(0, self._post_link_entity, fromUri, toUri)

    def _post_link_entity(self, fromUri, toUri):
        self.logger.debug('fromUri ' + fromUri + ' toUri:' + toUri);

        headers = {'Content-type': 'text/uri-list'}

        r = requests.post(fromUri.rsplit("{")[0],
                          data=toUri.rsplit("{")[0], headers=headers)

        return r

    def _retry_when_http_error(self, tries, func, *args):
        max_retries = 5

        if tries < max_retries:
            if tries > 1:
                self.logger.info("no of tries: " + str(tries + 1))

            r = None
            
            try:
                r = func(*args)
                r.raise_for_status()

            except HTTPError:
                self.logger.error("\nResponse was: " + str(r.status_code) + " (" + r.text + ")")
                tries += 1
                time.sleep(1)
                r = self._retry_when_http_error(tries, func, *args)

            except requests.ConnectionError as e:
                self.logger.exception(str(e))
                tries += 1
                time.sleep(1)
                r = self._retry_when_http_error(tries, func, *args)

            except Exception as e:
                self.logger.exception(str(e))
                tries += 1
                time.sleep(1)
                r = self._retry_when_http_error(tries, func, *args)

            return r
        else:
            error_message = "Maximum no of tries reached: " + str(max_retries)
            self.logger.error(error_message)
            return None

    def _request_post(self, url, data, params, headers):
        if params:
            return requests.post(url, data=data, params=params, headers=headers)

        return requests.post(url, data=data, headers=headers)

    def _request_put(self, url, data, params, headers):
        if params:
            return requests.put(url, data=data, params=params, headers=headers)

        return requests.put(url, data=data, headers=headers)

    def createBundleManifest(self, bundleManifest):
        r = self._retry_when_http_error(0, self._post_bundle_manifest, bundleManifest, self.ingest_api["bundleManifests"]["href"].rsplit("{")[0])

        if not (200 <= r.status_code < 300):
            error_message = "Failed to create bundle manifest at URL {0} with request payload: {1}".format(self.ingest_api["bundleManifests"]["href"].rsplit("{")[0],
                                                                                                           json.dumps(bundleManifest.__dict__))
            self.logger.error(error_message)
            raise ValueError(error_message)
        else:
            self.logger.info("successfully created bundle manifest")

    def _post_bundle_manifest(self, bundleManifest, url):
        return requests.post(url, data=json.dumps(bundleManifest.__dict__), headers=self.headers)

    def updateSubmissionWithStagingCredentials(self, subUrl, uuid, submissionCredentials):
        stagingDetails = \
            {
                "stagingDetails": {
                    "stagingAreaUuid": {
                        "uuid": uuid
                    },
                    "stagingAreaLocation": {
                        "value": submissionCredentials
                    }
                }
            }

        if self.retrySubmissionUpdateWithStagingDetails(subUrl, stagingDetails, 0):
            self.logger.debug("envelope updated with staging details " + json.dumps(stagingDetails))
        else:
            self.logger.error("Failed to update envelope with staging details: " + json.dumps(stagingDetails))

    def retrySubmissionUpdateWithStagingDetails(self, subUrl, stagingDetails, tries):
        if tries < 5:
            # do a GET request to get latest submission envelope
            entity_response = requests.get(subUrl)
            etag = entity_response.headers['ETag']
            if etag:
                # set the etag header so we get 412 if someone beats us to set validating
                self.headers['If-Match'] = etag
                r = requests.patch(subUrl, data=json.dumps(stagingDetails))
                try:
                    r.raise_for_status()
                    return True
                except HTTPError:
                    self.logger.error("PATCHing submission envelope with creds failed, retrying")
                    tries += 1
                    self.retrySubmissionUpdateWithStagingDetails(subUrl, stagingDetails, tries)
        else:
            return False


class BundleManifest:
    def __init__(self):
        self.bundleUuid = str(uuid.uuid4())
        self.envelopeUuid = {}
        self.dataFiles = []
        self.fileBiomaterialMap = {}
        self.fileProcessMap = {}
        self.fileFilesMap = {}
        self.fileProjectMap = {}
        self.fileProtocolMap = {}
