#!/usr/bin/env python
"""
desc goes here
"""
from requests import HTTPError

__author__ = "jupp"
__license__ = "Apache 2.0"

import json, os, urllib, requests, logging, uuid


class IngestApi:
    def __init__(self, url=None):
        formatter = logging.Formatter(' %(asctime)s  - %(name)s - %(levelname)s in %(filename)s:%(lineno)s %(funcName)s(): %(message)s')
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
            reply = urllib.urlopen(self.url)
            self.ingest_api = json.load(reply)["_links"]

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

    def getAssays(self, submissionUrl):
        return self.getEntities(submissionUrl, "assays")

    def getAnalyses(self, submissionUrl):
        return self.getEntities(submissionUrl, "analyses")

    def getEntities(self, submissionUrl, entityType):
        r = requests.get(submissionUrl, headers=self.headers)
        if r.status_code == requests.codes.ok:
            if entityType in json.loads(r.text)["_links"]:
                # r2 = requests.get(, headers=self.headers)
                for entity in self._getAllObjectsFromSet(json.loads(r.text)["_links"][entityType]["href"], entityType):
                    yield entity

    def _getAllObjectsFromSet(self, url, entityType):
        r = requests.get(url, headers=self.headers)
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

    def createProject(self, submissionUrl, jsonObject):
        return self.createEntity(submissionUrl, jsonObject, "projects")

    def createSample(self, submissionUrl, jsonObject):
        return self.createEntity(submissionUrl, jsonObject, "samples")

    def createAssay(self, submissionUrl, jsonObject):
        return self.createEntity(submissionUrl, jsonObject, "assays")

    def createDonor(self, submissionUrl, jsonObject):
        return self.createSample(submissionUrl, jsonObject)

    def createProtocol(self, submissionUrl, jsonObject):
        return self.createEntity(submissionUrl, jsonObject, "protocols")

    def createAnalysis(self, submissionUrl, jsonObject):
        return self.createEntity(submissionUrl, jsonObject, "analyses")

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

    def createEntity(self, submissionUrl, jsonObject, entityType):
        self.logger.debug(".", )
        submissionUrl = self.submission_links[submissionUrl][entityType]['href'].rsplit("{")[0]

        self.logger.debug("posting " + submissionUrl)
        r = requests.post(submissionUrl, data=jsonObject,
                          headers=self.headers)
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

        fromUri = fromEntity["_links"][relationship]["href"]
        toUri = self.getObjectId(toEntity)
        headers = {'Content-type': 'text/uri-list'}
        r = requests.post(fromUri.rsplit("{")[0],
                          data=toUri.rsplit("{")[0], headers=headers)
        if r.status_code != requests.codes.no_content:
            raise ValueError("Error creating relationship between entity: " + fromUri + " -> " + toUri)
        self.logger.debug("Asserted relationship between " + fromUri + " -> " + toUri)

    def createBundleManifest(self, bundleManifest):
        r = requests.post(self.ingest_api["bundleManifests"]["href"].rsplit("{")[0],
                          data=json.dumps(bundleManifest.__dict__),
                          headers=self.headers)

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
        self.bundleUuid = unicode(uuid.uuid4())
        self.envelopeUuid = {}
        self.files = []
        self.fileSampleMap = {}
        self.fileAssayMap = {}
        self.fileProjectMap = {}
        self.fileProtocolMap = {}
