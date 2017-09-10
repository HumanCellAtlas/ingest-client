#!/usr/bin/env python
"""
desc goes here 
"""
__author__ = "jupp"
__license__ = "Apache 2.0"

import glob, json, os, urllib, requests, logging

class IngestApi:
    def __init__(self, url=None):

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logging.basicConfig(formatter=formatter)
        logging.getLogger("requests").setLevel(logging.WARNING)
        self.logger = logging.getLogger(__name__)

        if not url and 'INGEST_API' in os.environ:
            url = os.environ['INGEST_API']
            # expand interpolated env vars
            url = os.path.expandvars(url)
            self.logger.info("using " +url+ " for ingest API")
        self.url = url if url else "http://localhost:8080"

        self.ingest_api = None
        self.headers = {'Content-type': 'application/json'}

        self.submission_links = {}
        self.load_root()

    def load_root (self):
        if not self.ingest_api:
            reply = urllib.urlopen(self.url)
            self.ingest_api = json.load(reply)["_links"]

    def getSubmissions(self):
        params = {'sort':'submissionDate,desc'}
        r = requests.get(self.ingest_api["submissionEnvelopes"]["href"].rsplit("{")[0], params=params,
                          headers=self.headers)
        if r.status_code == requests.codes.ok:
            return json.loads(r.text)["_embedded"]["submissionEnvelopes"]

    def createSubmission(self):
        r = requests.post(self.ingest_api["submissionEnvelopes"]["href"].rsplit("{")[0], data="{}",
                          headers=self.headers)

        if r.status_code == requests.codes.created:
            submissionUrl = json.loads(r.text)["_links"]["self"]["href"].rsplit("{")[0]
            self.submission_links[submissionUrl] = json.loads(r.text)["_links"]
            return submissionUrl
        else:
            self.logger.error("Error getting submission envelope:" + json.loads(r.text)["message"])
            exit(1)

    def finishSubmission(self, submissionUrl):
        r = requests.put(submissionUrl, headers=self.headers)
        if r.status_code == requests.codes.update:
            self.logger.info("Submission complete!")
            return r.text

    def finishedForNow(self, submissionUrl):
        self._updateStatusToPending(submissionUrl)

    def getSubmissionUri(self, submissionId):
        return self.ingest_api["submissionEnvelopes"]["href"].rsplit("{")[0]+ "/"+submissionId

    def getAssays(self, submissionUrl):
        return self.getEntities(submissionUrl, "assays")

    def getEntities(self, submissionUrl, entityType):
        r = requests.get(submissionUrl, headers=self.headers)
        if r.status_code == requests.codes.ok:
            if entityType in json.loads(r.text)["_links"]:
                # r2 = requests.get(, headers=self.headers)
                for entity in self._getAllObjectsFromSet(json.loads(r.text)["_links"][entityType]["href"], entityType):
                    yield entity
            else:
                yield []

    def _getAllObjectsFromSet(self, url, entityType):
        r = requests.get(url, headers=self.headers)
        if r.status_code == requests.codes.ok:
            for entity in json.loads(r.text)["_embedded"][entityType]:
                yield entity
            if "next" in json.loads(r.text)["_links"]:
                yield self._getAllObjectsFromSet(json.loads(r.text)["_links"]["next"]["href"], entityType)

    def getRelatedEntities(self, relation, entity, entityType):
        # get the self link from entity
        if relation in entity["_links"]:
            entityUri = entity["_links"][relation]["href"]
            for entity in self._getAllObjectsFromSet(entityUri, entityType):
                yield entity



    def _updateStatusToPending(self, submissionUrl):
        r = requests.patch(submissionUrl, data="{\"submissionStatus\" : \"Pending\"}", headers=self.headers)

    def createProject(self, submissionUrl, jsonObject):
        return self.createEntity(submissionUrl,jsonObject, "projects")

    def createSample(self, submissionUrl, jsonObject):
        return self.createEntity(submissionUrl,jsonObject, "samples")

    def createAssay(self, submissionUrl, jsonObject):
        return self.createEntity(submissionUrl,jsonObject, "assays")

    def createDonor(self,submissionUrl,  jsonObject):
        return self.createSample(submissionUrl,jsonObject)

    def createProtocol(self,submissionUrl,  jsonObject):
        return self.createEntity(submissionUrl,jsonObject, "protocols")

    def createAnalysis(self, submissionUrl, jsonObject):
        return self.createEntity(submissionUrl,jsonObject, "analyses")

    def createFile(self, submissionUrl, fileName, jsonObject):
        submissionUrl = self.submission_links[submissionUrl]["files"]['href'].rsplit("{")[0]
        self.logger.debug("posting " + submissionUrl)
        fileToCreateObject = {
            "fileName" : fileName,
            "content" : json.loads(jsonObject)
        }
        r = requests.post(submissionUrl, data=json.dumps(fileToCreateObject),
                          headers=self.headers)
        if r.status_code == requests.codes.created or r.status_code == requests.codes.accepted:
            return json.loads(r.text)
        raise ValueError('Create file failed: File ' + fileName + " - "+ r.text)

    def createEntity(self, submissionUrl, jsonObject, entityType):
        self.logger.debug(".",)
        submissionUrl = self.submission_links[submissionUrl][entityType]['href'].rsplit("{")[0]

        self.logger.debug("posting "+submissionUrl)
        r = requests.post(submissionUrl, data=jsonObject,
                          headers=self.headers)
        if r.status_code == requests.codes.created or r.status_code == requests.codes.accepted:
            return json.loads(r.text)
        raise ValueError('Create entity failed: Entity ' + entityType + " " + json.dumps(jsonObject))


    # given a HCA object return the URI for the object from ingest
    def getObjectId(self, entity):
        if "_links" in entity:
            entityUrl = entity["_links"]["self"]["href"].rsplit("{")[0]
            return entityUrl
        raise ValueError('Can\'t get id for '+json.dumps(entity) +' is it a HCA entity?')

    def linkEntity(self,fromEntity, toEntity, relationship):

        fromUri = fromEntity["_links"][relationship]["href"]
        toUri = self.getObjectId(toEntity)
        headers = {'Content-type': 'text/uri-list'}
        r = requests.post(fromUri.rsplit("{")[0],
                          data=toUri.rsplit("{")[0], headers=headers)
        if r.status_code != requests.codes.no_content:
            raise ValueError("Error creating relationship between entity: "+fromUri+" -> "+toUri)
        self.logger.debug("Asserted relationship between "+fromUri+" -> "+toUri)

