import glob, json, os, urllib, requests

class IngestApi:
    def __init__(self, url=None):

        if not url and 'INGEST_API' in os.environ:
            url = os.environ['INGEST_API']
            print "using " +url+ " for ingest API"
        self.url = url if url else "http://localhost:8080"

        self.ingest_api = None
        self.headers = {'Content-type': 'application/json', 'Accept': 'application/json'}

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
            print "Error getting submission envelope:" + json.loads(r.text)["message"]
            exit(1)

    def finishSubmission(self, submissionUrl):
        r = requests.put(submissionUrl, headers=self.headers)
        if r.status_code == requests.codes.update:
            print "Submission complete!"
            return r.text

    def finishedForNow(self, submissionUrl):
        self._updateStatusToPending(submissionUrl)

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

    def createAnalysis(self, submissionUrl, jsonObject):
        return self.createEntity(submissionUrl,jsonObject, "analyses")

    def createEntity(self, submissionUrl, jsonObject, entityType):
        print ".",
        r = requests.post(self.ingest_api[entityType]["href"].rsplit("{")[0], data=jsonObject,
                          headers=self.headers)
        if r.status_code == requests.codes.created:
            # associate this project to the submission
            headers = {'Content-type': 'text/uri-list'}
            r = requests.post(self.submission_links[submissionUrl][entityType]["href"].rsplit("{")[0],
                             data=json.loads(r.text)["_links"]["self"]["href"].rsplit("{")[0], headers=headers)
            return r.text


