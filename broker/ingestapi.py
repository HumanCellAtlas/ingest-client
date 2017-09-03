import glob, json, os, urllib, requests

class IngestApi:
    def __init__(self, url=None):

        self.url = url if url else "http://localhost:8080"

        self.ingest_api = None
        self.headers = {'Content-type': 'application/json', 'Accept': 'application/json'}

        self.submission_links = None
        self.currentSubmission = None
        self.load_root()

    def load_root (self):
        reply = urllib.urlopen(self.url)
        self.ingest_api = json.load(reply)["_links"]

    def createSubmission(self):
        r = requests.post(self.ingest_api["submissionEnvelopes"]["href"].rsplit("{")[0], data="{}",
                          headers=self.headers)

        if r.status_code == requests.codes.created:
            self.submission_links = json.loads(r.text)["_links"]
            self.currentSubmission = self.submission_links["self"]["href"]
            return r.text
        else:
            print "Error getting submission envelope:" + json.loads(r.text)["message"]
            exit(1)

    def finishSubmission(self):
        r = requests.put(self.submission_links["submit"]["href"].rsplit("{")[0], headers=self.headers)
        if r.status_code == requests.codes.update:
            print "Submission complete!"
            return r.text

    def createProject(self, jsonObject):
        return self.createEntity(jsonObject, "projects")

    def createSample(self, jsonObject):
        return self.createEntity(jsonObject, "samples")

    def createAssay(self, jsonObject):
        return self.createEntity(jsonObject, "assays")

    def createDonor(self, jsonObject):
        return self.createSample(jsonObject)

    def createAnalysis(self, jsonObject):
        return self.createEntity(jsonObject, "analyses")

    def createEntity(self, jsonObject, entityType):
        print ".",
        r = requests.post(self.ingest_api[entityType]["href"].rsplit("{")[0], data=jsonObject,
                          headers=self.headers)
        if r.status_code == requests.codes.created:
            # associate this project to the submission
            headers = {'Content-type': 'text/uri-list'}
            r = requests.post(self.submission_links[entityType]["href"].rsplit("{")[0],
                             data=json.loads(r.text)["_links"]["self"]["href"].rsplit("{")[0], headers=headers)
            return r.text


