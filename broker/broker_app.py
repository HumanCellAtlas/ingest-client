#!/usr/bin/env python
from spreadsheetUploadError import SpreadsheetUploadError

__author__ = "jupp"
__license__ = "Apache 2.0"

from flask import Flask, Markup, flash, request, render_template, redirect, url_for
from flask_cors import CORS, cross_origin
from flask import json
from hcaxlsbroker import SpreadsheetSubmission
from ingestapi import IngestApi
from stagingapi import StagingApi
from werkzeug.utils import secure_filename
import os, sys
import tempfile
import threading
import logging
import traceback
import token_util as token_util

STATUS_LABEL = {
    'Valid': 'label-success',
    'Validating': 'label-info',
    'Invalid': 'label-danger',
    'Submitted': 'label-default',
    'Complete': 'label-default'
}

DEFAULT_STATUS_LABEL = 'label-warning'


HTML_HELPER = {
    'status_label': STATUS_LABEL,
    'default_status_label': DEFAULT_STATUS_LABEL
}


app = Flask(__name__, static_folder='static')
app.secret_key = 'cells'
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'
logger = logging.getLogger(__name__)


@app.route('/api_upload', methods=['POST'])
@cross_origin()
def upload_spreadsheet():
    try:
        logger.info("Uploading spreadsheet")

        # check token
        logger.info("Checking token")
        token = request.headers.get('Authorization')
        if token is None:
            raise SpreadsheetUploadError(401, "An authentication token must be supplied when uploading a spreadsheet", "")

        # save file
        logger.info("Saving file")
        try:
            path = _save_file()
        except Exception as err:
            logger.error(traceback.format_exc())
            message = "We experienced a problem when saving your spreadsheet"
            raise SpreadsheetUploadError(500, message, str(err))

        # check for project_id
        logger.info("Checking for project_id")
        project_id = None

        if 'project_id' in request.form:
            project_id = request.form['project_id']
            logger.info("Found project_id: " + project_id)
        else:
            logger.info("No existing project_id found")

        # do a dry run to minimally validate spreadsheet
        logger.info("Attempting dry run to validate spreadsheet")
        try:
            submission = SpreadsheetSubmission(dry=True)
            submission.submit(path, None, None, project_id)
        except ValueError as err:
            logger.error(traceback.format_exc())
            message = "There was a problem validating your spreadsheet"
            raise SpreadsheetUploadError(400, message, str(err))
        except KeyError as err:
            logger.error(traceback.format_exc())
            message = "There was a problem with the content of your spreadsheet"
            raise SpreadsheetUploadError(400, message, str(err))

        # if we get here can go ahead and submit
        logger.info("Attempting submission")
        try:
            submission.dryrun = False
            submission_url = submission.createSubmission(token)
            submission.submit(path, submission_url, token, project_id)
            # REMOVED THREADING AS IT IS NOT EASY TO GET AN ERROR MESSAGE BACK BUT THIS MAY BE NEEDED
            # thread = threading.Thread(target=submission.submit, args=(path, submission_url, token, project_id))
            # thread.start()

        except Exception as err:
            logger.error(traceback.format_exc())
            message = "We experienced a problem when creating a submission for your spreadsheet"
            raise SpreadsheetUploadError(400, message, str(err))
        logger.info("Spreadsheet upload completed")
        return create_upload_success_response(submission_url)
    except SpreadsheetUploadError as spreadsheetUploadError:
        return create_upload_failure_response(spreadsheetUploadError.http_code, spreadsheetUploadError.message,
                                              spreadsheetUploadError.details)
    except Exception as err:
        logger.error(traceback.format_exc())
        return create_upload_failure_response(500, "We experienced a problem while uploading your spreadsheet",
                                              str(err))


def _save_file():
    f = request.files['file']
    filename = secure_filename(f.filename)
    path = os.path.join(tempfile.gettempdir(), filename)
    logger.info("Saved file to: " + path)
    f.save(path)
    return path


def create_upload_success_response(submission_url):
    ingest_api = IngestApi()
    submission_uuid = ingest_api.getObjectUuid(submission_url)
    display_id = submission_uuid or '<UUID not generated yet>'
    submission_id = submission_url.rsplit('/', 1)[-1]

    data = {
        "message": "Your spreadsheet was uploaded and processed successfully",
        "details": {
            "submission_url": submission_url,
            "submission_uuid": submission_uuid,
            "display_uuid": display_id,
            "submission_id": submission_id
        }
    }

    success_response = app.response_class(
        response=json.dumps(data),
        status=201,
        mimetype='application/json'
    )
    return success_response


def create_upload_failure_response(status_code, message, details):
    data = {
        "message": message,
        "details": details,
    }
    failure_response = app.response_class(
        response=json.dumps(data),
        status=status_code,
        mimetype='application/json'
    )
    print failure_response
    return failure_response

@app.route('/')
def index():
    submissions = []
    try:
        submissions = IngestApi().getSubmissions()
    except Exception as e:
        flash("Can't connect to ingest API!!", "alert-danger")
    return render_template('index.html', submissions=submissions, helper=HTML_HELPER)

@app.route('/submissions/<id>')
def get_submission_view(id):
    ingest_api = IngestApi()
    submission = ingest_api.getSubmissionIfModifiedSince(id, None)

    if(submission):
        response = ingest_api.getProjects(id)

        projects = []

        if('_embedded' in response and 'projects' in response['_embedded']):
            projects = response['_embedded']['projects']

        project = projects[0] if projects else None # there should always 1 project

        files = []

        response = ingest_api.getFiles(id)
        if('_embedded' in response and 'files' in response['_embedded']):
            files = response['_embedded']['files']

        filePage = None
        if('page' in response):
            filePage = response['page']
            filePage['len'] = len(files)

        bundleManifests = []
        bundleManifestObj = {}

        response = ingest_api.getBundleManifests(id)
        if('_embedded' in response and 'bundleManifests' in response['_embedded']):
            bundleManifests = response['_embedded']['bundleManifests']

        bundleManifestObj['list'] = bundleManifests
        bundleManifestObj['page'] = None

        if('page' in response):
            bundleManifestObj['page'] = response['page']
            bundleManifestObj['page']['len'] = len(bundleManifests)

        return render_template('submission.html',
                               sub=submission,
                               helper=HTML_HELPER,
                               project=project,
                               files=files,
                               filePage=filePage,
                               bundleManifestObj=bundleManifestObj)
    else:
        flash("Submission doesn't exist!", "alert-danger")
        return  redirect(url_for('index'))

@app.route('/submissions/<id>/files')
def get_submission_files(id):
    ingest_api = IngestApi()
    response = ingest_api.getFiles(id)

    files = []
    if('_embedded' in response and 'files' in response['_embedded']):
        files = response['_embedded']['files']

    filePage = None
    if('page' in response):
        filePage = response['page']
        filePage['len'] = len(files)


    return render_template('submission-files-table.html',
                           files=files,
                           filePage=filePage,
                           helper=HTML_HELPER)

@app.route('/upload', methods=['POST'])
def upload_file():
    if request.method == 'POST':
        logger.info("saving..")
        f = request.files['file']
        filename = secure_filename(f .filename)
        path = os.path.join(tempfile.gettempdir(), filename)
        f.save(path)

        # do a dry run to minimally validate spreadsheet
        submission = SpreadsheetSubmission(dry=True)
        try:
            submission.submit(path,None)
        except ValueError as e:
            flash(str(e), 'alert-danger')
            return redirect(url_for('index'))
        except Exception as e:
            flash(str(e), 'alert-danger')
            return redirect(url_for('index'))

        # if we get here can go ahead and submit
        submission.dryrun = False

        token = "Bearer " + token_util.get_token()

        logger.info("token: " + token)

        submissionUrl = submission.createSubmission(token)
        thread = threading.Thread(target=submission.submit, args=(path,submissionUrl))
        thread.start()

        ingestApi = IngestApi()
        submissionUUID = ingestApi.getObjectUuid(submissionUrl)
        displayId= submissionUUID or '<UUID not generated yet>'
        submissionId = submissionUrl.rsplit('/', 1)[-1]

        message = Markup("Submission created with UUID : <a class='submission-id' href='"+submissionUrl+"'>"+ displayId +"</a>")

        flash(message, "alert-success")
        return redirect(url_for('index') + '#' + submissionId) # temporarily adding submission id in url for integration testing
    flash("You can only POST to the upload endpoint", "alert-warning")
    return  redirect(url_for('index'))

@app.route('/submit', methods=['POST'])
def submit_envelope():
    subUrl = request.form.get("submissionUrl")
    ingestApi = IngestApi()
    if subUrl:
        text = ingestApi.finishSubmission(subUrl)

    return  redirect(url_for('index'))

@app.route('/staging/delete', methods=['POST'])
def delete_staging():
    subUrl = request.form.get("submissionUrl")
    submissionId = subUrl.rsplit('/', 1)[-1]
    if submissionId:
        ingestApi = IngestApi()
        uuid = ingestApi.getObjectUuid(subUrl)
        stagingApi = StagingApi()
        text = stagingApi.deleteStagingArea(uuid)
        message = Markup("Staging area deleted for <a href='" + text + "'>" + text + "</a>")
        flash(message, "alert-success")
    return      redirect(url_for('index'))

if __name__ == '__main__':

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    app.run(host='0.0.0.0', port=5000)
