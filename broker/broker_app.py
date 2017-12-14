#!/usr/bin/env python
from broker.spreadsheetUploadError import SpreadsheetUploadError

__author__ = "jupp"
__license__ = "Apache 2.0"

import logging
import os
import sys
import tempfile
import threading
import traceback

from flask import Flask, request
from flask import json
from flask_cors import CORS, cross_origin
from werkzeug.utils import secure_filename

from broker.hcaxlsbroker import SpreadsheetSubmission
from broker.ingestapi import IngestApi

app = Flask(__name__)
app.secret_key = 'cells'
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'


@app.route('/upload', methods=['POST'])
@cross_origin()
def upload_spreadsheet():
    try:
        # check token
        token = request.headers.get('Authorization')
        if token is None:
            raise SpreadsheetUploadError(401, "An authentication token must be supplied when uploading a spreadsheet",
                                         "")

        # save file
        try:
            print ("Saving file..")
            f = request.files['file']
            filename = secure_filename(f.filename)
            path = os.path.join(tempfile.gettempdir(), filename)
            f.save(path)
        except Exception as err:
            print(traceback.format_exc())
            message = "We experienced a problem when saving your spreadsheet"
            raise SpreadsheetUploadError(500, message, str(err))

        # do a dry run to minimally validate spreadsheet
        try:
            submission = SpreadsheetSubmission(dry=True)
            submission.submit(path, None)
        except ValueError as err:
            print(traceback.format_exc())
            message = "There was a problem validating your spreadsheet"
            raise SpreadsheetUploadError(400, message, str(err))
        except KeyError as err:
            print(traceback.format_exc())
            message = "There was a problem with the content of your spreadsheet"
            raise SpreadsheetUploadError(400, message, str(err))

        # if we get here can go ahead and submit
        try:
            submission.dryrun = False
            submission_url = submission.createSubmission(token)
            thread = threading.Thread(target=submission.submit, args=(path, submission_url, token, None))
            thread.start()
        except Exception as err:
            print(traceback.format_exc())
            message = "We experienced a problem when creating a submission from your spreadsheet"
            raise SpreadsheetUploadError(500, message, str(err))

        return create_upload_success_response(submission_url)
    except SpreadsheetUploadError as spreadsheetUploadError:
        return create_upload_failure_response(spreadsheetUploadError.http_code, spreadsheetUploadError.message,
                                              spreadsheetUploadError.details)
    except Exception as err:
        print(traceback.format_exc())
        return create_upload_failure_response(500, "We experienced a problem while uploading your spreadsheet",
                                              str(err))


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


# @app.route('/staging/delete', methods=['POST'])
# def delete_staging():
#    subUrl = request.form.get("submissionUrl")
#    submissionId = subUrl.rsplit('/', 1)[-1]
#    if submissionId:
#        ingestApi = IngestApi()
#        uuid = ingestApi.getObjectUuid(subUrl)
#        stagingApi = StagingApi()
#        text = stagingApi.deleteStagingArea(uuid)
#        message = Markup("Staging area deleted for <a href='" + text + "'>" + text + "</a>")
#        flash(message, "alert-success")
#    return redirect(url_for('index'))


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    app.run(host='0.0.0.0', port=5000)
