#!/usr/bin/env python
"""
desc goes here 
"""
__author__ = "jupp"
__license__ = "Apache 2.0"

from flask import Flask, Markup, flash, request, render_template, redirect, url_for
from broker.hcaxlsbroker import SpreadsheetSubmission
from broker.ingestapi import IngestApi
from broker.stagingapi import StagingApi
from werkzeug.utils import secure_filename
import os, sys
import tempfile
import threading
import logging

app = Flask(__name__, static_folder='static')
app.secret_key = 'cells'

@app.route('/')
def index():
    submissions = []
    try:
        submissions = IngestApi().getSubmissions()
    except Exception, e:
        flash("Can't connect to ingest API!!", "alert-danger")

    return render_template('index.html', submissions=submissions)


@app.route('/upload', methods=['POST'])
def upload_file():
    if request.method == 'POST':
        print "saving.."
        f = request.files['file']
        filename = secure_filename(f .filename)
        path = os.path.join(tempfile.gettempdir(), filename)
        f.save(path)

        # do a dry run to minimally validate spreadsheet
        submission = SpreadsheetSubmission(dry=True)
        try:
            submission.submit(path,None)
        except ValueError, e:
            flash(str(e), 'alert-danger')
            return redirect(url_for('index'))
        except Exception, e:
            flash(str(e), 'alert-danger')
            return redirect(url_for('index'))

        # if we get here can go ahead and submit
        submission.dryrun = False
        submissionId = submission.createSubmission()
        thread = threading.Thread(target=submission.submit, args=(path,submissionId))
        thread.start()
        message = Markup("Submission created with id <a href='"+submissionId+"'>"+submissionId+"</a>")
        flash(message, "alert-success")
        return redirect(url_for('index'))
    flash("You can only POST to the upload endpoint", "alert-warning")
    return  redirect(url_for('index'))

@app.route('/submit', methods=['POST'])
def submit_envelope():
    subUrl = request.form.get("submissionUrl")
    ingestApi = IngestApi()
    if subUrl:
        text = ingestApi.finishSubmission(subUrl)

    return  redirect(url_for('index'))

@app.route('/staging/create', methods=['POST'])
def create_staging():
    subUrl = request.form.get("submissionUrl")
    submissionId = subUrl.rsplit('/', 1)[-1]
    if submissionId:
        ingestApi = IngestApi()
        uuid = ingestApi.getObjectUuid(subUrl)
        stagingApi = StagingApi()
        text = stagingApi.createStagingArea(uuid)
        message = Markup("Staging area created at <a href='" + text + "'>" + text + "</a>")
        flash(message, "alert-success")
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
    return  redirect(url_for('index'))

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    app.run(host='0.0.0.0')

