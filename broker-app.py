from flask import Flask, Markup, flash, request, render_template, redirect, url_for
from broker.hcaxlsbroker import SpreadsheetSubmission
from broker.ingestapi import IngestApi
from werkzeug.utils import secure_filename
import os
import sys
import tempfile
import threading
import requests

app = Flask(__name__)
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
        submission = SpreadsheetSubmission()
        submissionId = submission.createSubmission()
        thread = threading.Thread(target=submission.submit, args=(path,submissionId))
        thread.start()

        message = Markup("Submission created with id <a href='"+submissionId+"'>"+submissionId+"</a>")
        flash(message, "alert-success")
        return redirect(url_for('index'))
    return  redirect(url_for('index'))

@app.route('/submit', methods=['POST'])
def submit_envelope():
    subUrl = request.form.get("submissionUrl")
    ingestApi = IngestApi()
    if subUrl:
        text = ingestApi.finishSubmission(subUrl)

    return  redirect(url_for('index'))

if __name__ == '__main__':
    app.config['ingest_url'] = sys.argv[2]
    app.run(debug=True,host='0.0.0.0')
