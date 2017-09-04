from flask import Flask, Markup, flash, request, render_template, redirect, url_for
from broker.hcaxlsbroker import SpreadsheetSubmission
from broker.ingestapi import IngestApi
from werkzeug.utils import secure_filename
import os
import tempfile
import threading
import requests

app = Flask(__name__)
app.secret_key = 'cells'

@app.route('/')
def index():
    submissions = IngestApi().getSubmissions()
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
        thread = threading.Thread(target=submission.submit, args=(path,))
        thread.start()

        message = Markup("Submission created with id <a href='"+submissionId+"'>"+submissionId+"</a>")
        flash(message)
        return redirect(url_for('index'))
    return  redirect(url_for('index'))

@app.route('/submit', methods=['POST'])
def submit_envelope():
    subUrl = request.form.get("submissionUrl")
    if subUrl:
        print subUrl
        headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
        r = requests.put(subUrl, headers=headers)
        if r.status_code == requests.codes.update:
            flash(r.text)

    return  redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0')