from flask import Flask,request,  render_template, redirect, url_for
from broker.hcaxlsbroker import SpreadsheetSubmission
from werkzeug.utils import secure_filename
import os
import tempfile

app = Flask(__name__)

@app.route('/')
def index(submissionId=None):
    return render_template('index.html', submissionId=submissionId)


@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    print "got a request "+request.method
    if request.method == 'POST':
        print "saving.."
        f = request.files['file']
        filename = secure_filename(f .filename)
        path = os.path.join(tempfile.gettempdir(), filename)
        f.save(path)
        submission = SpreadsheetSubmission()
        submissionId = submission.submit(path)
        print "submitted with id: "+submissionId
        return redirect(url_for('index', submissionId=submissionId))
    return  redirect(url_for('index'))


if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0')