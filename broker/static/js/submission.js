$(document).ready(function() {
    var POLL_INTERVAL = 5000; //in milliseconds

    var url = $('#submission-url').attr('href');

    var env = /staging/.test(url) ? 'staging' : 'dev';
    console.log('Currently in ' + env);

    setInterval(function(){
        pollSubmission(url);
        pollFiles(url);
    }, POLL_INTERVAL)

    // $('#files').DataTable({searching: false, paging: false});
    // Requires Bootstrap 3 for functionality
    $('.js-tooltip').tooltip();

    $('.js-copy').click(function() {
        var text = $('#staging-credentials').text();
        console.log(text);
        var el = $(this);
        copyToClipboard(text, el);
    });

    renderDates();
    renderDSSLinks(env);
});

function copyToClipboard(text, el) {
    var copyTest = document.queryCommandSupported('copy');
    var elOriginalText = el.attr('data-original-title');

    if (copyTest === true) {
        var copyTextArea = document.createElement("textarea");
        copyTextArea.value = text;
        document.body.appendChild(copyTextArea);
        copyTextArea.select();
        try {
            var successful = document.execCommand('copy');
            var msg = successful ? 'Copied!' : 'Not copied!';
            el.attr('data-original-title', msg).tooltip('show');
        } catch (err) {
            console.log('Unable to copy');
        }
        document.body.removeChild(copyTextArea);
        el.attr('data-original-title', elOriginalText);
    } else {
        // Fallback if browser doesn't support .execCommand('copy')
        window.prompt("Copy to clipboard: Ctrl+C or Command+C, Enter", text);
    }
}

function pollSubmission(url){
    var date = $('#submission-update-date').data('date');

    var submissionId = url.split("/").pop(); // for logging only
    $.ajax({
        url: url,
        headers: {"If-Modified-Since":date},
        type: 'GET',
        dataType: 'json',
        success: function(data, textStatus, jqXHR){
            if(data){
                var lastModifiedDate = jqXHR.getResponseHeader('Last-Modified');
                console.log("Got response, submissionId '" + submissionId + " was last modified at " + lastModifiedDate);
                renderSubmissionChanges(url, data);
                $('#submission-update-date').data('date', lastModifiedDate);
            }
        },
    });
}

function renderSubmissionChanges(url, data) {
    var STATUS_LABEL = {
        'Valid': 'label-success',
        'Validating': 'label-info',
        'Invalid': 'label-danger',
        'Submitted': 'label-default',
        'Complete': 'label-default'
    }

    var DEFAULT_STATUS_LABEL = 'label-warning';

    var status = $('#submission-status');
    status.text(data.submissionState);
    status.attr('class', 'submission-state label ' + (STATUS_LABEL[data.submissionState] || DEFAULT_STATUS_LABEL) + ' label-lg');

    var date = moment(data.updateDate).toDate(); // use moment to correctly parse date even in safari
    var formattedDate = date.toLocaleTimeString() + " " + date.toLocaleDateString();

    $('#submission-update-date').text(formattedDate);

    var formDiv = $('#submission-form');
    var submitUrl = data['_links']['submit'] ? data['_links']['submit']['href'] : null;
    var completeForm = createSubmissionForm(submitUrl);
    formDiv.html(completeForm);
    console.log(submitUrl);
    console.log("Rendered updated info");
}

function createSubmissionForm(submissionUrl){
    var htmlForm = '';
    if(submissionUrl){
        htmlForm = `
        <div class="col-lg-10">
            <dl class="dl-horizontal">
                <dt></dt>
                <dd>
                    <form class="submission-form complete-form" action="/submit" method="POST">
                        <input class="submission-url-input" type="hidden" name="submissionUrl" value="` + submissionUrl + `"/>
                        <button class="btn btn-success" onclick="return confirm(\'Are all data files uploaded to the staging area?\');">
                            Complete submission
                        </button>
                    </form>
                </dd>
            </dl>
        </div>`;
    }

    return htmlForm;
}

// TODO: improve submission files polling
function pollFiles(url){
    var submissionId = url.split("/").pop();

    $.ajax({
        url: '/submissions/'+ submissionId + '/files',
        type: 'GET',
        dataType: 'html',
        success: function(data, textStatus, jqXHR){
            if(data){
                $('#submission-files').html(data);
                renderDates();
                console.log('Rendered submission files.');
            }

        },
    });
}

function renderDSSLinks(env){
    var DSS_API = {
        'dev': 'https://dss.dev.data.humancellatlas.org/v1/bundles/{UUID}?replica=aws',
        'staging': 'https://dss.staging.data.humancellatlas.org/v1/bundles/{UUID}?replica=aws'
    };

    $('a.dss-url').each(function () {
        var uuid = $(this).data('uuid');
        var url = DSS_API[env];
        url = url.replace('{UUID}', uuid);
        $(this).attr('href', url);
    });




}