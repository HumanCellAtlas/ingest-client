$(function() {
    triggerPolling();
    renderDates();
});

function triggerPolling(){
    $('#submissions > tbody > tr').each(function(){
        var row = $(this);
        pollRow(row);
    });
}

function pollRow(row){
    var POLL_INTERVAL = 3000; //in milliseconds
    var LAST_UPDATE_INTERVAL_S = 2; //in seconds, added to capture any updates that happened in the last time interval

    var rowData = row.data();
    var url = rowData.url;
    var date = rowData.date;

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
                renderResponse(url, data);
                row.data('date', lastModifiedDate);
            }
        },
        complete: function(data){
            setTimeout(function(){
                pollRow(row);
            }, POLL_INTERVAL)
        }
    });
}

function renderResponse(url, data) {
    var row = $('#submissions').find('> tbody > tr[data-url="' + url + '"]');
    row.find('.submission-state').text(data.submissionState);

    var date = moment(data.updateDate.date).toDate(); // use moment to correctly parse date even in safari
    var formattedDate = date.toLocaleTimeString() + " " + date.toLocaleDateString();

    row.find('.update-date').text(formattedDate);
    console.log("Rendered updated info");
}
