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
    var LAST_UPDATE_INTERVAL = 0; //in minutes, added to capture any updates that happened in the last time interval

    var rowData = row.data();
    var url = rowData.url;
    var date = new Date(rowData.date);
    date.setMinutes(date.getMinutes() - LAST_UPDATE_INTERVAL);

    var submissionId = url.split("/").pop();

    $.ajax({
        url: '/submissions/'+ submissionId + '/'+ date.toUTCString(),
        type: 'GET',
        dataType: 'html',
        success: function(response){
            if(response){
                row.html(response);
                renderDates();
                var now = new Date();
                now.setMinutes(now.getMinutes() - LAST_UPDATE_INTERVAL);
                row.data('date', now.toUTCString());
            }
        },
        complete: function(data){
            setTimeout(function(){
                pollRow(row);
            }, POLL_INTERVAL)
        },
    });
}

function renderDates() {
    $(".date-column").each(function () {
        var date = new Date($(this).data("date"));
        $(this).text(date.toLocaleTimeString() + " " + date.toLocaleDateString());
    });
}
