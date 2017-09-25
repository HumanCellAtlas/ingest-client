function renderDates() {
    $(".date-column").each(function () {
        var date = new Date($(this).data("date"));
        $(this).text(date.toLocaleTimeString() + " " + date.toLocaleDateString());
    });
}