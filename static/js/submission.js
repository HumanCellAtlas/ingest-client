$(document).ready(function() {
    renderDates();
    // $('#files').DataTable({searching: false, paging: false});
    // Requires Bootstrap 3 for functionality
    $('.js-tooltip').tooltip();

    $('.js-copy').click(function() {
        var text = $('#staging-credentials').text();
        console.log(text);
        var el = $(this);
        copyToClipboard(text, el);
    });
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

