String.prototype.format = function (col) {
    //  "Hello {friend}, my name is {name}".format({friend:'joe', name:'mud'})
    //  "Hello joe, my name is mud"
    col = typeof col === 'object' ? col : Array.prototype.slice.call(arguments, 1);

    return this.replace(/\{\{|\}\}|\{(\w+)\}/g, function (m, n) {
        if (m == "{{") { return "{"; }
        if (m == "}}") { return "}"; }
        return col[n];
    });
};

function setupCSRFAjax() {
    //Make all AJAX calls send the CSRF token:
    var token = $("meta[name='csrf_token']").attr("content");
    $.ajaxSetup({
        headers: {'X-csrf': token}
    });
}

$(document).ready(function() {
    setupCSRFAjax();
});
