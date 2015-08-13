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

// parseUri 1.2.2
// (c) Steven Levithan <stevenlevithan.com>
// MIT License

function parseUri (str) {
	var	o   = parseUri.options,
		m   = o.parser[o.strictMode ? "strict" : "loose"].exec(str),
		uri = {},
		i   = 14;

	while (i--) uri[o.key[i]] = m[i] || "";

	uri[o.q.name] = {};
	uri[o.key[12]].replace(o.q.parser, function ($0, $1, $2) {
		if ($1) uri[o.q.name][$1] = $2;
	});

	return uri;
};

parseUri.options = {
	strictMode: false,
	key: ["source","protocol","authority","userInfo","user","password","host","port","relative","path","directory","file","query","anchor"],
	q:   {
		name:   "queryKey",
		parser: /(?:^|&)([^&=]*)=?([^&]*)/g
	},
	parser: {
		strict: /^(?:([^:\/?#]+):)?(?:\/\/((?:(([^:@]*)(?::([^:@]*))?)?@)?([^:\/?#]*)(?::(\d*))?))?((((?:[^?#\/]*\/)*)([^?#]*))(?:\?([^#]*))?(?:#(.*))?)/,
		loose:  /^(?:(?![^:@]+:[^:@\/]*@)([^:\/?#.]+):)?(?:\/\/)?((?:(([^:@]*)(?::([^:@]*))?)?@)?([^:\/?#]*)(?::(\d*))?)(((\/(?:[^?#](?![^?#\/]*\.[^?#\/.]+(?:[?#]|$)))*\/?)?([^?#\/]*))(?:\?([^#]*))?(?:#(.*))?)/
	}
};


function update_select_with_values(element, values, context) {
    var el = $(element);

    //Remember the current jvm selections:
    var current_selections = [];
    el.each(function(i, e) {
        current_selections[i] = $(e).val();
    });

    //Clear out the lists and fetch new one:
    el.empty();
    if(values==null || values.length == 0) {
        alert("Warning: cluster has no "+context+" defined.");
    }

    $.each(values, function(key, val) {
        el.append($("<option value='"+val+"'>"+key+"</option>"));
    });

    //Try to set the one we had from before:
    el.each(function(i, e) {
        if (current_selections[i] != null) {
            $(e).val(current_selections[i]);
        }
        if ($(e).val() == null) {
            $(e).find("option:first-child").attr("selected", "selected");
            alert("Warning - cluster "+context+" selection changed from '"+current_selections[i]+"' to '"+$(e).val()+"'");
        }
    });
}

$(document).ready(function() {
    setupCSRFAjax();
});
