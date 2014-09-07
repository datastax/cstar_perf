var ws;
var connectionAttempts = 1;

var consoleMessage = function(msg, classes) {
    var conn = $("#console");
    var span = $("<span>");
    span.text(msg);
    if (classes != undefined) {
        $.each(classes, function(i, v) {
            span.addClass(v);
        });
    }
    conn.append(span);
}

var newWebsocket = function() {
    var wsUri = "ws://" + window.location.host + "/api/console";
    var conn = $("#console");
    conn.empty();
    var indicator = $("#status_indicator");
    var change_status = function(state, job_id) {
        if (state === 'wait') {
            indicator.removeClass().addClass("status_wait");
            $("#cluster_status").text("Connected - cluster is waiting for work");
        } else if (state === 'working') {
            indicator.removeClass().addClass("status_working");
            $("#cluster_status").html("Connected -cluster is currently working on job <a href='/tests/id/"+job_id+"'>"+job_id+"</a>");
        } else if (state === 'client_disconnected') {
            indicator.removeClass().addClass("status_disconnected");
            $("#cluster_status").text("Disconnected from server");
        } else if (state === 'unknown') {
            indicator.removeClass().addClass("status_disconnected");
            $("#cluster_status").text("Connected, but no messages from cluster received yet.");
        } else if (state === 'cluster_disconnected') {
            indicator.removeClass().addClass("status_error");
            $("#cluster_status").text("Cluster is not online");
        } else {
            indicator.removeClass();
            $("#cluster_status").text("Unknown state: " + state);
        }
    }
    change_status('client_disconnected');
    ws = new WebSocket(wsUri);
    ws.onmessage = function(evt) {
        var data = JSON.parse(evt.data);
        // Handle keep alive:
        if (data.ctl === "KEEPALIVE") {
            ws.send(evt.data)
            return;
        } 
        // Handle control messages.
        if (data.ctl != undefined) {
            // The server will relay us old messages, so make sure they
            // have the realtime flag:
            if (data.realtime === true) {
                if (data.ctl === "WAIT") {
                    change_status('wait');
                } else if (data.ctl === "START") {
                    change_status('working', data.job_id);
                } else if (data.ctl === "DONE") {
                    change_status('wait');
                } else if (data.ctl === "IN_PROGRESS") {
                    change_status('working', data.job_id);
                } else if (data.ctl != undefined) {
                    change_status(data.ctl);
                }
            } 
            if (data.ctl === "GOODBYE") {
                // Server goodbye doesn't matter if it's realtime or not:
                change_status("cluster_disconnected")
            }
        }
        console.log("Console data : " + evt.data);
        var is_on_bottom = false;
        if (0.95 * (conn.prop("scrollHeight") - conn.scrollTop()) < conn.outerHeight()) {
            is_on_bottom = true;
        }
        if (data.msg != undefined) {
            if (data.realtime === true) {
                change_status('working', data.job_id);
                consoleMessage(data.msg);
            } else {
                // Make non-realtime messages darker:
                consoleMessage(data.msg, ['non_realtime']);
            }
        }
        if (is_on_bottom) {
            conn.scrollTop(conn.prop("scrollHeight"));
        }
    };
    ws.onopen = function(evt) {
        connectionAttempts = 1;
        conn.empty();
        change_status('unknown');
        conn.scrollTop(conn.prop("scrollHeight"));
        var cluster_re = /\/cluster\/(.*)/;
        var cluster_name = cluster_re.exec(window.location.pathname)[1];
        ws.send(cluster_name);
    };
    ws.onclose = function(evt) {
        var timeToWait = exponentialBackoff(connectionAttempts);
        change_status('client_disconnected');
        conn.empty();
        conn.append("<span class='error_text'>Disconnected from server.</span>" + "<br/>");
        conn.append("<span class='error_text'>Will retry in " + (timeToWait/1000) +" seconds ...</span>" + "<br/>");
        setTimeout(function() {
            conn.append("Attempting to reconnect ...<br/>");
            connectionAttempts=connectionAttempts+1;
            ws = newWebsocket();
        }, timeToWait);
    };
    ws.onerror = function(evt) {
        change_status('client_disconnected');
    };
    return ws;
}


function exponentialBackoff (k, limit) {
    var maxInterval = (Math.pow(2, k) - 1) * 1000;
    
    if (limit === undefined) {
        limit = 30;
    }
    if (maxInterval > limit*1000) {
        maxInterval = limit*1000; // If the generated interval is more than [limit] seconds, truncate it down to [limit] seconds.
    }
    
    return maxInterval; 
}

$(document).ready(function() {
    ws = newWebsocket();
    //Close the websocket cleanly on page unload:
    window.onbeforeunload = function() {
        ws.onclose = function() {};
        console.log("Shutting down websocket due to page unload");
        ws.close();
    };
});
