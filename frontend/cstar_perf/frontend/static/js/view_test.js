var cancelTest = function(test_id, callback){
    $.post('/api/tests/cancel', {'test_id':test_id}, callback);
}

var getTest = function(test_id, callback, fail_callback){
    $.getJSON('/api/tests/id/'+test_id, callback).fail(fail_callback);
}

var checkForTestStatusUpdates = function(interval) {
    // Check for test status updates, refresh the page if there are.
    // If [interval] is undefined, check only once.
    // Otherwise check periodically every [interval] seconds.
    var currentStatus = $("#test_status").text();
    var test_id = $("#test_id").text();
    console.log("Checking for test updates " + (interval ? 'every '+interval+'ms' : 'once'));
    getTest(test_id, function(test) {
        var newStatus = test['status'];
        if (currentStatus != newStatus) {
            window.location.reload();
        } else {
            //Check again for update in 30s:
            if (interval != undefined) 
                setTimeout(function(){checkForTestStatusUpdates(interval)}, interval);
        }
    }, function() {
        //If there's an error, schedule retry in 3xinterval:
        if (interval != undefined) 
            setTimeout(function(){checkForTestStatusUpdates(interval)}, 3*interval)
    });
}

$(document).ready(function() {
    //Create cancel button for tests in scheduled status:
    var field = $("#test_status");
    var test_id = $("#test_id").text();
    if (field.text() === "scheduled") {
        var btn = $("<button class='btn' style='margin-left:30px'>Cancel Test</button>").click(function() {
            cancelTest(test_id, function(){
                window.location.reload();
            });
        });
        field.parent().append(btn);
    } else if (field.text() === 'failed') {
        field.addClass('error')
    } else if (field.text() === 'completed') {
        field.addClass('success')
    }

    setTimeout(function() {
        // On page load, one time, check for status updates after 5s:
        checkForTestStatusUpdates();
        // Then check periodically every 30s:
        setTimeout(function() { checkForTestStatusUpdates(30000)}, 30000);
    }, 5000);
    
});
