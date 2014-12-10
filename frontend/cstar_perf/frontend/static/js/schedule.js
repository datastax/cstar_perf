schedule = {
    n_revisions: 0,
    n_operations: 0
};

var addRevisionDiv = function(animate){
    schedule.n_revisions++;
    var revision_id = 'revision-'+schedule.n_revisions;
    var template = "<div id='{revision_id}' class='revision'><legend>Test Revisions<a id='remove-{revision_id}' class='pull-right remove-revision'><span class='glyphicon" +
        "                  glyphicon-remove'></span></a></legend>" +
        "      <div class='form-group'>" +
        "        <label class='col-md-4 control-label' for='{revision_id}-refspec'>Revision</label>  " +
        "        <div class='col-md-8'>" +"" +
        "          <input id='{revision_id}-refspec' type='text' placeholder='Git branch, tag, or commit id' class='refspec form-control input-md' required>" +
        "        </div>" +
        "      </div>" +
        "" +
        "      <div class='form-group'>" +
        "        <label class='col-md-4 control-label' for='{revision_id}-label'>Label</label>  " +
        "        <div class='col-md-8'>" +
        "          <input id='{revision_id}-label' type='text'" +
        "          placeholder='One line description of Revision'" +
        "          class='form-control input-md revision-label'> " +
        "          <span class='help-block'>Defaults to Revision if unspecified</span>       " +
        "        </div>" +
        "      </div>" +
        "" +
        "      <div class='form-group'>" +
        "        <label class='col-md-4 control-label' for='{revision_id}-yaml'>Cassandra.yaml</label>" +
        "        <div class='col-md-8'>" +
        "          <textarea class='form-control yaml' id='{revision_id}-yaml'" +
        "          placeholder='Any cassandra.yaml options you want that differ from the default settings for the chosen cluster.'></textarea>" +
        "        </div>" +
        "      </div>" +
        "" +
        "      <div class='form-group'>" +
        "        <label class='col-md-4 control-label' for='{revision_id}-env-vars'>Environment Script</label>" +
        "        <div class='col-md-8'>" +
        "          <textarea class='form-control env-vars' id='{revision_id}-env-vars'" +
        "          placeholder='Environment settings to prepend to cassandra-env.sh'></textarea>" +
        "        </div>" +
        "      </div>" +
        "" +
        "      <div class='form-group'>" +
        "        <label class='col-md-4 control-label' for='{revision_id}-jvm'>JVM</label>" +
        "        <div class='col-md-8'>" +
        "          <select id='{revision_id}-jvm' " +
        "                  class='jvm-select form-control required'>" +
        "          </select>" +
        "        </div>" +
        "      </div>" +
        "" +
        "      <div class='form-group'>" +
        "        <label class='col-md-4 control-label'" +
        "        for='{revision_id}-options'>Other Options</label>" +
        "        <div class='col-md-8'>" +
        "          <div class='checkbox'>" +
        "            <input type='checkbox' class='options-vnodes' id='{revision_id}-options-vnodes' checked='checked'>" +
        "            <label for='{revision_id}-options'>" +
        "              Use Virtual Nodes" +
        "            </label>" +
        "	  </div>" +
        "        </div>" +
        "      </div>" +
        "    </div>";
    var newDiv = $(template.format({revision:schedule.n_revisions, revision_id:revision_id}));
    if (animate) 
        newDiv.hide();
    $("#schedule-revisions").append(newDiv);
    if (animate)
        newDiv.slideDown();

    //Populate JVMs per the previous revision:
    if (schedule.n_revisions > 1) {
        $("#revision-"+(schedule.n_revisions-1)+"-jvm option").clone().appendTo("#"+revision_id+"-jvm");
        $("#"+revision_id+"-jvm").val($("#revision-"+(schedule.n_revisions-1)+"-jvm").val());
    } else {
        $("#cluster").change();
    }

    //Remove revision handler:
    $("#remove-"+revision_id).click(function() {
        $("div#"+revision_id).slideUp(function() {
            this.remove();
        });
    });

};

var addOperationDiv = function(animate, operation, cmd){
    schedule.n_operations++;
    var operation_id = 'operation-'+schedule.n_operations;
    if (!cmd)
        cmd = 'write n=19000000 -rate threads=50';
    var template = "<div id='{operation_id}' class='operation'><legend>Operation<a class='pull-right' id='remove-{operation_id}'><span class='glyphicon" +
        "                  glyphicon-remove'></span></a></legend>" +
        "      <div class='form-group'>" +
        "        <label class='col-md-3 control-label'" +
        "        for='{operation_id}-type'>Operation</label>" +
        "        <div class='col-md-9'>" +
        "          <select id='{operation_id}-type'" +
        "                  class='form-control type'>" +
        "            <option value='stress'>stress</option>" +
        "            <option value='flush'>flush</option>" +
        "            <option value='compact'>compact</option>" +
        "          </select>" +
        "        </div>" +
        "      </div>" +
        "      " +
        "      <div class='form-group stress'>" +
        "        <label class='col-md-3 control-label'" +
        "        for='{operation_id}-command'>Stress Command</label>  " +
        "        <div class='col-md-9'>" +
        "          <input id='{operation_id}-command' type='text'" +
        "                 class='form-control input-md command' value='{cmd}' required=''></input>" +
        "        </div>" +
        "      </div>" +
        "            " +
        "      <div class='panel-group stress' id='{operation_id}-stress-variations'>" +
        "        <div class='panel col-md-12'>" +
        "          <div class='panel-heading'>" +
        "           <a data-toggle='collapse' data-parent='#{operation_id}-stress-variations' href='#{operation_id}-stress-variations-collapse'>Stress Variations </a><span class='glyphicon glyphicon-chevron-down'></span>" +
        "          </div>" +
        "          <div id='{operation_id}-stress-variations-collapse' class='panel-collapse collapse'>" +
        "           <table class='col-md-12'>" +
        "            <tr><td>" +
        "              <input disabled=disabled type='checkbox' class='kill-nodes' id='{operation_id}-kill-nodes' value='1'>" +
        "              Kill</td><td>" +
        "                <select disabled=disabled id='{revision_id}-kill-nodes-num' class='form-control kill-nodes-dropdown kill-nodes-num'>" +
        "                  <option value='1'> 1 </option>" +
        "                  <option value='2'> 2 </option>" +
        "                  <option value='3'> 3 </option>" +
        "                  <option value='4'> 4 </option>" +
        "                </select>" +
        "              </td><td>nodes after</td><td>" +
        "                 <input disabled=disabled id='{revision_id}-kill-nodes-delay' class='kill-nodes-delay' value='300'/> seconds" +
        "              </td>" +
        "            </tr><tr>" +
        "             <td>" +
        "              <input disabled=disabled type='checkbox' class='compact' id='{operation_id}-compact' value='1'>" +
        "              Major Compaction</td><td>" +
        "              </td><td>after</td><td>" +
        "                 <input disabled=disabled id='{revision_id}-kill-nodes-delay' class='kill-nodes-delay' value='300'/> seconds" +
        "              </td>" +
        "            </tr><tr>" +
        "             <td>" +
        "              <input disabled=disabled type='checkbox' class='bootstrap' id='{operation_id}-bootstrap' value='1'>" +
        "              Bootstrap</td><td>" +
        "                <select disabled=disabled id='{revision_id}-kill-nodes'  class='form-control kill-nodes-dropdown kill-nodes'>" +
        "                  <option value='1'> 1 </option>" +
        "                  <option value='2'> 2 </option>" +
        "                  <option value='3'> 3 </option>" +
        "                  <option value='4'> 4 </option>" +
        "                </select>" +
        "              </td><td>nodes after</td><td>" +
        "                 <input disabled=disabled id='{revision_id}-kill-nodes-delay' class='kill-nodes-delay' value='300'/> seconds" +
        "              </td></tr>" +
        "           </table>" +
        "	    </div>" +
        "        </div>" +

        "      </div>" +
        "     </div>";

    var newDiv = $(template.format({operation:schedule.n_operations, operation_id:operation_id, cmd:cmd}));
    if (animate)
        newDiv.hide();
    $("#schedule-operations").append(newDiv);
    $("#"+operation_id+"-type").change(function(){
        if (this.value == 'stress') {
            $("#"+operation_id+" div.stress").show();
        } else {
            $("#"+operation_id+" div.stress").hide();
        }
    }).val(operation).change();
    if (animate)
        newDiv.slideDown();

    //Remove operation handler:
    $("#remove-"+operation_id).click(function() {
        $("div#"+operation_id).slideUp(function() {
            this.remove();
        });
    });
    
};

var createJob = function() {
    //Parse the form elements and schedule job to run.
    var job = {
        title: $("#testname").val(),
        description: $("#description").val(),
        cluster: $("#cluster").val(),
        num_nodes: $("#numnodes").val(),
    }
    
    //Revisions:
    job.revisions = [];
    $("#schedule-revisions div.revision").each(function(i, revision) {
        revision = $(revision);
        job.revisions[i] = {
            revision: revision.find(".refspec").val(),
            label: revision.find(".revision-label").val() ? revision.find(".revision-label").val() : null,
            yaml: revision.find(".yaml").val(),
            env: revision.find(".env-vars").val(),
            java_home: revision.find(".jvm-select").val(),
            options: {'use_vnodes': revision.find(".options-vnodes").is(":checked") }
        };
    });

    //Operations:
    job.operations = [];
    $("#schedule-operations div.operation").each(function(i, operation) {
        operation = $(operation);
        job.operations[i] = {
            operation: operation.find(".type").val(),
        };
        if (job.operations[i]['operation'] === 'stress') {
            job.operations[i]['command'] = operation.find(".command").val();
        }
    });

    return JSON.stringify(job);
}

var cloneExistingJob = function(job_id) {
    $.get("/api/tests/id/" + job_id, function(job) {
        test = job['test_definition'];
        $("input#testname").val(test['title']); 
        $("textarea#description").val(test['description']);
        $("select#cluster").val(test['cluster']);
        $("select#numnodes").val(test['num_nodes']);
        //Revisions:
        $.each(test['revisions'], function(i, revision) {
            addRevisionDiv(false);
            var rev = i + 1;
            $("#revision-"+rev+"-refspec").val(revision['revision']);
            $("#revision-"+rev+"-label").val(revision['label']);
            $("#revision-"+rev+"-yaml").val(revision['yaml']);
            $("#revision-"+rev+"-env-vars").val(revision['env']);
            $("#revision-"+rev+"-options-vnodes").attr("checked", revision['options']['use_vnodes'])
        });
        //Operations:
        $.each(test['operations'], function(i, operation) {
            addOperationDiv(false, operation['operation'], operation['command']);            
        });
   });
}

$(document).ready(function() {
    //Add revision button callback:
    $('button#add-revision').click(function(e) {
        addRevisionDiv(true);
        e.preventDefault();
    });

    //Add operation button callback:
    $('button#add-operation').click(function(e) {
        addOperationDiv(true, 'stress');
        e.preventDefault();
    });

    //Refresh jvm list on cluster selection
    $('#cluster').change(function(e) {
        var cluster = $('#cluster').val();
        $.get('/api/clusters/'+cluster, function(data) {
            //Remember the current jvm selections:
            var current_jvm_selections = [];
            $(".jvm-select").each(function(i, e) {
                current_jvm_selections[i] = $(e).val();
            });
            //Clear out the jvm lists and fetch new one:
            $(".jvm-select").empty();
            if(data.jvms==null) {
                alert("Warning: cluster '"+ cluster+ "' has no JVMs defined.");
                return;
            }
            $.each(data.jvms, function(jvm, path) {
                $(".jvm-select").append($("<option value='"+path+"'>"+jvm+"</option>"));
            });
            //Try to set the one we had from before:
            $(".jvm-select").each(function(i, e) {
                if (current_jvm_selections[i] != null) {
                    $(e).val(current_jvm_selections[i]);
                }
                if ($(e).val() == null) {
                    $(e).find("option:first-child").attr("selected", "selected");
                    alert("Warning - cluster JVM selection changed")
                }
            });

        });
    });
    //$("#cluster").change();

    query = parseUri(location).queryKey;
    if (query.clone != undefined) {
        //Clone an existing job specified in the query string:
        cloneExistingJob(query.clone);
    } else {
        //Create a new job from scratch:
        addRevisionDiv(false);
        addOperationDiv(false, 'stress', 'write n=19000000 -rate threads=50');
        addOperationDiv(false, 'stress', 'read n=19000000 -rate threads=50');
    }

    //Validate form and submit:
    $("form#schedule-test").submit(function(e) {
        var job = createJob();
        console.log(job);
        $.ajax({
            type: "POST",
            url: "/api/tests/schedule",
            data: job,
            contentType: 'application/json'
        }).success(function(data) {
            //Redirect to the status URL for the job.
            //Use replace so we don't ever go back to the schedule page if
            //we click back, since the form will have lost it's state.
            window.location.replace(data['url']);
        }).error(function(data) {
            console.log(data);
            alert("error: "+data.status+" "+data.statusText+" "+data.responseText);
        });
        e.preventDefault();
    });
});
