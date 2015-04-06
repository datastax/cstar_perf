schedule = {
    n_revisions: 0,
    n_operations: 0
};

clusters = {};

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
        "                  class='jvm-select form-control' required>" +
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

var addOperationDiv = function(animate, operation, cmd, wait_for_compaction){
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
        "            <option value='nodetool'>nodetool</option>" +
        "            <option value='cqlsh'>cqlsh</option>" +
        "            <option value='bash'>bash</option>" +
        "          </select>" +
        "        </div>" +
        "      </div>" +
        "      " +
        "      <div class='form-group type stress'>" +
        "        <label class='col-md-3 control-label'" +
        "        for='{operation_id}-command'>Stress Command</label>  " +
        "        <div class='col-md-9'>" +
        "          <input id='{operation_id}-command' type='text'" +
        "                 class='form-control input-md command-stress' value='{cmd}' required=''></input>" +
        "        </div>" +
        "      </div>" +
        "      <div class='form-group nodes stress'>" +
        "        <label class='col-md-3 control-label'" +
        "            for='{operation_id}-command'>Nodes</label>  " +
        "        <div class='col-md-9'>" +
        "          <select multiple id='{operation_id}-nodes'" +
        "                 class='form-control input-md command-stress-nodes'></select>" +
        "        </div>" +
        "      </div>" +
        "      " +
        "      <div class='form-group type nodetool'>" +
        "        <label class='col-md-3 control-label'" +
        "        for='{operation_id}-command'>Nodetool Command</label>  " +
        "        <div class='col-md-9'>" +
        "          <input id='{operation_id}-command' type='text'" +
        "                 class='form-control input-md command-nodetool' value='' required=''></input>" +
        "        </div>" + 
        "      </div>" +
        "      <div class='form-group nodes nodetool'>" +
        "        <label class='col-md-3 control-label'" +
        "            for='{operation_id}-command'>Nodes</label>  " +
        "        <div class='col-md-9'>" +
        "          <select multiple id='{operation_id}-nodes'" +
        "                 class='form-control input-md command-nodetool-nodes'></select>" +
        "        </div>" +
        "      </div>" +
        "      " +
        "      <div class='form-group type cqlsh'>" +
        "        <label class='col-md-3 control-label'" +
        "        for='{operation_id}-command'>CQL script</label>  " +
        "        <div class='col-md-9'>" +
        "          <textarea id='{operation_id}-script' type='text'" +
        "                 class='form-control input-md script-cqlsh' required=''></textarea>" +
        "        </div>" +
        "      </div>" +
        "      <div class='form-group nodes cqlsh'>" +
        "        <label class='col-md-3 control-label'" +
        "            for='{operation_id}-command'>Node</label>  " +
        "        <div class='col-md-9'>" +
        "          <select id='{operation_id}-nodes'" +
        "                 class='form-control input-md command-cqlsh-nodes'></select>" +
        "        </div>" +
        "      </div>" +
        "            " +
        "      <div class='form-group type bash'>" +
        "        <label class='col-md-3 control-label'" +
        "        for='{operation_id}-command'>BASH script</label>  " +
        "        <div class='col-md-9'>" +
        "          <textarea id='{operation_id}-script' type='text'" +
        "                 class='form-control input-md script-bash' required=''></textarea>" +
        "        </div>" +
        "      </div>" +
        "      <div class='form-group nodes bash'>" +
        "        <label class='col-md-3 control-label'" +
        "            for='{operation_id}-command'>Nodes</label>  " +
        "        <div class='col-md-9'>" +
        "          <select multiple id='{operation_id}-nodes'" +
        "                 class='form-control input-md command-bash-nodes'></select>" +
        "        </div>" +
        "      </div>" +
        "            " +
        "      <div class='form-group'>" +
        "        <label class='col-md-3 control-label'" +
        "        for='{operation_id}-options'>Options</label>" +
        "        <div class='col-md-8'>" +
        "          <div class='checkbox'>" +
        "            <input type='checkbox' class='wait-for-compaction' id='{operation_id}-wait-for-compaction' checked='checked'>" +
        "            <label for='{operation_id}-wait-for-compaction'>" +
        "              Wait for compactions before next operation" +
        "            </label>" +
        "	  </div>" +
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
    fillOperationNodesList();
    $("#"+operation_id+"-type").change(function(){
        $("#"+operation_id+" div.type").hide();
        $("#"+operation_id+" div.nodes").hide();
        $("#"+operation_id+" div."+this.value).show();
    }).val(operation).change();
    if (animate)
        newDiv.slideDown();

    //Remove operation handler:
    $("#remove-"+operation_id).click(function() {
        $("div#"+operation_id).slideUp(function() {
            this.remove();
        });
    });

    //Check wait_for_compaction box:
    if (wait_for_compaction === false) {
        $("#"+operation_id+"-wait-for-compaction").prop("checked", false);
    }
};

var fillOperationNodesList = function() {
    var cluster = $("#cluster").val();
    console.log("Filling node lists for " + cluster);
    $(".nodes select").children("option").remove();
    $(".nodes select").each(function(i, e) {
        //Fill in available nodes to run commands on:
        $.each(clusters[cluster].nodes, function(i, node) {
            $(e).append($("<option value='"+node+"'>"+node+"</option>"));
        });
    });
    $(".nodes.stress select, .nodes.bash select, .nodes.nodetool select").each(function(i, e) {
        $(e).multiselect({
            includeSelectAllOption: true,
            selectAllValue: 'ALL'
        });
        $(e).multiselect('selectAll', false);
        $(e).multiselect('rebuild');
        $(e).multiselect('refresh');
    });
}


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
        var operation = $(operation);
        var type = operation.find(".type").val();
        job.operations[i] = {
            operation: type,
        };
        if (_.contains(['stress','nodetool'], type)) {
            job.operations[i]['command'] = operation.find(".command-"+type).val();
        } else if (_.contains(['cqlsh','bash'], type)) {
            job.operations[i]['script'] = operation.find(".script-"+type).val().split("\n");
        }
        //Gather nodes to run operation on:
        if (_.contains(['stress', 'nodetool','bash'], type)) {
            job.operations[i]['nodes'] = [];
            operation.find('.nodes.'+type+" :selected").each(function(j, selected){
                job.operations[i]['nodes'][j] = $(selected).text();
            });
        } else if (type === 'cqlsh') {
            job.operations[i]['node'] = operation.find('.nodes.'+type+" :selected").text();
        }
        job.operations[i]['wait_for_compaction'] = operation.find(".wait-for-compaction").is(":checked");
    });

    return JSON.stringify(job);
}

//Get test definition link:
var show_job_json = function() {
    var json = JSON.stringify(JSON.parse(createJob()), undefined, 2);
    $("#schedule-test").hide();
    $("#container").append($("<pre id='job_json'>").append(json));
    $("#get_job_json").hide();
    if (query.clone != undefined) {
        query.show_json = true;
        updateURLBar(query);
    }

    history.pushState(null, null, '/schedule/json');
    window.addEventListener("popstate", function(e) {
        $("#job_json").remove();
        $("#get_job_json").show();
        $("#schedule-test").show();        
    });

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
            if (revision['options'] == undefined) {
                revision['options'] = {};
            }
            $("#revision-"+rev+"-options-vnodes").prop("checked", revision['options']['use_vnodes'])
            update_jvm_selections(function(){
                $("#revision-"+rev+"-jvm").val(revision['java_home']);
            });

        });
        //Operations:
        $.each(test['operations'], function(i, operation) {
            addOperationDiv(false, operation['operation'], operation['command'], operation['wait_for_compaction']);
        });

        query = parseUri(location).queryKey;
        if (query.show_json != undefined) {
            show_job_json();
        }

   });
}

var update_jvm_selections = function(callback) {
    var cluster = clusters[$('#cluster').val()];
    //Remember the current jvm selections:
    var current_jvm_selections = [];
    $(".jvm-select").each(function(i, e) {
        current_jvm_selections[i] = $(e).val();
    });
    //Clear out the jvm lists and fetch new one:
    $(".jvm-select").empty();
    if(cluster.jvms==null) {
        alert("Warning: cluster '"+ cluster.name + "' has no JVMs defined.");
        return;
    }
    $.each(cluster.jvms, function(jvm, path) {
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
    if (callback != null) 
        callback();
}

var updateURLBar = function(query) {
    //Update the URL bar with the current parameters:
    window.history.replaceState(null,null,parseUri(location).path + "?" + $.param(query));
};


$(document).ready(function() {
    //Get cluster information:
    $.getJSON('/api/clusters', function(data) {
        clusters = data['clusters'];
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

        //Refresh node and jvm lists on cluster selection
        $('#cluster').change(function(e) {
            update_jvm_selections();
            fillOperationNodesList();
        });

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


        $("#get_job_json").click(function(e) {
            show_job_json()
            e.preventDefault();
        });
    });

});
