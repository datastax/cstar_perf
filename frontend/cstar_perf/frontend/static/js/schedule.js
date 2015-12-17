schedule = {
    n_revisions: 0,
    n_operations: 0
};

var addRevisionDiv = function(animate){
    schedule.n_revisions++;
    var revision_id = 'revision-'+schedule.n_revisions;
    var template = "<div id='{revision_id}' class='revision'><legend>Test Revisions<a id='remove-{revision_id}' class='pull-right remove-revision'><span class='glyphicon" +
        "                  glyphicon-remove'></span></a></legend>" +

        "      <div class='form-group product-select-div' style='display:block'>" +
        "        <label class='col-md-4 control-label' for='{revision_id}-product'>Product</label>" +
        "        <div class='col-md-8'>" +
        "          <select id='{revision_id}-product' " +
        "                  class='product-select form-control' required onchange='maybe_show_dse_yaml_div(this.id, this.value);'>" +
        "            <option value='cassandra'>cassandra</option>" +
        "          </select>" +
        "        </div>" +
        "      </div>" +

        "      <div class='form-group'>" +
        "        <label class='col-md-4 control-label' for='{revision_id}-refspec'>Revision</label>  " +
        "        <div class='col-md-8'>" +"" +
        "          <input id='{revision_id}-refspec' type='text' placeholder='Git branch, tag, commit id or DSE version' class='refspec form-control input-md' required>" +
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
        "      <div class='form-group dse-yaml-settings-div' style='display:block' id='{revision_id}-dse_yaml_div'>" +
        "        <label class='col-md-4 control-label' for='{revision_id}-dse_yaml'>dse.yaml</label>" +
        "        <div class='col-md-8'>" +
        "          <textarea class='form-control dse_yaml' id='{revision_id}-dse_yaml'" +
        "          placeholder='Any dse.yaml options you want that differ from the default settings for the chosen cluster.'></textarea>" +
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
        "      <div class='form-group' style='display:block' id='{revision_id}-spark_env_div'>" +
        "        <label class='col-md-4 control-label' for='{revision_id}-spark-env-vars'>Spark Environment Script</label>" +
        "        <div class='col-md-8'>" +
        "          <textarea class='form-control spark-env-vars' id='{revision_id}-spark-env-vars'" +
        "          placeholder='Spark Environment settings to append to spark-env.sh'></textarea>" +
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
        "        <label class='col-md-4 control-label' for='{revision_id}-token-allocation'>Token Allocation</label>" +
        "        <div class='col-md-8'>" +
        "          <select id='{revision_id}-token-allocation' " +
        "                  class='token-allocation-select form-control' required>" +
        "             <option value='non-vnodes'>non-vnodes</option>"+
        "             <option value='random' selected='selected'>vnodes (random)</option>"+
        "             <option value='static-random'>vnodes (static-random)</option>"+
        "             <option value='static-algorithmic'>vnodes (static-algorithmic)</option>"+
        "          </select>" +
        "          <div class='checkbox'>" +
        "            <input type='checkbox' class='options-yourkit' id='{revision_id}-options-yourkit'/>" +
        "            <label for='{revision_id}-options-yourkit'>Enable yourkit profiling</label>" +
        "	  </div>" +
        "        </div>" +
        "      </div>" +
        "" +
        "      <div class='form-group' style='display:block' id='{revision_id}-dse_node_type_div'>" +
        "        <label class='col-md-4 control-label' for='{revision_id}-dse_node_type'>DSE Node Type</label>" +
        "        <div class='col-md-8'>" +
        "          <select id='{revision_id}-dse_node_type' class='dse_node_type form-control'>" +
        "                  <option value='cassandra'>Cassandra</option>" +
        "                  <option value='search'>Search</option>" +
        "                  <option value='search-analytics'>SearchAnalytics</option>" +
        "                  <option value='spark'>Spark</option>" +
        "                  <option value='spark-hadoop'>SparkHadoop</option>" +
        "                  <option value='hadoop'>Hadoop</option>" +
        "          </select>" +
        "        </div>" +
        "      </div>" +
        "    </div>";
    var newDiv = $(template.format({revision:schedule.n_revisions, revision_id:revision_id}));
    if (animate)
        newDiv.hide();
    $("#schedule-revisions").append(newDiv);
    if (animate)
        newDiv.slideDown();

    // Update cluster. Will update all revisions.
    update_cluster_selections();
    update_cluster_options();

    // initially hide the dse yaml & dse node type settings
    $("#" + revision_id + "-dse_yaml_div").hide();
    $("#" + revision_id + "-dse_node_type_div").hide();
    $("#" + revision_id + "-spark_env_div").hide();

    //Remove revision handler:
    $("#remove-"+revision_id).click(function() {
        $("div#"+revision_id).slideUp(function() {
            this.remove();
        });
    });
};

var addOperationDiv = function(animate, operationDefaults){
    schedule.n_operations++;
    var operation_id = 'operation-'+schedule.n_operations;
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
        "          <textarea id='{operation_id}-command' type='text'" +
        "                 class='form-control input-md command-stress' required=''>{command_stress}</textarea>" +
        "        </div>" +
        "      </div>" +
        "      <div class='form-group type stress'>" +
        "        <label class='col-md-3 control-label'" +
        "        for='{operation_id}-stress-revision'>Stress Revision</label>  " +
        "        <div class='col-md-9'>" +
        "          <input id='{operation_id}-stress-revision' type='text'" +
        "                 class='form-control input-md stress-revision' value='{stress_revision}' required=''></input>" +
        "        </div>" +
        "      </div>" +
        "      " +
        "      <div class='form-group type nodetool'>" +
        "        <label class='col-md-3 control-label'" +
        "        for='{operation_id}-command'>Nodetool Command</label>  " +
        "        <div class='col-md-9'>" +
        "          <input id='{operation_id}-command' type='text'" +
        "                 class='form-control input-md command-nodetool' value='{command_nodetool}' required=''>" +
        "        </div>" +
        "      </div>" +
        "      <div class='form-group nodes nodetool'>" +
        "        <label class='col-md-3 control-label'" +
        "            for='{operation_id}-command'>Nodes</label>  " +
        "        <div class='col-md-9'>" +
        "          <select multiple id='{operation_id}-nodes' type='text'" +
        "                 class='form-control input-md nodes-nodetool node-select'>" +
        "          </select>" +
        "        </div>" +
        "      </div>" +
        "      " +
        "      <div class='form-group type cqlsh'>" +
        "        <label class='col-md-3 control-label'" +
        "        for='{operation_id}-command'>CQL script</label>  " +
        "        <div class='col-md-9'>" +
        "          <textarea id='{operation_id}-script' type='text'" +
        "                 class='form-control input-md script-cqlsh' required=''>{script_cqlsh}</textarea>" +
        "        </div>" +
        "      </div>" +
        "      <div class='form-group nodes cqlsh'>" +
        "        <label class='col-md-3 control-label'" +
        "            for='{operation_id}-command'>Nodes</label>  " +
        "        <div class='col-md-9'>" +
        "          <select id='{operation_id}-nodes' type='text'" +
        "                 class='form-control input-md node-cqlsh node-select'>" +
        "          </select>" +
        "        </div>" +
        "      </div>" +
        "            " +
        "      <div class='form-group type bash'>" +
        "        <label class='col-md-3 control-label'" +
        "        for='{operation_id}-command'>Bash script</label>  " +
        "        <div class='col-md-9'>" +
        "          <textarea id='{operation_id}-script' type='text'" +
        "                 class='form-control input-md script-bash' required=''>{script_bash}</textarea>" +
        "        </div>" +
        "      </div>" +
        "      <div class='form-group nodes bash'>" +
        "        <label class='col-md-3 control-label'" +
        "            for='{operation_id}-command'>Nodes</label>  " +
        "        <div class='col-md-9'>" +
        "          <select multiple id='{operation_id}-nodes' type='text'" +
        "                 class='form-control input-md nodes-bash node-select'>" +
        "          </select>" +
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
        "   </div>" +
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
        "     </div>" +
        "        </div>" +

        "      </div>" +
        "     </div>";

    var operationDefaults = operationDefaults || {};
    newOperation = {
        operationType: operationDefaults.operation || "stress",
        operation: schedule.n_operations,
        operation_id: operation_id,
    };
    if (newOperation.operationType === 'stress' && operationDefaults.command) {
        newOperation.command_stress = operationDefaults.command
    } else {
        newOperation.command_stress = "write n=19M -rate threads=50";
    }
    if (newOperation.operationType === 'stress' && operationDefaults.stress_revision) {
        newOperation.stress_revision = operationDefaults.stress_revision;
    } else {
        newOperation.stress_revision = "apache/trunk";
    }
    if (newOperation.operationType === 'nodetool' && operationDefaults.command) {
        newOperation.command_nodetool = operationDefaults.command;
    } else {
        newOperation.command_nodetool = "status";
    }
    if (newOperation.operationType === 'cqlsh' && operationDefaults.script) {
        newOperation.script_cqlsh = operationDefaults.script;
    } else {
        newOperation.script_cqlsh = "DESCRIBE TABLES;";
    }
    if (newOperation.operationType === 'bash' && operationDefaults.script) {
        newOperation.script_bash = operationDefaults.script;
    } else {
        newOperation.script_bash = "df -h";
    }

    var newDiv = $(template.format(newOperation));
    if (animate)
        newDiv.hide();
    $("#schedule-operations").append(newDiv);
    $("#"+operation_id+"-type").change(function(){
        var validOperations = ['stress', 'nodetool', 'cqlsh', 'bash'];
        if (validOperations.indexOf(this.value) < 0) {
            console.log(this.value + ' not a valid selection')
        }
        for (var i = 0; i < validOperations.length; i++) {
            var op = validOperations[i];
            var selected = $("#"+operation_id+" div."+op);
            if (op === this.value) {
                selected.show();
            } else {
                selected.hide();
            }
        }
    }).val(newOperation.operationType).change();
    if (animate)
        newDiv.slideDown();

    //Remove operation handler:
    $("#remove-"+operation_id).click(function() {
        $("div#"+operation_id).slideUp(function() {
            this.remove();
        });
    });

    //Check wait_for_compaction box:
    if (operationDefaults.wait_for_compaction === false) {
        $("#"+operation_id+"-wait-for-compaction").prop("checked", false);
    }
    update_cluster_options(operation_id, operationDefaults)
};

var maybe_show_dse_yaml_div = function(id, value) {
    dse_yaml_div_id = id.replace("product", "dse_yaml_div")
    dse_node_type_div_id = id.replace("product", "dse_node_type_div")
    spark_env_div_id = id.replace("product", "spark_env_div")
    if (value == "dse") {
        $("#" + dse_yaml_div_id).show();
        $("#" + dse_node_type_div_id).show();
        $("#" + spark_env_div_id).show();
    } else {
        $("#" + dse_yaml_div_id).hide();
        $("#" + dse_node_type_div_id).hide();
        $("#" + spark_env_div_id).hide();
    }
}

var createJob = function() {
    //Parse the form elements and schedule job to run.
    var job = {
        title: $("#testname").val(),
        testseries: $("#testseries").val(),
        description: $("#description").val(),
        cluster: $("#cluster").val(),
        num_nodes: $("#numnodes").val()
    };

    //Revisions:
    job.revisions = [];
    $("#schedule-revisions div.revision").each(function(i, revision) {
        revision = $(revision);
        job.revisions[i] = {
            product: revision.find(".product-select").val(),
            revision: revision.find(".refspec").val(),
            label: revision.find(".revision-label").val() ? revision.find(".revision-label").val() : null,
            yaml: revision.find(".yaml").val(),
            dse_yaml: revision.find(".dse_yaml").val(),
            env: revision.find(".env-vars").val(),
            spark_env: revision.find(".spark-env-vars").val(),
            java_home: revision.find(".jvm-select").val(),
            dse_node_type: revision.find(".dse_node_type").val(),
            options: {
                'use_vnodes': revision.find(".token-allocation-select").val() != 'non-vnodes',
                'token_allocation': revision.find(".token-allocation-select").val()
            },
            yourkit_profiler: revision.find(".options-yourkit").is(":checked")
        };
    });

    //Operations:
    job.operations = [];
    $("#schedule-operations div.operation").each(function(i, operation) {
        operation = $(operation);
        var op = operation.find(".type").val()
        var jobSpec = {
            operation: op,
        };
        if (op === 'stress') {
            jobSpec['command'] = operation.find(".command-stress").val();
            jobSpec['stress_revision'] = operation.find(".stress-revision").val();
        }
        if (op === "nodetool") {
            jobSpec['command'] = operation.find(".command-nodetool").val();
            jobSpec['nodes'] = operation.find(".nodes-nodetool").val() || [];
        }
        if (op === "cqlsh") {
            jobSpec['script'] = operation.find(".script-cqlsh").val();
            jobSpec['node'] = operation.find(".node-cqlsh").val() || [];
        }
        if (op === "bash") {
            jobSpec['script'] = operation.find(".script-bash").val();
            jobSpec['nodes'] = operation.find(".nodes-bash").val();
        }
        jobSpec['wait_for_compaction'] = operation.find(".wait-for-compaction").is(":checked");
        job.operations[i] = jobSpec;
    });

    return JSON.stringify(job);
};

//Get test definition link:
var show_job_json = function() {
    var json = JSON.stringify(JSON.parse(createJob()), undefined, 2);
    $("#schedule-test").hide();
    $("#container").append($("<pre>").append(json));
    $("#get_job_json").remove();
    if (query.clone != undefined) {
        query.show_json = true;
        updateURLBar(query);
    }
};

var cloneExistingJob = function(job_id) {
    $.get("/api/tests/id/" + job_id, function(job) {
        test = job['test_definition'];
        $("input#testname").val(test['title']);
        $("inpur#testseries").val(test['testseries']);
        $("textarea#description").val(test['description']);
        $("select#cluster").val(test['cluster']);
        $("select#numnodes").val(test['num_nodes']);
        //Revisions:
        $.each(test['revisions'], function(i, revision) {
            addRevisionDiv(false);
            var rev = i + 1;
            $("#revision-"+rev+"-refspec").val(revision['revision']);
            $("#revision-"+rev+"-product").val(revision['product']);
            $("#revision-"+rev+"-label").val(revision['label']);
            $("#revision-"+rev+"-yaml").val(revision['yaml']);
            $("#revision-"+rev+"-dse_yaml").val(revision['dse_yaml']);
            $("#revision-"+rev+"-dse_node_type").val(revision['dse_node_type']);
            $("#revision-"+rev+"-env-vars").val(revision['env']);
            $("#revision-"+rev+"-spark-env-vars").val(revision['spark_env']);
            if (revision['options'] == undefined) {
                revision['options'] = {};
            }
            $("#revision-"+rev+"-options-yourkit").prop("checked", revision['yourkit_profiler']);
            update_cluster_options();
            update_cluster_selections(function(){
                $("#revision-"+rev+"-jvm").val(revision['java_home']);
                $("#revision-"+rev+"-product").val(revision['product']);
                $("#revision-"+rev+"-token-allocation").val(revision['options']['token_allocation']);
                if (revision['product'] == 'dse') {
                    $("#revision-"+rev+"-dse_yaml_div").show();
                    $("#revision-"+rev+"-dse_node_type_div").show();
                    $("#revision-"+rev+"-spark_env_div").show();
                }
            });

        });
        //Operations:
        $.each(test['operations'], function(i, operation) {
            addOperationDiv(false, operation);
        });

        query = parseUri(location).queryKey;
        if (query.show_json != undefined) {
            show_job_json();
        }

   });
};

var update_cluster_options = function(operation_id, operation_defaults, callback) {
    operation_defaults = operation_defaults || {};
    var defaultNodeSpec = operation_defaults.nodes;
    if (!defaultNodeSpec) {
        if (operation_defaults.node) {
            var defaultNodeSpec = [operation_defaults.node];
        }
    }

    var changeDivs;
    if (operation_id == null) {
        changeDivs = $(".node-select");
    } else {
        changeDivs = $("div#" + operation_id).find(".node-select");
    }

    var cluster = $('#cluster').val();
    $.get('/api/clusters/'+cluster, function(data) {
        //Clear out the node lists and fetch new one:
        changeDivs.empty();
        if(data.nodes==null) {
            alert("Warning: cluster '"+ cluster+ "' has no nodes defined.");
            return;
        }
        $.each(data.nodes, function(node, path) {
            var newNodeOption = "<option value='"+path+"'";
            // if the node's name was selected in the default operation, select it here
            if (defaultNodeSpec) {
                for (i = 0; i < defaultNodeSpec.length; i++) {
                    if (defaultNodeSpec[i] === path) {
                        newNodeOption += " selected";
                        break;
                    }
                }
            } else {
                newNodeOption += " selected";
            }
            newNodeOption += ">"+path+"</option>";
            changeDivs.append($(newNodeOption));
        });
        if (callback != null)
            callback();
    });
};

var update_cluster_selections = function(callback) {
    var cluster = $('#cluster').val();
    $.get('/api/clusters/'+cluster, function(data) {

        $('#numnodes').val(data.nodes.length);

        var default_selections = update_select_with_values(".jvm-select", data.jvms, "JVM");
         // try to select jvm 1.8 for all default selections
        $(".jvm-select").each(function(i, jvm) {
            if (default_selections[i]) {
                $("option", jvm).each(function(i, e) {
                    if ($(e).text().lastIndexOf("1.8.", 0) === 0) {
                        $(jvm).val($(e).val());
                    }
                });
            }
        });

        var product_options = {"cassandra": "cassandra"};
        $.each(data.additional_products, function(_, product) {
            product_options[product] = product;
        });
        update_select_with_values(".product-select", product_options, "Product");
        if (data.additional_products.length == 0) {
            $(".product-select-div").hide();
        } else {
            $(".product-select-div").show();
        }

        //Warn if the JVM option changed due to cluster switch:
        $(".jvm-select").each(function(i, e) {
            if ($(e).val() == null) {
                $(e).find("option:first-child").attr("selected", "selected");
                alert("Warning - cluster JVM selection changed")
            }
        });
        
        if (callback != null) {
            callback();
        }
    });
};

var updateURLBar = function(query) {
    //Update the URL bar with the current parameters:
    window.history.replaceState(null,null,parseUri(location).path + "?" + $.param(query));
};


$(document).ready(function() {
    //Add revision button callback:
    $('button#add-revision').click(function(e) {
        e.preventDefault();
        addRevisionDiv(true);
    });

    //Add operation button callback:
    $('button#add-operation').click(function(e) {
        e.preventDefault();
        addOperationDiv(true);
    });

    //Refresh jvm list on cluster selection
    $('#cluster').change(function(e) {
        update_cluster_selections();
        update_cluster_options();
    });

    query = parseUri(location).queryKey;
    if (query.clone != undefined) {
        //Clone an existing job specified in the query string:
        cloneExistingJob(query.clone);
    } else {
        //Create a new job from scratch:
        addRevisionDiv(false);
        addOperationDiv(false, {operation: 'stress', command: 'write n=19M -rate threads=50'});
        addOperationDiv(false, {operation: 'stress', command: 'read n=19M -rate threads=50'});
    }

    //Validate form and submit:
    $("form#schedule-test").submit(function(e) {
        e.preventDefault();
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
    });


    $("#get_job_json").click(function(e) {
        show_job_json()
        e.preventDefault();
    });

});
