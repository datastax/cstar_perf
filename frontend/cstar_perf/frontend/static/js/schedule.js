schedule = {
    n_revisions: 0,
    n_operations: 0
};

var addRevisionDiv = function(animate){
    schedule.n_revisions++;
    var revision_id = 'revision-'+schedule.n_revisions;
    $.get('/static/templates/schedule_revisions.html', function(template) {
        var newDiv = $(Mustache.render(template, {revision:schedule.n_revisions, revision_id: revision_id}));
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
        $("#remove-" + revision_id).click(function () {
            $("div#" + revision_id).slideUp(function () {
                this.remove();
            });
        });

        maybe_show_dse_operations();
    });
};

var addOperationDiv = function(animate, operationDefaults){
    schedule.n_operations++;
    var operation_id = 'operation-'+schedule.n_operations;
    var operationDefaults = operationDefaults || {};
    var newOperation = {
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
    if (newOperation.operationType === 'ctool' && operationDefaults.command) {
        newOperation.command_ctool = operationDefaults.command;
    } else {
        newOperation.command_ctool = "info cstar_perf";
    }
    if (newOperation.operationType === 'spark_cassandra_stress' && operationDefaults.script) {
        newOperation.script_spark_cassandra_stress = operationDefaults.script;
    } else {
        newOperation.script_spark_cassandra_stress = "-o 10000 -y 1000 -p 1000 writeperfrow";
    }
    if (newOperation.operationType === 'dsetool' && operationDefaults.script) {
        newOperation.script_dsetool = operationDefaults.script;
    } else {
        newOperation.script_dsetool = "status";
    }
    if (newOperation.operationType === 'dse' && operationDefaults.script) {
        newOperation.script_dse = operationDefaults.script;
    } else {
        newOperation.script_dse = "-v";
    }

    $.get('/static/templates/schedule_operations.html', function(template) {
        var newDiv = $(Mustache.render(template, newOperation));
        if (animate) {
            newDiv.hide();
        }
        $("#schedule-operations").append(newDiv);
        $("#" + operation_id + "-type").change(function () {
            var validOperations = ['stress', 'nodetool', 'cqlsh', 'bash', 'spark_cassandra_stress', 'ctool', 'dsetool', 'dse'];
            if (validOperations.indexOf(this.value) < 0) {
                console.log(this.value + ' not a valid selection')
            }
            for (var i = 0; i < validOperations.length; i++) {
                var op = validOperations[i];
                var selected = $("#" + operation_id + " div." + op);
                if (op === this.value) {
                    selected.show();
                } else {
                    selected.hide();
                }
            }
        }).val(newOperation.operationType).change();
        if (animate) {
            newDiv.slideDown();
        }

        maybe_show_dse_operations();

        //Remove operation handler:
        $("#remove-" + operation_id).click(function () {
            $("div#" + operation_id).slideUp(function () {
                this.remove();
            });
        });

        //Check wait_for_compaction box:
        if (operationDefaults.wait_for_compaction === false) {
            $("#" + operation_id + "-wait-for-compaction").prop("checked", false);
        }
        update_cluster_options(operation_id, operationDefaults)
    });
};

var maybe_show_dse_operations = function() {
    // show DSE-related operations if all product values are set to DSE.
    // if there's only one product set to Cassandra, then don't show it.
    var show_dse_operation = true;
    for (var i = 1; i <= $('.product-select').length; i++) {
        if ($('#revision-' + i + '-product').val() == "cassandra") {
            show_dse_operation = false;
            break;
        }
    }
    var operation_type = ['spark_cassandra_stress', 'dsetool', 'dse'];
    for (var idx = 0; idx < operation_type.length; idx++) {
        var op = operation_type[idx];
        for (var i = 1; i <= $('.operation-type').length; i++) {
            var id = "#operation-" + i + "-" + op + "_select";
            if (show_dse_operation == true) {
                $(id).show();
            } else {
                // if a DSE operation is selected, select 'stress'
                // before hiding the DSE op from the dropdown list
                if ($("#operation-" + i + "-type").val() == op) {
                    $("#operation-" + i + "-type").val("stress").change();
                }
                $(id).hide()
            }
        }
    }
};

var maybe_show_dse_options = function(id, value) {
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
    maybe_show_dse_operations();
};

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
            yourkit_profiler: revision.find(".options-yourkit").is(":checked"),
            debug_logging: revision.find(".options-debug-logging").is(":checked")
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
        if (op === "ctool") {
            jobSpec['command'] = operation.find(".command-ctool").val();
        }
        if (op === "cqlsh") {
            jobSpec['script'] = operation.find(".script-cqlsh").val();
            jobSpec['node'] = operation.find(".node-cqlsh").val() || [];
        }
        if (op === "bash") {
            jobSpec['script'] = operation.find(".script-bash").val();
            jobSpec['nodes'] = operation.find(".nodes-bash").val();
        }
        if (op === "spark_cassandra_stress") {
            jobSpec['script'] = operation.find(".script-spark-cassandra-stress").val();
        }
        if (op === "dsetool") {
            jobSpec['script'] = operation.find(".script-dsetool").val();
            jobSpec['nodes'] = operation.find(".nodes-dsetool").val();
        }
        if (op === "dse") {
            jobSpec['script'] = operation.find(".script-dse").val();
            jobSpec['node'] = operation.find(".node-dse").val();
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
        $("input#testseries").val(test['testseries']);
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
            $("#revision-"+rev+"-options-debug-logging").prop("checked", revision['debug_logging']);
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
