var drawGraph = function() {

    $("body").append($("<div id='loading_indicator'></div>"));

    $("svg").remove();

    //Dataset and metric to draw is passed via query option:
    query = parseUri(location).queryKey;
    query.stats = unescape(query.stats);
    stats_db = '/tests/artifacts/' + query.stats + '/stats';
    var metric = query.metric;
    var operation = query.operation;
    var smoothing = query.smoothing;
    var show_aggregates = query.show_aggregates;

    xmin = query.xmin;
    xmax = query.xmax;
    ymin = query.ymin;
    ymax = query.ymax;

    //Stress metrics, depend on the version of stress used.
    //Here are the latest trunk metrics:
    var stress_trunk_metrics = [
        'total_ops',
        'op_rate',
        'key_rate',
        'row_rate',
        'mean',
        'med',
        '95th_latency',
        '99th_latency',
        '99.9th_latency',
        'max_latency',
        'elapsed_time',
        'stderr',
        'errors',
        'gc_count',
        'gc_max_ms',
        'gc_sum_ms',
        'gc_sdv_ms',
        'gc_mb'
    ];

    //Stress metrics from cassandra 2.1
    var stress_21_metrics = [
        'total_ops',
        'adj_row_rate',
        'op_rate',
        'key_rate',
        'row_rate',
        'mean',
        'med',
        '95th_latency',
        '99th_latency',
        '99.9th_latency',
        'max_latency',
        'elapsed_time',
        'stderr',
        'gc_count',
        'gc_max_ms',
        'gc_sum_ms',
        'gc_sdv_ms',
        'gc_mb'
    ]

    // Use the stats date to determine which stats to use :
    // Before May 13th 2014 - we use cassandra 2.1 metrics
    if (UUID_to_Date(query.stats) < 1431534892202) {
        var stress_metrics = stress_21_metrics;
    } else {
        var stress_metrics = stress_trunk_metrics;
    }
    
    var stress_metric_names = {
        'total_ops': 'Total operations',
        'op_rate': 'Operations / Second',
        'key_rate': 'Key rate',
        'mean': 'Latency mean',
        'med': 'Latency median',
        '95th_latency': 'Latency 95th percentile',
        '99th_latency': 'Latency 99th percentile',
        '99.9th_latency': 'Latency 99.9th percentile',
        'max_latency': 'Maximum latency',
        'elapsed_time': 'Total operation time (seconds)',
        'stderr': 'stderr',
        'errors': 'error count',
        'gc_count': 'GC count',
        'gc_max_ms': 'GC longest pause (ms)',
        'gc_sum_ms': 'GC total pause (ms)',
        'gc_sdv_ms': 'GC pause standard deviation (ms)',
        'gc_mb': 'GC memory freed (MB)'
    };

    var updateURLBar = function() {
        //Update the URL bar with the current parameters:
        window.history.replaceState(null,null,parseUri(location).path + "?" + $.param(query));
    };

    //Check query parameters:
    if (metric == undefined) {
        metric = query.metric = 'op_rate';
    }
    if (operation == undefined) {
        operation = query.operation = 'write';
    }
    if (smoothing == undefined) {
        smoothing = query.smoothing = 1;
    }
    if (show_aggregates == undefined || query.show_aggregates == 'true') {
        show_aggregates = query.show_aggregates = true;
    } else {
        show_aggregates = query.show_aggregates = false;
    }
    console.log(show_aggregates);
    updateURLBar();

    var metric_index = stress_metrics.indexOf(metric);
    var time_index = stress_metrics.indexOf('elapsed_time');

    /// Add dropdown controls to select chart criteria / options:
    var chart_controls = $('<div id="chart_controls"/>');
    var chart_controls_tbl = $('<table/>');
    chart_controls.append(chart_controls_tbl);
    $('body').append(chart_controls);
    var metric_selector = $('<select id="metric_selector"/>');
    $.each(stress_metric_names, function(k,v) {
        if (k == 'elapsed_time') {
            return; //Elapsed time makes no sense to graph, skip it.
        }
        var option = $('<option/>').attr('value', k).text(v);
        if (metric == k) {
            option.attr('selected','selected');
        }
        metric_selector.append(option);

    });
    chart_controls_tbl.append('<tr><td><label for="metric_selector"/>Choose metric:</label></td><td id="metric_selector_td"></td></tr>')
    $('#metric_selector_td').append(metric_selector);

    var operation_selector = $('<select id="operation_selector"/>')
    chart_controls_tbl.append('<tr><td><label for="operation_selector"/>Choose operation:</label></td><td id="operation_selector_td"></td></tr>')
    $('#operation_selector_td').append(operation_selector);


    var smoothing_selector = $('<select id="smoothing_selector"/>')
    $.each([1,2,3,4,5,6,7,8], function(i, v) {
        var option = $('<option/>').attr('value', v).text(v);
        if (smoothing == v) {
            option.attr('selected','selected');
        }
        smoothing_selector.append(option);
    });
    chart_controls_tbl.append('<tr><td style="width:150px"><label for="smoothing_selector"/>Data smoothing:</label></td><td id="smoothing_selector_td"></td></tr>')
    $("#smoothing_selector_td").append(smoothing_selector);

    var show_aggregates_checkbox = $('<input type="checkbox" id="show_aggregates_checkbox"/>');
    chart_controls_tbl.append('<tr><td style="padding-top:10px"><label for="show_aggregates_checkbox">Show aggregates</label></td><td id="show_aggregates_td"></td></tr>');
    $("#show_aggregates_td").append(show_aggregates_checkbox);
    show_aggregates_checkbox.attr("checked", show_aggregates);

    chart_controls_tbl.append('<tr><td colspan="100%">Zoom: <a href="#" id="reset_zoom">reset</a><table id="zoom"><tr><td><label for="xmin"/>x min</label></td><td><input id="xmin"/></td><td><label for="xmax"/>x max</label></td><td><input id="xmax"/></td></tr><tr><td><label for="ymin"/>y min</label></td><td><input id="ymin"/></td><td><label for="ymax"/>y max</label></td><td><input id="ymax"/></td></tr></table></td></tr>');

    chart_controls_tbl.append('<tr><td style="padding-top:10px" colspan="100%">To hide/show a dataset click on the associated colored box</td></tr>');

    chart_controls_tbl.append('<tr><td style="padding-top:10px" colspan="100%"><a href="#" id="dl-test-data">Download raw test data</a></td></tr>');

    var raw_data;

    //Callback to draw graph once we have json data.
    var graph_callback = function() {
        var data = [];
        var trials = {};
        var data_by_title = {};
        //Keep track of what operations are availble from the test:
        var operations = {};
        
        raw_data.stats.forEach(function(d) {
            // Make a copy of d so we never modify raw_data
            d = $.extend({}, d);
            operations[d.test] = true;
            if (d.test!=operation) {
                return;
            }
            d.title = d['label'] != undefined ? d['label'] : d['revision'];
            data_by_title[d.title] = d;
            data.push(d);
            trials[d.title] = d;
            //Clean up the intervals:
            //Remove every other item, so as to smooth the line:
            var new_intervals = [];
            d.intervals.forEach(function(i, x) {
                if (x % smoothing == 0) {
                    new_intervals.push(i);
                }
            });
            d.intervals = new_intervals;
        });

        //Fill operations available from test:
        operation_selector.children().remove();
        $.each(operations, function(k) {
            var option = $('<option/>').attr('value', k).text(k);
            if (operation == k) {
                option.attr('selected','selected');
            }
            operation_selector.append(option);
        });

        var getMetricValue = function(d) {
            if (metric_index >= 0) {
                //This is one of the metrics directly reported by stress:
                return d[metric_index];
            } else {
                //This metric is not reported by stress, so compute it ourselves:
                if (metric == 'num_timeouts') {
                    return d[stress_metrics.indexOf('interval_op_rate')] - d[stress_metrics.indexOf('interval_key_rate')];
                }
            }
        };

        //Parse the dates:
        data.forEach(function(d) {
            d.date = new Date(Date.parse(d.date));
        });

        $("svg").remove();
        //Setup initial zoom level:
        defaultZoom = function(initialize) {
            if (!initialize) {
                //Reset zoom query params:
                query.xmin = xmin = undefined;
                query.xmax = xmax = undefined;
                query.ymin = ymin = undefined;
                query.ymax = ymax = undefined;
            }
            query.xmin = xmin = query.xmin ? query.xmin : 0;
            query.xmax = xmax = query.xmax ? query.xmax : Math.round(d3.max(data, function(d) {
                if (d.intervals.length > 0) {
                    return d.intervals[d.intervals.length-1][time_index];
                }
            }) * 1.1 * 100) / 100;
            query.ymin = ymin = query.ymin ? query.ymin : 0;
            query.ymax = ymax = query.ymax ? query.ymax : Math.round(d3.max(data, function(d) {
                return d3.max(d.intervals, function(i) {
                    return getMetricValue(i);
                });
            }) * 1.1 * 100) / 100;
            $("#xmin").val(xmin);
            $("#xmax").val(xmax);
            $("#ymin").val(ymin);
            $("#ymax").val(ymax);
            var updateX = function() {
                query.xmin = xmin = $("#xmin").val();
                query.xmax = xmax = $("#xmax").val();
                x.domain([xmin,xmax]);
                updateURLBar();
            };
            var updateY = function() {
                query.ymin = ymin = $("#ymin").val();
                query.ymax = ymax = $("#ymax").val();
                y.domain([ymin, ymax]);
                updateURLBar();
            };
            $("#xmin,#xmax").unbind().change(function(e) {
                updateX();
                redrawLines();
            });
            $("#ymin,#ymax").unbind().change(function(e) {
                updateY();
                redrawLines();
            });
            // The first time defaultZoom is called, we pass
            // initialize=true, and we do not call the change() method
            // yet. On subsequent calls, without initialize, we do.
            if (!initialize) {
                updateX();
                updateY();
                redrawLines();
            }
        }
        defaultZoom(true);

        $("#reset_zoom").click(function(e) {
            defaultZoom();
            e.preventDefault();
        });

        //Setup chart:
        var margin = {top: 20, right: 1180, bottom: 2240, left: 60};
        var width = 2060 - margin.left - margin.right;
        var height = 2700 - margin.top - margin.bottom;

        var x = d3.scale.linear()
            .domain([xmin, xmax])
            .range([0, width]);

        var y = d3.scale.linear()
            .domain([ymin, ymax])
            .range([height, 0]);

        var color = d3.scale.category10();
        color.domain(data.map(function(d){return d.title}));

        var xAxis = d3.svg.axis()
            .scale(x)
            .orient("bottom");

        var yAxis = d3.svg.axis()
            .scale(y)
            .orient("left");

        var line = d3.svg.line()
            .interpolate("basis")
            .x(function(d) {
                return x(d[time_index]); //time in seconds
            })
            .y(function(d) {
                return y(getMetricValue(d));
            });

        $("body").append("<div id='svg_container'>");

        var redrawLines = function() {
            svg.select(".x.axis").call(xAxis);
            svg.select(".y.axis").call(yAxis);
            svg.selectAll(".line")
                .attr("class","line")
                .attr("d", function(d) {
                    return line(d.intervals);
                })
            $("#xmin").val(x.domain()[0]);
            $("#xmax").val(x.domain()[1]);
            $("#ymin").val(y.domain()[0]);
            $("#ymax").val(y.domain()[1]);
        }

        var zoom = d3.behavior.zoom()
            .x(x)
            .y(y)
            .on("zoom", redrawLines);

        var svg = d3.select("div#svg_container").append("svg")
            .attr("width", width + margin.left + margin.right + 250)
            .attr("height", height + margin.top + margin.bottom)
            .append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")")

        // Clip Path
        svg.append("svg:clipPath")
            .attr("id", "chart_clip")
            .append("svg:rect")
            .attr("width", width)
            .attr("height", height);

        // Chart title
        svg.append("text")
            .attr("x", width / 2 )
            .attr("y", 0 )
            .style('font-size', '2em')
            .style("text-anchor", "middle")
            .text(raw_data.title + ' - ' + operation);

        // Chart subtitle
        svg.append("text")
            .attr("x", width / 2 )
            .attr("y", 15 )
            .style('font-size', '1.2em')
            .style("text-anchor", "middle")
            .text((raw_data.subtitle ? raw_data.subtitle : ''));

        // x-axis - time
        svg.append("g")
            .attr("class", "x axis")
            .attr("transform", "translate(0," + height + ")")
            .call(xAxis);

        // x-axis label
        svg.append("text")
            .attr("x", width / 2 )
            .attr("y", height + 30 )
            .style("text-anchor", "middle")
            .style("font-size", "1.2em")
            .text(stress_metric_names['elapsed_time']);

        // y-axis
        svg.append("g")
            .attr("class", "y axis")
            .call(yAxis)
            .append("text")
            .attr("transform", "rotate(-90)")
            .attr("y", -60)
            .attr("dy", ".91em")
            .style("font-size", "1.2em")
            .style("text-anchor", "end")
            .text(stress_metric_names[metric]);

        var trial = svg.selectAll(".trial")
            .data(data)
            .enter().append("g")
            .attr("class", "trial")
            .attr("title", function(d) {
                return d.title;
            });

        // Draw benchmarked data:
        trial.append("path")
            .attr("class", "line")
            .attr("clip-path", "url(#chart_clip)")
            .attr("d", function(d) {
                return line(d.intervals);
            })
            .style("stroke", function(d) { return color(d.title); });

        var legend = svg.selectAll(".legend")
            .data(color.domain())
            .enter().append("g")
            .attr("class", "legend")
            .attr("transform", function(d, i) {
                if (show_aggregates == true) {
                    var y_offset = 425 + (i*190) + 70;
                } else {
                    var y_offset = 425 + (i*25) + 70;
                }
                var x_offset = -550;
                return "translate(" + x_offset + "," + y_offset + ")";
            });

        var renderLegendText = function(linenum, getTextCallback) {
            legend.append("text")
                .attr("x", width - 24 - 250)
                .attr("y", 12*linenum)
                .attr("dy", ".35em")
                .style("font-family", "monospace")
                .style("font-size", "1.2em")
                .style("text-anchor", "start")
                .text(function(d) {
                    return getTextCallback(d);
                });
        };

        var padTextEnd = function(text, length) {
            for(var x=text.length; x<length; x++) {
                text = text + '\u00A0';
            }
            return text;
        };
        var padTextStart = function(text, length) {
            for(var x=text.length; x<length; x++) {
                text = '\u00A0' + text;
            }
            return text;
        };

        renderLegendText(1, function(title) {
            return padTextStart(title, title.length + 5);
        });

        if (show_aggregates === true) {
            renderLegendText(2, function(title) {
                return '---------------------------------------';
            });

            renderLegendText(3, function(title) {
                return padTextEnd('op rate', 26) + " : " + data_by_title[title]['op rate'];
            });

            renderLegendText(4, function(title) {
                return padTextEnd('partition rate', 26) + " : " + data_by_title[title]['partition rate'];
            });

            renderLegendText(5, function(title) {
                return padTextEnd('row rate', 26) + ' : ' + data_by_title[title]['row rate'];
            });

            renderLegendText(7, function(title) {
                return padTextEnd('latency mean', 26) + ' : ' + data_by_title[title]['latency mean'];
            });

            renderLegendText(8, function(title) {
                return padTextEnd('latency median', 26) + ' : ' + data_by_title[title]['latency median'];
            });

            renderLegendText(9, function(title) {
                return padTextEnd('latency 95th percentile', 26) + ' : ' + data_by_title[title]['latency 95th percentile'];
            });

            renderLegendText(10, function(title) {
                return padTextEnd('latency 99th percentile', 26) + ' : ' + data_by_title[title]['latency 99th percentile'];
            });

            renderLegendText(11, function(title) {
                return padTextEnd('latency 99.9th percentile', 26) + ' : ' + data_by_title[title]['latency 99.9th percentile'];
            });

            renderLegendText(12, function(title) {
                return padTextEnd('latency max', 26) + ' : ' + data_by_title[title]['latency max'];
            });

            renderLegendText(13, function(title) {
                return padTextEnd('Total operation time', 26) + ' : ' + data_by_title[title]['Total operation time'];
            });

            renderLegendText(14, function(title) {
                var cmd = data_by_title[title]['command'];
                return 'cmd: ' + cmd;
            });
        }
        legend.append("rect")
            .attr("x", width - 270)
            .attr("width", 18)
            .attr("height", 18)
            .attr("class", "legend-rect")
            .attr("title", function(title) {
                return title;
            })
            .style("fill", color);

        //Make trials hideable by double clicking on the colored legend box
        $("rect.legend-rect").click(function() {
            $("g.trial[title='" + $(this).attr('title') + "']").toggle();
        });


        // Chart control callbacks:
        metric_selector.unbind().change(function(e) {
            // change the metric in the url to reload the page:
            metric = query.metric = this.value;
            metric_index = stress_metrics.indexOf(metric);
            graph_callback();
            defaultZoom();
        });
        operation_selector.unbind().change(function(e) {
            // change the metric in the url to reload the page:
            operation = query.operation = this.value;
            graph_callback();
            defaultZoom();
        });
        smoothing_selector.unbind().change(function(e) {
            // change the metric in the url to reload the page:
            smoothing = query.smoothing = this.value;
            graph_callback();
            defaultZoom();
        });
        show_aggregates_checkbox.unbind().change(function(e) {
            show_aggregates = query.show_aggregates = this.checked;
            graph_callback();
        });

        updateURLBar();

        $("#dl-test-data").attr("href",stats_db);

        // Chart zoom/drag surface
        // This should always be last, so it's on top of everything else
        svg.append("svg:rect")
            .attr("id", "zoom_drag_surface")
            .attr("width", width)
            .attr("height", height);

        //If the operation selected is not one of the available
        //operations in the JSON data file, select the first
        //operation available:
        if (!(operation in operations)) {
            $(operation_selector).val($(operation_selector).find("option :first").text()).change();
        }

    }

    $('#loading_indicator').loadingOverlay();

    d3.json(stats_db, function(error, data) {
        //Filter the dataset for the one we want:
        raw_data = data;
        $('#loading_indicator').loadingOverlay('remove');
        graph_callback();
    });

}

$(document).ready(function(){

    drawGraph();

});
