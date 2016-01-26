#!/usr/bin/python
import json
import requests
import sys
import time
import datetime


def indent(level):
    retval = ""
    for i in range(0, level):
        retval += "    "
    return retval


def get_most_recent_test(series):
    res = requests.get("http://cstar.datastax.com/api/series/{series}/{ts_start}/{ts_end}".format(
            series=series, ts_start=int(time.time() - 250000), ts_end=int(time.time())))
    data = json.loads(res.text)
    if data and data['series'] and len(data['series']) > 0:
        return data['series'][-1]
    return None


def generate_graphs(cached=False):
    compaction_operation = [
        {'name': 'Initial write throughput', 'op': '1_write', 'metric': 'op_rate'},
        {'name': 'Initial read throughput', 'op': '4_read', 'metric': 'op_rate'},
        {'name': 'Second read throughput', 'op': '5_read', 'metric': 'op_rate'},
        {'name': 'Initial write P99.9', 'op': '1_write', 'metric': '99.9th_latency'},
        {'name': 'Initial read P99.9', 'op': '4_read', 'metric': '99.9th_latency'},
        {'name': 'Second read P99.9', 'op': '5_read', 'metric': '99.9th_latency'},
        # {'name': 'Compaction elapsed time', 'op': '3_nodetool', 'metric': 'elapsed_time'}
    ]

    simple_operation = [
        {'name': 'Initial write throughput', 'op': '1_write', 'metric': 'op_rate'},
        {'name': 'Initial read throughput', 'op': '2_read', 'metric': 'op_rate'},
        {'name': 'Second read throughput', 'op': '3_read', 'metric': 'op_rate'},
        {'name': 'Initial write P99.9', 'op': '1_write', 'metric': '99.9th_latency'},
        {'name': 'Initial read P99.9', 'op': '2_read', 'metric': '99.9th_latency'},
        {'name': 'Second read P99.9', 'op': '3_read', 'metric': '99.9th_latency'}
    ]

    repair_operation = [
        {'name': 'Initial read throughput', 'op': '4_read', 'metric': 'op_rate'},
        {'name': 'Second read throughput', 'op': '5_read', 'metric': 'op_rate'},
        {'name': 'Initial read P99.9', 'op': '4_read', 'metric': '99.9th_latency'},
        {'name': 'Second read P99.9', 'op': '5_read', 'metric': '99.9th_latency'},
        # {'name': 'Repair elapsed time', 'op': '3_nodetool', 'metric': 'elapsed_time'}
    ]

    rolling_upgrade = [
        {'name': 'Initial write throughput', 'op': '4_write', 'metric': 'op_rate'},
        {'name': 'Initial read throughput', 'op': '5_read', 'metric': 'op_rate'},
        {'name': 'Initial write P99.9', 'op': '4_write', 'metric': '99.9th_latency'},
        {'name': 'Initial read P99.9', 'op': '5_read', 'metric': '99.9th_latency'}
    ]

    mv_operation = [
        {'name': 'write', 'op': '1_user', 'metric': 'op_rate'}
    ]

    series_list = {
        'daily_regressions_trunk-compaction': compaction_operation,
        'daily_regressions_trunk-commitlog_sync': simple_operation,
        'daily_regressions_trunk-read_write': simple_operation,
        'daily_regressions_trunk-repair_10M': repair_operation,
        'daily_regressions_trunk-compaction_lcs': compaction_operation,
        'daily_regressions_trunk-compaction_stcs': compaction_operation,
        'daily_regressions_trunk-compaction_dtcs': compaction_operation,
        'daily_regressions_trunk-rolling_upgrade': rolling_upgrade,
        'daily_regressions_trunk-materialized_views_write_3_mv': mv_operation,
        'daily_regressions_trunk-materialized_views_write_1_mv': mv_operation
    }

    retval = ""
    retval += "<html>\n"
    retval += indent(1) + "<head>\n"
    retval += indent(2) + '<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/css/bootstrap.min.css" integrity="sha384-1q8mTJOASx8j1Au+a5WDVnPi2lkFfwwEAa8hDDdjZlpLegxhjVME1fgjWPGmkzs7" crossorigin="anonymous">\n'
    retval += indent(2) + '<script src="https://ajax.googleapis.com/ajax/libs/jquery/1.11.3/jquery.min.js"></script>\n'
    retval += indent(2) + '<script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/js/bootstrap.min.js" integrity="sha384-0mSbJDEHialfmuBBQP6A4Qrprq5OVfW37PRR3j5ELqxss1yVqOtnepnHVP9aJ7xS" crossorigin="anonymous"></script>\n'
    retval += indent(2) + '<style>body { margin: 15px; }</style>\n'
    retval += indent(1) + "</head>\n"
    retval += indent(1) + "<body>\n"
    retval += indent(1) + "<h2>Daily C*Perf Regression Dashboard\n"

    if cached:
        retval += indent(1) + '<span style="color: red; font-size: 0.5em;">cached {} <a href="#" onclick="confirm(\'Loading/Updating the cached images is extremely expensive.  Are you sure?\') == true ? window.location = \'dashboard_uncached.html\' : false; ">non-cached version</a></span>'.format(datetime.datetime.now().isoformat(' '))
    retval += indent(1) + "</h2>\n"

    for series, operations in series_list.iteritems():
        retval += indent(2) + "<h3>" + series + "</h3>\n"

        retval += indent(2) + "<h4>Most Recent Test Run: \n"
        id = get_most_recent_test(series)
        if id:
            retval += indent(3) + '<a href="http://cstar.datastax.com/tests/id/{id}">Details</a> \n'.format(id=id)
            retval += indent(3) + '<a href="http://cstar.datastax.com/tests/artifacts/{id}/graph">Graph</a></br>\n'.format(id=id)
        else:
            retval += indent(3) + ' (unavailable)</br>\n'
        retval += indent(2) + "</h4>\n"

        retval += indent(2) + '<div class="row">\n'

        for operation in operations:
            retval += indent(3) + '<div class="col-sm-6 col-md-4">\n'
            retval += indent(4) + '<div class="thumbnail">\n'

            retval += indent(5) + '<a href="#" class="popimage">\n'.format(
                    series=series, op=operation['op'], metric=operation['metric']
            )
            retval += indent(6) + "<img src='http://cstar.datastax.com/api/series/" + series + "/2538000/graph/"
            if cached:
                retval += "cached/"
            retval += operation['op'] + "/" + operation['metric'] + ".png'/>\n"
            retval += indent(5) + '</a>\n'

            retval += indent(5) + '<div class="caption">\n'
            retval += indent(6) + "{}\n".format(operation['name'])
            retval += indent(5) + '</div>\n'

            retval += indent(4) + "</div>\n"
            retval += indent(3) + "</div>\n"

        retval += indent(2) + "</div>\n"
        retval += indent(2) + "<br/>\n"

    retval += indent(2) + """
        <!-- Creates the bootstrap modal where the image will appear -->
        <div class="modal fade" id="imagemodal" tabindex="-1" role="dialog" aria-labelledby="myModalLabel" aria-hidden="true">
          <div class="modal-dialog">
            <div class="modal-content">
              <div class="modal-header">
                <button type="button" class="close" data-dismiss="modal"><span aria-hidden="true">&times;</span><span class="sr-only">Close</span></button>
                <h4 class="modal-title" id="myModalLabel">Full Size</h4>
              </div>
              <div class="modal-body">
                <img src="" id="imagepreview">
              </div>
              <div class="modal-footer">
                <button type="button" class="btn btn-default" data-dismiss="modal">Close</button>
              </div>
            </div>
          </div>
        </div>

        <script>
            $(document).ready(function() {
                $(".popimage").on("click", function(e) {
                   e.preventDefault();

                   $('#imagepreview').attr('src', $(this).children(":first").attr('src'));
                   $('#imagemodal').modal('show');
                });

                $('#imagemodal').on('shown.bs.modal', function () {
                    $(this).find('.modal-dialog').css({width:'945px',
                                               height:'auto',
                                              'max-height':'100%'});
                });
            });
        </script>
    """

    retval += indent(1) + "</body>\n"
    retval += "</html>\n"

    return retval

if __name__ == "__main__":
    print generate_graphs(len(sys.argv) == 2 and sys.argv[1] == 'cached')
    # print generate_graphs(True)
