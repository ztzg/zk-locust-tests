var serverMetrics_3_5 = [
  // Via MonitorCommand in Commands.java.
  { name: "latency", kind: "Summary (Basic)" },

  { name: "packets_received", kind: "Counter" },
  { name: "packets_sent", kind: "Counter" },
  { name: "num_alive_connections", kind: "Gauge" },
  { name: "outstanding_requests", kind: "Gauge" },
  { name: "znode_count", kind: "Gauge" },
  { name: "watch_count", kind: "Gauge" },
  { name: "ephemerals_count", kind: "Gauge" },
  { name: "approximate_data_size", kind: "Gauge" },
  { name: "open_file_descriptor_count", kind: "Gauge" },
  { name: "max_file_descriptor_count", kind: "Gauge" },
  { name: "last_client_response_size", kind: "Gauge" },
  { name: "max_client_response_size", kind: "Gauge" },
  { name: "min_client_response_size", kind: "Gauge" }
];

var serverMetrics_3_6 = [
  // Via registerGauge in ZooKeeperServer.java; tweaked.
  { name: "latency", kind: "Summary (Basic)" },

  { name: "packets_received", kind: "Counter" },
  { name: "packets_sent", kind: "Counter" },
  { name: "num_alive_connections", kind: "Gauge" },
  { name: "outstanding_requests", kind: "Gauge" },
  { name: "uptime", kind: "Counter" },
  { name: "znode_count", kind: "Gauge" },
  { name: "watch_count", kind: "Gauge" },
  { name: "ephemerals_count", kind: "Gauge" },
  { name: "approximate_data_size", kind: "Gauge" },
  { name: "global_sessions", kind: "Gauge" },
  { name: "local_sessions", kind: "Gauge" },
  { name: "open_file_descriptor_count", kind: "Gauge" },
  { name: "max_file_descriptor_count", kind: "Gauge" },
  { name: "connection_drop_probability", kind: "Gauge" },
  { name: "last_client_response_size", kind: "Gauge" },
  { name: "max_client_response_size", kind: "Gauge" },
  { name: "min_client_response_size", kind: "Gauge" },

  // Via registerGauge in FollowerZooKeeperServer.java; tweaked.
  { name: "synced_observers", kind: "Gauge" },

  // Via registerGauge in LeaderZooKeeperServer.java; tweaked.
  { name: "learners", kind: "Gauge" },
  { name: "synced_followers", kind: "Gauge" },
  { name: "synced_non_voting_followers", kind: "Gauge" },
  { name: "synced_observers", kind: "Gauge" },
  { name: "pending_syncs", kind: "Gauge" },
  { name: "leader_uptime", kind: "Counter" },
  { name: "last_proposal_size", kind: "Gauge" },
  { name: "max_proposal_size", kind: "Gauge" },
  { name: "min_proposal_size", kind: "Gauge" },

  // Via registerGauge in QuorumZooKeeperServer.java; tweaked.
  { name: "quorum_size", kind: "Gauge" },

  // Via ServerMetrics
  { name: "fsynctime", kind: "Summary (Basic)" },
  { name: "snapshottime", kind: "Summary (Basic)" },
  { name: "dbinittime", kind: "Summary (Basic)" },
  { name: "readlatency", kind: "Summary" },
  { name: "updatelatency", kind: "Summary" },
  { name: "propagation_latency", kind: "Summary" },
  { name: "follower_sync_time", kind: "Summary (Basic)" },
  { name: "election_time", kind: "Summary (Basic)" },
  { name: "looking_count", kind: "Counter" },
  { name: "diff_count", kind: "Counter" },
  { name: "snap_count", kind: "Counter" },
  { name: "commit_count", kind: "Counter" },
  { name: "connection_request_count", kind: "Counter" },
  { name: "connection_token_deficit", kind: "Summary (Basic)" },
  { name: "connection_rejected", kind: "Counter" },
  { name: "write_per_namespace", kind: "Summary Set (Basic)" },
  { name: "read_per_namespace", kind: "Summary Set (Basic)" },
  { name: "bytes_received_count", kind: "Counter" },
  { name: "unrecoverable_error_count", kind: "Counter" },
  { name: "node_created_watch_count", kind: "Summary (Basic)" },
  { name: "node_deleted_watch_count", kind: "Summary (Basic)" },
  { name: "node_changed_watch_count", kind: "Summary (Basic)" },
  { name: "node_children_watch_count", kind: "Summary (Basic)" },
  { name: "add_dead_watcher_stall_time", kind: "Counter" },
  { name: "dead_watchers_queued", kind: "Counter" },
  { name: "dead_watchers_cleared", kind: "Counter" },
  { name: "dead_watchers_cleaner_latency", kind: "Summary" },
  { name: "response_packet_cache_hits", kind: "Counter" },
  { name: "response_packet_cache_misses", kind: "Counter" },
  { name: "ensemble_auth_success", kind: "Counter" },
  { name: "ensemble_auth_fail", kind: "Counter" },
  { name: "ensemble_auth_skip", kind: "Counter" },
  { name: "prep_processor_queue_time_ms", kind: "Summary" },
  { name: "prep_processor_queue_size", kind: "Summary (Basic)" },
  { name: "prep_processor_request_queued", kind: "Counter" },
  { name: "outstanding_changes_queued", kind: "Counter" },
  { name: "outstanding_changes_removed", kind: "Counter" },
  { name: "prep_process_time", kind: "Summary (Basic)" },
  { name: "close_session_prep_time", kind: "Summary" },
  { name: "revalidate_count", kind: "Counter" },
  { name: "connection_drop_count", kind: "Counter" },
  { name: "connection_revalidate_count", kind: "Counter" },
  { name: "sessionless_connections_expired", kind: "Counter" },
  { name: "stale_sessions_expired", kind: "Counter" },
  { name: "requests_in_session_queue", kind: "Summary (Basic)" },
  { name: "pending_session_queue_size", kind: "Summary (Basic)" },
  { name: "reads_after_write_in_session_queue", kind: "Summary (Basic)" },
  { name: "reads_issued_from_session_queue", kind: "Summary (Basic)" },
  { name: "session_queues_drained", kind: "Summary (Basic)" },
  {
    name: "time_waiting_empty_pool_in_commit_processor_read_ms",
    kind: "Summary (Basic)"
  },
  { name: "write_batch_time_in_commit_processor", kind: "Summary (Basic)" },
  {
    name: "concurrent_request_processing_in_commit_processor",
    kind: "Summary (Basic)"
  },
  { name: "read_commit_proc_req_queued", kind: "Summary (Basic)" },
  { name: "write_commit_proc_req_queued", kind: "Summary (Basic)" },
  { name: "commit_commit_proc_req_queued", kind: "Summary (Basic)" },
  { name: "request_commit_queued", kind: "Counter" },
  { name: "read_commit_proc_issued", kind: "Summary (Basic)" },
  { name: "write_commit_proc_issued", kind: "Summary (Basic)" },
  { name: "read_commitproc_time_ms", kind: "Summary" },
  { name: "write_commitproc_time_ms", kind: "Summary" },
  { name: "local_write_committed_time_ms", kind: "Summary" },
  { name: "server_write_committed_time_ms", kind: "Summary" },
  { name: "commit_process_time", kind: "Summary (Basic)" },
  { name: "om_proposal_process_time_ms", kind: "Summary" },
  { name: "om_commit_process_time_ms", kind: "Summary" },
  { name: "read_final_proc_time_ms", kind: "Summary" },
  { name: "write_final_proc_time_ms", kind: "Summary" },
  { name: "proposal_latency", kind: "Summary" },
  { name: "proposal_ack_creation_latency", kind: "Summary" },
  { name: "commit_propagation_latency", kind: "Summary" },
  { name: "learner_proposal_received_count", kind: "Counter" },
  { name: "learner_commit_received_count", kind: "Counter" },
  { name: "learner_handler_qp_size", kind: "Summary Set (Basic)" },
  { name: "learner_handler_qp_time_ms", kind: "Summary Set" },
  { name: "startup_txns_loaded", kind: "Summary (Basic)" },
  { name: "startup_txns_load_time", kind: "Summary (Basic)" },
  { name: "startup_snap_load_time", kind: "Summary (Basic)" },
  { name: "sync_processor_queue_and_flush_time_ms", kind: "Summary" },
  { name: "sync_processor_queue_size", kind: "Summary (Basic)" },
  { name: "sync_processor_request_queued", kind: "Counter" },
  { name: "sync_processor_queue_time_ms", kind: "Summary" },
  { name: "sync_processor_queue_flush_time_ms", kind: "Summary" },
  { name: "sync_process_time", kind: "Summary (Basic)" },
  { name: "sync_processor_batch_size", kind: "Summary (Basic)" },
  { name: "quorum_ack_latency", kind: "Summary" },
  { name: "ack_latency", kind: "Summary Set" },
  { name: "proposal_count", kind: "Counter" },
  { name: "quit_leading_due_to_disloyal_voter", kind: "Counter" },
  { name: "stale_requests", kind: "Counter" },
  { name: "stale_requests_dropped", kind: "Counter" },
  { name: "stale_replies", kind: "Counter" },
  { name: "request_throttle_wait_count", kind: "Counter" },
  { name: "netty_queued_buffer_capacity", kind: "Summary (Basic)" },
  { name: "digest_mismatches_count", kind: "Counter" }
];

// We don't know which server we are targeting yet.
var serverMetrics = [];

$("ul.tabs")
  .tabs("div.panes > div")
  .on("onClick", function(event) {
    if (event.target == $(".chart-tab-link")[0]) {
      // trigger resizing of charts
      metricsTargets.forEach(function(target) {
        for (var metricName in target.charts) {
          target.charts[metricName].resize();
        }
      });
    }
  });

$(".edit_refresh_ms").click(function(event) {
  event.preventDefault();
  $("#edit").show();
  $("#new_refresh_ms")
    .val("" + refreshDelayMs)
    .focus()
    .select();
});

$("#new_refresh_ms_button").click(function(event) {
  event.preventDefault();
  $("#edit").hide();
  var msText = $("#new_refresh_ms").val();
  var ms = parseInt(msText, 10);

  refreshDelayMs = isNaN(ms) || ms < 0 ? 0 : ms;
  if (refreshTimeoutId > 0) {
    clearTimeout(refreshTimeoutId);
  }
  updateStats();
});

$(".close_link").click(function(event) {
  event.preventDefault();
  $(this)
    .parent()
    .parent()
    .hide();
});

// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Regular_Expressions#Escaping
function escapeRegExp(string) {
  return string.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"); // $& means the whole matched string
}

function compileFilter(pattern) {
  pattern = pattern.trim();
  if (!pattern) {
    return undefined;
  }

  var rx = new RegExp(
    escapeRegExp(pattern)
      .split(/\s+/)
      .join(".*")
  );

  return function(s) {
    return rx.test(s);
  };
}

function applyFilterToItems(filter, item$) {
  if (!filter) {
    item$.show();
  } else {
    item$.each(function() {
      var el$ = $(this);
      var itemName = el$.attr("data-item-name");

      if (itemName && filter(itemName)) {
        el$.show();
      } else {
        el$.hide();
      }
    });
  }
}

$("#metrics_filter").on("input", function(event) {
  var filter = compileFilter(event.target.value);

  applyFilterToItems(filter, $("#metrics tbody").children("tr"));
});

$("#stats_filter").on("input", function(event) {
  statsTableFilter = compileFilter(event.target.value);

  applyFilterToItems(statsTableFilter, $("#stats tbody").children("tr"));
});

$("#charts_filter").on("input", function(event) {
  chartsFilter = compileFilter(event.target.value);

  applyFilterToItems(chartsFilter, $(".charts-container").children("div"));
});

var metricsScheme = "http";
var metricsPort = 8080;

var refreshDelayMs = 2000;
var refreshTimeoutId = -1;

var lastEnsembleText;
var metricsTargets = [];

var statsTableFilter = undefined;
var chartsFilter = undefined;

function updateEnsemble(ensembleText) {
  var hosts = [];

  ensembleText = ensembleText.trim();
  ensembleText = ensembleText.replace(/\/.*/, "");

  ensembleText.split(",").forEach(function(zkHostPort) {
    zkHostPort = zkHostPort.trim();
    if (!zkHostPort) {
      return;
    }

    var host = zkHostPort.replace(/:\d{1,4}$/, "");
    hosts.push(host);
  });

  var newMetricsTargets = [];

  var chartsContainer$ = $(".charts-container").empty();

  hosts.forEach(function(host, index) {
    newMetricsTargets.push({
      host,
      index,
      charts: {},
      lastReport: undefined
    });
  });

  return (metricsTargets = newMetricsTargets);
}

function collectKeyedValues(key, qualifiers, reports, collector) {
  var n = 0;

  var values = reports.map(function(report) {
    if (!report || typeof report[key] === "undefined") {
      return undefined;
    }

    n++;
    return report[key];
  });

  collector(key, n, qualifiers, values);
}

var scalarQualifiers = {};
function collectScalarValues(name, reports, collector) {
  collectKeyedValues(name, scalarQualifiers, reports, collector);
}

function collectAbstractSummaryValues(name, qualifiersSeq, reports, collector) {
  qualifiersSeq.forEach(function(qualifiers) {
    var key = qualifiers.stat + "_" + name;

    collectKeyedValues(key, qualifiers, reports, collector);
  });
}

var basicSummaryQualifiersSeq = ["avg", "min", "max", "cnt", "sum"].map(
  function(stat) {
    return { stat };
  }
);

function collectBasicSummaryValues(name, reports, collector) {
  collectAbstractSummaryValues(
    name,
    basicSummaryQualifiersSeq,
    reports,
    collector
  );
}

var advancedSummaryQualifiersSeq = basicSummaryQualifiersSeq.concat(
  ["p50", "p95", "p99", "p999"].map(function(stat) {
    return { stat };
  })
);

function collectAdvancedSummaryValues(name, reports, collector) {
  return collectAbstractSummaryValues(
    name,
    advancedSummaryQualifiersSeq,
    reports,
    collector
  );
}

function gatherSummarySetEntries(name, keys) {
  var subkeys = [];
  var knownSubkeys = {};

  keys.forEach(function(key) {
    if (!key.endsWith(name)) {
      return;
    }

    var cutAt = key.length - name.length - 1;
    if (cutAt <= 0 || key[cutAt] !== "_") {
      return;
    }

    var usAt = key.indexOf("_");
    if (usAt < 0 || usAt + 1 >= cutAt) {
      return;
    }

    var subkey = key.substring(usAt + 1, cutAt);
    if (knownSubkeys[subkey]) {
      return;
    }

    knownSubkeys[subkey] = true;
    subkeys.push(subkey);
  });

  return subkeys;
}

function collectAbstractSummarySetValues(
  name,
  entries,
  qualifiersSeq,
  reports,
  collector
) {
  entries.forEach(function(entry) {
    var extName = entry + "_" + name;
    var extQualifiersSeq = qualifiersSeq.map(function(qualifiers) {
      return Object.assign({}, qualifiers, { entry });
    });

    collectAbstractSummaryValues(extName, extQualifiersSeq, reports, collector);
  });
}

function collectBasicSummarySetValues(name, entries, reports, collector) {
  collectAbstractSummarySetValues(
    name,
    entries,
    basicSummaryQualifiersSeq,
    reports,
    collector
  );
}

function collectAdvancedSummarySetValues(name, entries, reports, collector) {
  collectAbstractSummarySetValues(
    name,
    entries,
    advancedSummaryQualifiersSeq,
    reports,
    collector
  );
}

function getServerMetricDefinitions(reports) {
  function looksLike_3_6(report) {
    return report["snap_count"] != null;
  }

  if (reports.some(looksLike_3_6)) {
    return serverMetrics_3_6;
  } else {
    return serverMetrics_3_5;
  }
}

var metricsTemplate = $("#metrics-template");

function updateMetricsTable(metrics) {
  var metricLookup = {};
  metrics.forEach(function(metric) {
    metricLookup[metric.name] = metric;
  });

  var tbody = $("#metrics tbody").empty();

  alternate = false;
  tbody.jqoteapp(metricsTemplate, metrics);

  $("input.metrics-toggle").change(function(event) {
    var target = $(event.target);
    var metricKey = target.attr("data-metric");
    var prop = target.attr("data-prop");

    var metric = metricLookup[metricKey];
    if (metric && prop) {
      var value = event.target.checked;

      metric[prop] = value;

      if (prop === "inCharts" && !value) {
        var sectionClass = "charts-section-" + metric.name;

        $("." + sectionClass).remove();
      }
    }
  });
}

var statsHeaderTemplate = $("#stats-header-template");
var statsTemplate = $("#stats-template");
var alternate = false;

function updateStatsTable(targets, reports) {
  var keyset = {};

  reports.forEach(function(report) {
    Object.keys(report).forEach(function(key) {
      keyset[key] = true;
    });
  });

  var rows = [];
  var currentMetric = undefined;

  function collector(key, n, qualifiers, values) {
    delete keyset[key];

    if (n > 0) {
      var name = currentMetric.name;
      var visible = !statsTableFilter || statsTableFilter(name);

      rows.push(
        Object.assign(
          {
            name,
            key,
            values,
            visible
          },
          qualifiers
        )
      );
    }
  }

  if (!serverMetrics.length) {
    serverMetrics = getServerMetricDefinitions(reports);
    updateMetricsTable(serverMetrics);
  }

  serverMetrics.forEach(function(metric) {
    if (!metric.inStats) {
      return;
    }

    currentMetric = metric;

    switch (metric.kind) {
      case "Counter":
      case "Gauge":
        collectScalarValues(metric.name, reports, collector);
        break;
      case "Summary (Basic)":
        collectBasicSummaryValues(metric.name, reports, collector);
        break;
      case "Summary":
        collectAdvancedSummaryValues(metric.name, reports, collector);
        break;
      case "Summary Set (Basic)":
        {
          var entries = gatherSummarySetEntries(
            metric.name,
            Object.keys(keyset)
          );
          collectBasicSummarySetValues(
            metric.name,
            entries,
            reports,
            collector
          );
        }
        break;
      case "Summary Set":
        {
          var entries = gatherSummarySetEntries(
            metric.name,
            Object.keys(keyset)
          );
          collectAdvancedSummarySetValues(
            metric.name,
            entries,
            reports,
            collector
          );
        }
        break;
      default:
        break;
    }
  });

  var thead = $("#stats thead").empty();
  var tbody = $("#stats tbody").empty();

  thead.jqoteapp(statsHeaderTemplate, [{ targets }]);
  alternate = false;
  tbody.jqoteapp(statsTemplate, rows);
}

function ensureChart(target, columns, metric) {
  var sectionClass = "charts-section-" + metric.name;
  var section$ = $("." + sectionClass);
  var hasSection = section$.length > 0;

  var chart = target.charts[metric.name];
  if (chart && hasSection) {
    return chart;
  }

  var chartIdBase = "charts-" + metric.name + "-";
  var chartId = chartIdBase + target.index;

  if (!hasSection) {
    var chartsContainer$ = $(".charts-container");

    section$ = $("<div>")
      .attr("data-item-name", metric.name)
      .addClass(sectionClass)
      .css("width", "100%")
      .appendTo(chartsContainer$);

    if (chartsFilter && !chartsFilter(metric.name)) {
      section$.hide();
    }

    $("<h2>")
      .text(metric.name)
      .appendTo(section$);

    var row = $("<div>")
      .css({ width: "100%", display: "flex" })
      .appendTo(section$);

    var width = 100.0 / columns + "%";
    for (var col = 0; col < columns; col++) {
      $("<div>", { id: chartIdBase + col })
        .css({ width, margin: "4px" })
        .appendTo(row);
    }
  }

  var chart$ = section$.find("div#" + chartId);
  if (!chart$.length) {
    return undefined;
  }

  switch (metric.kind) {
    case "Gauge":
      chart = new LocustLineChart(chart$, target.host, ["Value"], "");
      break;
    case "Counter":
      chart = new LocustLineChart(chart$, target.host, ["Change"], "");
      break;
    case "Summary (Basic)":
      chart = new LocustLineChart(chart$, target.host, ["Average", "Max"], "");
      break;
    case "Summary":
      chart = new LocustLineChart(
        chart$,
        target.host,
        ["Median", "95% percentile"],
        ""
      );
      break;
  }

  if (!chart) {
    return undefined;
  }

  target.charts[metric.name] = chart;

  return chart;
}

function updateCharts(target, columns, report) {
  serverMetrics.forEach(function(metric) {
    if (!metric.inCharts) {
      return;
    }

    var chart = ensureChart(target, columns, metric);
    if (!chart) {
      console.log({ l: "updateCharts/missing", target, columns, metric });
      return;
    }

    switch (metric.kind) {
      case "Gauge":
        {
          var value = report[metric.name];

          if (value != null) {
            chart.addValue([value]);
          }
        }
        break;
      case "Counter":
        {
          var lastValue = target.lastReport
            ? target.lastReport[metric.name]
            : undefined;
          var value = report[metric.name];

          if (lastValue != null && value != null) {
            chart.addValue([value - lastValue]);
          }
        }
        break;
      case "Summary":
        {
          var values = [
            report["p50_" + metric.name],
            report["p95_" + metric.name]
          ];

          chart.addValue(values);
        }
        break;
      case "Summary (Basic)":
        {
          var values = [
            report["avg_" + metric.name],
            report["max_" + metric.name]
          ];

          chart.addValue(values);
        }
        break;
      default:
        break;
    }
  });
}

function updateStats() {
  refreshTimeoutId = -1;

  var targets = metricsTargets;

  var ensemble$ = $("#ensemble_text");
  var ensembleText = ensemble$.attr("data-ensemble");
  if (ensembleText !== lastEnsembleText) {
    lastEnsembleText = ensembleText;
    targets = updateEnsemble(ensembleText);

    ensemble$.empty().text(
      targets
        .map(function(target) {
          return target.host;
        })
        .join(" ")
    );
  }

  var statusContainer$ = $(".box_status");

  statusContainer$.find("#refresh_ms").text(refreshDelayMs + " ms");
  status$ = statusContainer$.find("#refresh_status");

  if (!targets.length) {
    status$.text("IDLE");
    return;
  }

  status$.text("FETCHING");

  var barrier = targets.length + 1;
  var reports = new Array(targets.length);

  function done() {
    if (--barrier === 0) {
      if (targets === metricsTargets) {
        updateStatsTable(targets, reports);

        for (var i = 0; i < targets.length; i++) {
          targets[i].lastReport = reports[i];
        }
      }

      status$.text("IDLE");

      if (refreshDelayMs > 0) {
        refreshTimeoutId = setTimeout(updateStats, refreshDelayMs);
      }
    }
  }

  targets.forEach(function(target, index) {
    var url;

    if (false) {
      // Direct access doesn't work (no configurable CORS policy on ZK.)
      var hostPort = target.host;
      if (metricsPort) {
        hostPort += ":" + metricsPort;
      }

      url = metricsScheme + "://" + hostPort + "/commands/monitor";
    } else {
      url = "/zk-metrics/proxy/monitor/" + index;
    }

    $.get(url, function(data) {
      try {
        var report = JSON.parse(data);

        reports[index] = report;

        updateCharts(target, targets.length, report);
      } catch (e) {
        console.log({ l: "updateStats/get", url: url, error: e });
      }

      done();
    }).fail(function() {
      done();
    });
  });

  done();
}
updateStats();
