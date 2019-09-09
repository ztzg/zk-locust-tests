metrics_3_5 = [
    # Via MonitorCommand in Commands.java.
    {
        'name': 'latency',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'packets_received',
        'kind': 'Counter'
    },
    {
        'name': 'packets_sent',
        'kind': 'Counter'
    },
    {
        'name': 'num_alive_connections',
        'kind': 'Gauge'
    },
    {
        'name': 'outstanding_requests',
        'kind': 'Gauge'
    },
    {
        'name': 'znode_count',
        'kind': 'Gauge'
    },
    {
        'name': 'watch_count',
        'kind': 'Gauge'
    },
    {
        'name': 'ephemerals_count',
        'kind': 'Gauge'
    },
    {
        'name': 'approximate_data_size',
        'kind': 'Gauge'
    },
    {
        'name': 'open_file_descriptor_count',
        'kind': 'Gauge'
    },
    {
        'name': 'max_file_descriptor_count',
        'kind': 'Gauge'
    },
    {
        'name': 'last_client_response_size',
        'kind': 'Gauge'
    },
    {
        'name': 'max_client_response_size',
        'kind': 'Gauge'
    },
    {
        'name': 'min_client_response_size',
        'kind': 'Gauge'
    }
]

metrics_3_6 = [
    # Via registerGauge in ZooKeeperServer.java; tweaked.
    {
        'name': 'latency',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'packets_received',
        'kind': 'Counter'
    },
    {
        'name': 'packets_sent',
        'kind': 'Counter'
    },
    {
        'name': 'num_alive_connections',
        'kind': 'Gauge'
    },
    {
        'name': 'outstanding_requests',
        'kind': 'Gauge'
    },
    {
        'name': 'uptime',
        'kind': 'Counter'
    },
    {
        'name': 'znode_count',
        'kind': 'Gauge'
    },
    {
        'name': 'watch_count',
        'kind': 'Gauge'
    },
    {
        'name': 'ephemerals_count',
        'kind': 'Gauge'
    },
    {
        'name': 'approximate_data_size',
        'kind': 'Gauge'
    },
    {
        'name': 'global_sessions',
        'kind': 'Gauge'
    },
    {
        'name': 'local_sessions',
        'kind': 'Gauge'
    },
    {
        'name': 'open_file_descriptor_count',
        'kind': 'Gauge'
    },
    {
        'name': 'max_file_descriptor_count',
        'kind': 'Gauge'
    },
    {
        'name': 'connection_drop_probability',
        'kind': 'Gauge'
    },
    {
        'name': 'last_client_response_size',
        'kind': 'Gauge'
    },
    {
        'name': 'max_client_response_size',
        'kind': 'Gauge'
    },
    {
        'name': 'min_client_response_size',
        'kind': 'Gauge'
    },

    # Via registerGauge in FollowerZooKeeperServer.java; tweaked.
    {
        'name': 'synced_observers',
        'kind': 'Gauge'
    },

    # Via registerGauge in LeaderZooKeeperServer.java; tweaked.
    {
        'name': 'learners',
        'kind': 'Gauge'
    },
    {
        'name': 'synced_followers',
        'kind': 'Gauge'
    },
    {
        'name': 'synced_non_voting_followers',
        'kind': 'Gauge'
    },
    {
        'name': 'synced_observers',
        'kind': 'Gauge'
    },
    {
        'name': 'pending_syncs',
        'kind': 'Gauge'
    },
    {
        'name': 'leader_uptime',
        'kind': 'Counter'
    },
    {
        'name': 'last_proposal_size',
        'kind': 'Gauge'
    },
    {
        'name': 'max_proposal_size',
        'kind': 'Gauge'
    },
    {
        'name': 'min_proposal_size',
        'kind': 'Gauge'
    },

    # Via registerGauge in QuorumZooKeeperServer.java; tweaked.
    {
        'name': 'quorum_size',
        'kind': 'Gauge'
    },

    # Via ServerMetrics
    {
        'name': 'fsynctime',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'snapshottime',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'dbinittime',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'readlatency',
        'kind': 'Summary'
    },
    {
        'name': 'updatelatency',
        'kind': 'Summary'
    },
    {
        'name': 'propagation_latency',
        'kind': 'Summary'
    },
    {
        'name': 'follower_sync_time',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'election_time',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'looking_count',
        'kind': 'Counter'
    },
    {
        'name': 'diff_count',
        'kind': 'Counter'
    },
    {
        'name': 'snap_count',
        'kind': 'Counter'
    },
    {
        'name': 'commit_count',
        'kind': 'Counter'
    },
    {
        'name': 'connection_request_count',
        'kind': 'Counter'
    },
    {
        'name': 'connection_token_deficit',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'connection_rejected',
        'kind': 'Counter'
    },
    {
        'name': 'write_per_namespace',
        'kind': 'Summary Set (Basic)'
    },
    {
        'name': 'read_per_namespace',
        'kind': 'Summary Set (Basic)'
    },
    {
        'name': 'bytes_received_count',
        'kind': 'Counter'
    },
    {
        'name': 'unrecoverable_error_count',
        'kind': 'Counter'
    },
    {
        'name': 'node_created_watch_count',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'node_deleted_watch_count',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'node_changed_watch_count',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'node_children_watch_count',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'add_dead_watcher_stall_time',
        'kind': 'Counter'
    },
    {
        'name': 'dead_watchers_queued',
        'kind': 'Counter'
    },
    {
        'name': 'dead_watchers_cleared',
        'kind': 'Counter'
    },
    {
        'name': 'dead_watchers_cleaner_latency',
        'kind': 'Summary'
    },
    {
        'name': 'response_packet_cache_hits',
        'kind': 'Counter'
    },
    {
        'name': 'response_packet_cache_misses',
        'kind': 'Counter'
    },
    {
        'name': 'ensemble_auth_success',
        'kind': 'Counter'
    },
    {
        'name': 'ensemble_auth_fail',
        'kind': 'Counter'
    },
    {
        'name': 'ensemble_auth_skip',
        'kind': 'Counter'
    },
    {
        'name': 'prep_processor_queue_time_ms',
        'kind': 'Summary'
    },
    {
        'name': 'prep_processor_queue_size',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'prep_processor_request_queued',
        'kind': 'Counter'
    },
    {
        'name': 'outstanding_changes_queued',
        'kind': 'Counter'
    },
    {
        'name': 'outstanding_changes_removed',
        'kind': 'Counter'
    },
    {
        'name': 'prep_process_time',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'close_session_prep_time',
        'kind': 'Summary'
    },
    {
        'name': 'revalidate_count',
        'kind': 'Counter'
    },
    {
        'name': 'connection_drop_count',
        'kind': 'Counter'
    },
    {
        'name': 'connection_revalidate_count',
        'kind': 'Counter'
    },
    {
        'name': 'sessionless_connections_expired',
        'kind': 'Counter'
    },
    {
        'name': 'stale_sessions_expired',
        'kind': 'Counter'
    },
    {
        'name': 'requests_in_session_queue',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'pending_session_queue_size',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'reads_after_write_in_session_queue',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'reads_issued_from_session_queue',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'session_queues_drained',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'time_waiting_empty_pool_in_commit_processor_read_ms',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'write_batch_time_in_commit_processor',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'concurrent_request_processing_in_commit_processor',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'read_commit_proc_req_queued',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'write_commit_proc_req_queued',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'commit_commit_proc_req_queued',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'request_commit_queued',
        'kind': 'Counter'
    },
    {
        'name': 'read_commit_proc_issued',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'write_commit_proc_issued',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'read_commitproc_time_ms',
        'kind': 'Summary'
    },
    {
        'name': 'write_commitproc_time_ms',
        'kind': 'Summary'
    },
    {
        'name': 'local_write_committed_time_ms',
        'kind': 'Summary'
    },
    {
        'name': 'server_write_committed_time_ms',
        'kind': 'Summary'
    },
    {
        'name': 'commit_process_time',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'om_proposal_process_time_ms',
        'kind': 'Summary'
    },
    {
        'name': 'om_commit_process_time_ms',
        'kind': 'Summary'
    },
    {
        'name': 'read_final_proc_time_ms',
        'kind': 'Summary'
    },
    {
        'name': 'write_final_proc_time_ms',
        'kind': 'Summary'
    },
    {
        'name': 'proposal_latency',
        'kind': 'Summary'
    },
    {
        'name': 'proposal_ack_creation_latency',
        'kind': 'Summary'
    },
    {
        'name': 'commit_propagation_latency',
        'kind': 'Summary'
    },
    {
        'name': 'learner_proposal_received_count',
        'kind': 'Counter'
    },
    {
        'name': 'learner_commit_received_count',
        'kind': 'Counter'
    },
    {
        'name': 'learner_handler_qp_size',
        'kind': 'Summary Set (Basic)'
    },
    {
        'name': 'learner_handler_qp_time_ms',
        'kind': 'Summary Set'
    },
    {
        'name': 'startup_txns_loaded',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'startup_txns_load_time',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'startup_snap_load_time',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'sync_processor_queue_and_flush_time_ms',
        'kind': 'Summary'
    },
    {
        'name': 'sync_processor_queue_size',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'sync_processor_request_queued',
        'kind': 'Counter'
    },
    {
        'name': 'sync_processor_queue_time_ms',
        'kind': 'Summary'
    },
    {
        'name': 'sync_processor_queue_flush_time_ms',
        'kind': 'Summary'
    },
    {
        'name': 'sync_process_time',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'sync_processor_batch_size',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'quorum_ack_latency',
        'kind': 'Summary'
    },
    {
        'name': 'ack_latency',
        'kind': 'Summary Set'
    },
    {
        'name': 'proposal_count',
        'kind': 'Counter'
    },
    {
        'name': 'quit_leading_due_to_disloyal_voter',
        'kind': 'Counter'
    },
    {
        'name': 'stale_requests',
        'kind': 'Counter'
    },
    {
        'name': 'stale_requests_dropped',
        'kind': 'Counter'
    },
    {
        'name': 'stale_replies',
        'kind': 'Counter'
    },
    {
        'name': 'request_throttle_wait_count',
        'kind': 'Counter'
    },
    {
        'name': 'netty_queued_buffer_capacity',
        'kind': 'Summary (Basic)'
    },
    {
        'name': 'digest_mismatches_count',
        'kind': 'Counter'
    }
]

metric_defs = {
    '3.5': metrics_3_5,
    '3.6': metrics_3_6,
}
