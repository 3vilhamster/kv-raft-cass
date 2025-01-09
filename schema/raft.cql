-- schema.cql
-- Raft storage schema with 1-day retention optimizations

-- Create keyspace if it doesn't exist
CREATE KEYSPACE IF NOT EXISTS raft_storage
    WITH replication = {
        'class': 'SimpleStrategy',
        'replication_factor': 1
        };

USE raft_storage;

-- Log entries table with TTL and time-window compaction
CREATE TABLE IF NOT EXISTS raft_entries (
                                            namespace_id text,
                                            snap_index bigint,
                                            term bigint,
                                            entry_type int,
                                            data blob,
                                            created_at timestamp,
                                            PRIMARY KEY ((namespace_id), snap_index)
) WITH CLUSTERING ORDER BY (snap_index ASC)
   AND gc_grace_seconds = 3600
   AND default_time_to_live = 86400
   AND compaction = {
            'class': 'TimeWindowCompactionStrategy',
            'compaction_window_size': 1,
            'compaction_window_unit': 'HOURS'
            };

-- Hard state table for current term and vote
CREATE TABLE IF NOT EXISTS raft_state (
                                          namespace_id text,
                                          term bigint,
                                          vote bigint,
                                          commit bigint,
                                          last_updated timestamp,
                                          PRIMARY KEY (namespace_id)
) WITH gc_grace_seconds = 3600;

-- Configuration state table for cluster membership
CREATE TABLE IF NOT EXISTS raft_membership (
                                               namespace_id text,
                                               node_id bigint,
                                               address text,
                                               metadata text,
                                               PRIMARY KEY ((namespace_id), node_id)
) WITH gc_grace_seconds = 3600;

-- Snapshots table with TTL
CREATE TABLE IF NOT EXISTS raft_snapshots (
                                              namespace_id text,
                                              snap_index bigint,
                                              term bigint,
                                              data blob,
                                              conf_state blob,
                                              created_at timestamp,
                                              PRIMARY KEY ((namespace_id), snap_index)
) WITH CLUSTERING ORDER BY (snap_index DESC)
   AND gc_grace_seconds = 3600
   AND default_time_to_live = 86400;

-- Create indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_log_entries_created_at ON raft_entries (created_at);
CREATE INDEX IF NOT EXISTS idx_snapshots_created_at ON raft_snapshots (created_at);
