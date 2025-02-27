/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package inst

import (
	"fmt"
	"regexp"
	"time"

	"github.com/openark/orchestrator/go/config"
	"github.com/openark/orchestrator/go/db"
	"github.com/openark/orchestrator/go/process"
	"github.com/openark/orchestrator/go/raft"
	"github.com/openark/orchestrator/go/util"

	"github.com/openark/golib/log"
	"github.com/openark/golib/sqlutils"
	"github.com/patrickmn/go-cache"
	"github.com/rcrowley/go-metrics"
)

var analysisChangeWriteAttemptCounter = metrics.NewCounter()
var analysisChangeWriteCounter = metrics.NewCounter()

var recentInstantAnalysis *cache.Cache

func init() {
	metrics.Register("analysis.change.write.attempt", analysisChangeWriteAttemptCounter)
	metrics.Register("analysis.change.write", analysisChangeWriteCounter)

	go initializeAnalysisDaoPostConfiguration()
}

func initializeAnalysisDaoPostConfiguration() {
	config.WaitForConfigurationToBeLoaded()

	recentInstantAnalysis = cache.New(time.Duration(config.RecoveryPollSeconds*2)*time.Second, time.Second)
}

// GetReplicationAnalysis will check for replication problems (dead master; unreachable master; etc)
// GetReplicationAnalysis 将检查复制问题  (dead master; unreachable master; 等，查询SQL，根据实例的状态确定故障或者警告类型
func GetReplicationAnalysis(clusterName string, hints *ReplicationAnalysisHints) ([]ReplicationAnalysis, error) {
	result := []ReplicationAnalysis{}

	args := sqlutils.Args(config.Config.ReasonableLockedSemiSyncMasterSeconds, ValidSecondsFromSeenToLastAttemptedCheck(), config.Config.ReasonableReplicationLagSeconds, clusterName)
	analysisQueryReductionClause := ``

	if config.Config.ReduceReplicationAnalysisCount {
		analysisQueryReductionClause = `
			HAVING
				(
					MIN(
						master_instance.last_checked <= master_instance.last_seen
						and master_instance.last_attempted_check <= master_instance.last_seen + interval ? second
					) = 1
					/* AS is_last_check_valid */
				) = 0
				OR (
					IFNULL(
						SUM(
							replica_instance.last_checked <= replica_instance.last_seen
							AND replica_instance.slave_io_running = 0
							AND replica_instance.last_io_error like '%error %connecting to master%'
							AND replica_instance.slave_sql_running = 1
						),
						0
					)
					/* AS count_replicas_failing_to_connect_to_master */
					> 0
				)
				OR (
					IFNULL(
						SUM(
							replica_instance.last_checked <= replica_instance.last_seen
						),
						0
					)
					/* AS count_valid_replicas */
					< COUNT(replica_instance.server_id)
					/* AS count_replicas */
				)
				OR (
					IFNULL(
						SUM(
							replica_instance.last_checked <= replica_instance.last_seen
							AND replica_instance.slave_io_running != 0
							AND replica_instance.slave_sql_running != 0
						),
						0
					)
					/* AS count_valid_replicating_replicas */
					< COUNT(replica_instance.server_id)
					/* AS count_replicas */
				)
				OR (
					MIN(
						master_instance.slave_sql_running = 1
						AND master_instance.slave_io_running = 0
						AND master_instance.last_io_error like '%error %connecting to master%'
					)
					/* AS is_failing_to_connect_to_master */
				)
				OR (
					COUNT(replica_instance.server_id)
					/* AS count_replicas */
					> 0
				)
			`
		args = append(args, ValidSecondsFromSeenToLastAttemptedCheck())
	}
	// "OR count_replicas > 0" above is a recent addition, which, granted, makes some previous conditions redundant.
	// It gives more output, and more "NoProblem" messages that I am now interested in for purpose of auditing in database_instance_analysis_changelog
	query := fmt.Sprintf(`
	SELECT
		master_instance.hostname,
		master_instance.port,
		master_instance.read_only AS read_only,
		MIN(master_instance.data_center) AS data_center,
		MIN(master_instance.region) AS region,
		MIN(master_instance.physical_environment) AS physical_environment,
		MIN(master_instance.master_host) AS master_host,
		MIN(master_instance.master_port) AS master_port,
		MIN(master_instance.cluster_name) AS cluster_name,
		MIN(master_instance.binary_log_file) AS binary_log_file,
		MIN(master_instance.binary_log_pos) AS binary_log_pos,
		MIN(
			IFNULL(
				master_instance.binary_log_file = database_instance_stale_binlog_coordinates.binary_log_file
				AND master_instance.binary_log_pos = database_instance_stale_binlog_coordinates.binary_log_pos
				AND database_instance_stale_binlog_coordinates.first_seen < NOW() - interval ? second,
				0
			)
		) AS is_stale_binlog_coordinates,
		MIN(
			IFNULL(
				cluster_alias.alias,
				master_instance.cluster_name
			)
		) AS cluster_alias,
		MIN(
			IFNULL(
				cluster_domain_name.domain_name,
				master_instance.cluster_name
			)
		) AS cluster_domain,
		MIN(
			master_instance.last_checked <= master_instance.last_seen
			and master_instance.last_attempted_check <= master_instance.last_seen + interval ? second
		) = 1 AS is_last_check_valid,
		/* To be considered a master, traditional async replication must not be present/valid AND the host should either */
		/* not be a replication group member OR be the primary of the replication group */
		MIN(master_instance.last_check_partial_success) as last_check_partial_success,
		MIN(
			(
				master_instance.master_host IN ('', '_')
				OR master_instance.master_port = 0
				OR substr(master_instance.master_host, 1, 2) = '//'
			)
			AND (
				master_instance.replication_group_name = ''
				OR master_instance.replication_group_member_role = 'PRIMARY'
			)
		) AS is_master,
		-- A host is not a group member if it has no replication group name OR if it does, but its state in the group is
		-- OFFLINE (e.g. some GR configuration is in place but the host has not actually joined a group yet. Notice that
		-- we DO consider it a group member if its state is ERROR (which is what happens when it gets expelled from the
		-- group)
		MIN(
			master_instance.replication_group_name != ''
			AND master_instance.replication_group_member_state != 'OFFLINE'
		) AS is_replication_group_member,
		MIN(master_instance.is_co_master) AS is_co_master,
		MIN(
			CONCAT(
				master_instance.hostname,
				':',
				master_instance.port
			) = master_instance.cluster_name
		) AS is_cluster_master,
		MIN(master_instance.gtid_mode) AS gtid_mode,
		COUNT(replica_instance.server_id) AS count_replicas,
		IFNULL(
			SUM(
				replica_instance.last_checked <= replica_instance.last_seen
			),
			0
		) AS count_valid_replicas,
		IFNULL(
			SUM(
				replica_instance.last_checked <= replica_instance.last_seen
				AND replica_instance.slave_io_running != 0
				AND replica_instance.slave_sql_running != 0
			),
			0
		) AS count_valid_replicating_replicas,
		IFNULL(
			SUM(
				replica_instance.last_checked <= replica_instance.last_seen
				AND replica_instance.slave_io_running = 0
				AND replica_instance.last_io_error like '%%error %%connecting to master%%'
				AND replica_instance.slave_sql_running = 1
			),
			0
		) AS count_replicas_failing_to_connect_to_master,
		MIN(master_instance.replication_depth) AS replication_depth,
		GROUP_CONCAT(
			concat(
				replica_instance.Hostname,
				':',
				replica_instance.Port
			)
		) as slave_hosts,
		MIN(
			master_instance.slave_sql_running = 1
			AND master_instance.slave_io_running = 0
			AND master_instance.last_io_error like '%%error %%connecting to master%%'
		) AS is_failing_to_connect_to_master,
		MIN(
			master_downtime.downtime_active is not null
			and ifnull(master_downtime.end_timestamp, now()) > now()
		) AS is_downtimed,
		MIN(
			IFNULL(master_downtime.end_timestamp, '')
		) AS downtime_end_timestamp,
		MIN(
			IFNULL(
				unix_timestamp() - unix_timestamp(master_downtime.end_timestamp),
				0
			)
		) AS downtime_remaining_seconds,
		MIN(
			master_instance.binlog_server
		) AS is_binlog_server,
		MIN(master_instance.pseudo_gtid) AS is_pseudo_gtid,
		MIN(
			master_instance.supports_oracle_gtid
		) AS supports_oracle_gtid,
		MIN(
			master_instance.semi_sync_master_enabled
		) AS semi_sync_master_enabled,
		MIN(
			master_instance.semi_sync_master_wait_for_slave_count
		) AS semi_sync_master_wait_for_slave_count,
		MIN(
			master_instance.semi_sync_master_clients
		) AS semi_sync_master_clients,
		MIN(
			master_instance.semi_sync_master_status
		) AS semi_sync_master_status,
		SUM(replica_instance.is_co_master) AS count_co_master_replicas,
		SUM(replica_instance.oracle_gtid) AS count_oracle_gtid_replicas,
		IFNULL(
			SUM(
				replica_instance.last_checked <= replica_instance.last_seen
				AND replica_instance.oracle_gtid != 0
			),
			0
		) AS count_valid_oracle_gtid_replicas,
		SUM(
			replica_instance.binlog_server
		) AS count_binlog_server_replicas,
		IFNULL(
			SUM(
				replica_instance.last_checked <= replica_instance.last_seen
				AND replica_instance.binlog_server != 0
			),
			0
		) AS count_valid_binlog_server_replicas,
		SUM(
			replica_instance.semi_sync_replica_enabled
		) AS count_semi_sync_replicas,
		IFNULL(
			SUM(
				replica_instance.last_checked <= replica_instance.last_seen
				AND replica_instance.semi_sync_replica_enabled != 0
			),
			0
		) AS count_valid_semi_sync_replicas,
		MIN(
			master_instance.mariadb_gtid
		) AS is_mariadb_gtid,
		SUM(replica_instance.mariadb_gtid) AS count_mariadb_gtid_replicas,
		IFNULL(
			SUM(
				replica_instance.last_checked <= replica_instance.last_seen
				AND replica_instance.mariadb_gtid != 0
			),
			0
		) AS count_valid_mariadb_gtid_replicas,
		IFNULL(
			SUM(
				replica_instance.log_bin
				AND replica_instance.log_slave_updates
			),
			0
		) AS count_logging_replicas,
		IFNULL(
			SUM(
				replica_instance.log_bin
				AND replica_instance.log_slave_updates
				AND replica_instance.binlog_format = 'STATEMENT'
			),
			0
		) AS count_statement_based_logging_replicas,
		IFNULL(
			SUM(
				replica_instance.log_bin
				AND replica_instance.log_slave_updates
				AND replica_instance.binlog_format = 'MIXED'
			),
			0
		) AS count_mixed_based_logging_replicas,
		IFNULL(
			SUM(
				replica_instance.log_bin
				AND replica_instance.log_slave_updates
				AND replica_instance.binlog_format = 'ROW'
			),
			0
		) AS count_row_based_logging_replicas,
		IFNULL(
			SUM(replica_instance.sql_delay > 0),
			0
		) AS count_delayed_replicas,
		IFNULL(
			SUM(replica_instance.slave_lag_seconds > ?),
			0
		) AS count_lagging_replicas,
		IFNULL(MIN(replica_instance.gtid_mode), '') AS min_replica_gtid_mode,
		IFNULL(MAX(replica_instance.gtid_mode), '') AS max_replica_gtid_mode,
		IFNULL(
			MAX(
				case when replica_downtime.downtime_active is not null
				and ifnull(replica_downtime.end_timestamp, now()) > now() then '' else replica_instance.gtid_errant end
			),
			''
		) AS max_replica_gtid_errant,
		IFNULL(
			SUM(
				replica_downtime.downtime_active is not null
				and ifnull(replica_downtime.end_timestamp, now()) > now()
			),
			0
		) AS count_downtimed_replicas,
		COUNT(
			DISTINCT case when replica_instance.log_bin
			AND replica_instance.log_slave_updates then replica_instance.major_version else NULL end
		) AS count_distinct_logging_major_versions
	FROM
		database_instance master_instance
		LEFT JOIN hostname_resolve ON (
			master_instance.hostname = hostname_resolve.hostname
		)
		LEFT JOIN database_instance replica_instance ON (
			COALESCE(
				hostname_resolve.resolved_hostname,
				master_instance.hostname
			) = replica_instance.master_host
			AND master_instance.port = replica_instance.master_port
		)
		LEFT JOIN database_instance_maintenance ON (
			master_instance.hostname = database_instance_maintenance.hostname
			AND master_instance.port = database_instance_maintenance.port
			AND database_instance_maintenance.maintenance_active = 1
		)
		LEFT JOIN database_instance_stale_binlog_coordinates ON (
			master_instance.hostname = database_instance_stale_binlog_coordinates.hostname
			AND master_instance.port = database_instance_stale_binlog_coordinates.port
		)
		LEFT JOIN database_instance_downtime as master_downtime ON (
			master_instance.hostname = master_downtime.hostname
			AND master_instance.port = master_downtime.port
			AND master_downtime.downtime_active = 1
		)
		LEFT JOIN database_instance_downtime as replica_downtime ON (
			replica_instance.hostname = replica_downtime.hostname
			AND replica_instance.port = replica_downtime.port
			AND replica_downtime.downtime_active = 1
		)
		LEFT JOIN cluster_alias ON (
			cluster_alias.cluster_name = master_instance.cluster_name
		)
		LEFT JOIN cluster_domain_name ON (
			cluster_domain_name.cluster_name = master_instance.cluster_name
		)
	WHERE
		database_instance_maintenance.database_instance_maintenance_id IS NULL
		AND ? IN ('', master_instance.cluster_name)
	GROUP BY
		master_instance.hostname,
		master_instance.port
		%s
	ORDER BY
		is_master DESC,
		is_cluster_master DESC,
		count_replicas DESC
	`,
		analysisQueryReductionClause)

	err := db.QueryOrchestrator(query, args, func(m sqlutils.RowMap) error {
		a := ReplicationAnalysis{
			Analysis:               NoProblem,
			ProcessingNodeHostname: process.ThisHostname,
			ProcessingNodeToken:    util.ProcessToken.Hash,
		}

		a.IsMaster = m.GetBool("is_master")
		a.IsReplicationGroupMember = m.GetBool("is_replication_group_member")
		countCoMasterReplicas := m.GetUint("count_co_master_replicas")
		a.IsCoMaster = m.GetBool("is_co_master") || (countCoMasterReplicas > 0)
		a.AnalyzedInstanceKey = InstanceKey{Hostname: m.GetString("hostname"), Port: m.GetInt("port")}
		a.AnalyzedInstanceMasterKey = InstanceKey{Hostname: m.GetString("master_host"), Port: m.GetInt("master_port")}
		a.AnalyzedInstanceDataCenter = m.GetString("data_center")
		a.AnalyzedInstanceRegion = m.GetString("region")
		a.AnalyzedInstancePhysicalEnvironment = m.GetString("physical_environment")
		a.AnalyzedInstanceBinlogCoordinates = BinlogCoordinates{
			LogFile: m.GetString("binary_log_file"),
			LogPos:  m.GetInt64("binary_log_pos"),
			Type:    BinaryLog,
		}
		isStaleBinlogCoordinates := m.GetBool("is_stale_binlog_coordinates")
		a.ClusterDetails.ClusterName = m.GetString("cluster_name")
		a.ClusterDetails.ClusterAlias = m.GetString("cluster_alias")
		a.ClusterDetails.ClusterDomain = m.GetString("cluster_domain")
		a.GTIDMode = m.GetString("gtid_mode")
		a.LastCheckValid = m.GetBool("is_last_check_valid")
		a.LastCheckPartialSuccess = m.GetBool("last_check_partial_success")
		a.CountReplicas = m.GetUint("count_replicas")
		a.CountValidReplicas = m.GetUint("count_valid_replicas")
		a.CountValidReplicatingReplicas = m.GetUint("count_valid_replicating_replicas")
		a.CountReplicasFailingToConnectToMaster = m.GetUint("count_replicas_failing_to_connect_to_master")
		a.CountDowntimedReplicas = m.GetUint("count_downtimed_replicas")
		a.ReplicationDepth = m.GetUint("replication_depth")
		a.IsFailingToConnectToMaster = m.GetBool("is_failing_to_connect_to_master")
		a.IsDowntimed = m.GetBool("is_downtimed")
		a.DowntimeEndTimestamp = m.GetString("downtime_end_timestamp")
		a.DowntimeRemainingSeconds = m.GetInt("downtime_remaining_seconds")
		a.IsBinlogServer = m.GetBool("is_binlog_server")
		a.ClusterDetails.ReadRecoveryInfo()

		a.Replicas = *NewInstanceKeyMap()
		a.Replicas.ReadCommaDelimitedList(m.GetString("slave_hosts"))

		countValidOracleGTIDReplicas := m.GetUint("count_valid_oracle_gtid_replicas")
		a.OracleGTIDImmediateTopology = countValidOracleGTIDReplicas == a.CountValidReplicas && a.CountValidReplicas > 0
		countValidMariaDBGTIDReplicas := m.GetUint("count_valid_mariadb_gtid_replicas")
		a.MariaDBGTIDImmediateTopology = countValidMariaDBGTIDReplicas == a.CountValidReplicas && a.CountValidReplicas > 0
		countValidBinlogServerReplicas := m.GetUint("count_valid_binlog_server_replicas")
		a.BinlogServerImmediateTopology = countValidBinlogServerReplicas == a.CountValidReplicas && a.CountValidReplicas > 0
		a.PseudoGTIDImmediateTopology = m.GetBool("is_pseudo_gtid")
		a.SemiSyncMasterEnabled = m.GetBool("semi_sync_master_enabled")
		a.SemiSyncMasterStatus = m.GetBool("semi_sync_master_status")
		a.CountSemiSyncReplicasEnabled = m.GetUint("count_semi_sync_replicas")
		// countValidSemiSyncReplicasEnabled := m.GetUint("count_valid_semi_sync_replicas")
		a.SemiSyncMasterWaitForReplicaCount = m.GetUint("semi_sync_master_wait_for_slave_count")
		a.SemiSyncMasterClients = m.GetUint("semi_sync_master_clients")

		a.MinReplicaGTIDMode = m.GetString("min_replica_gtid_mode")
		a.MaxReplicaGTIDMode = m.GetString("max_replica_gtid_mode")
		a.MaxReplicaGTIDErrant = m.GetString("max_replica_gtid_errant")

		a.CountLoggingReplicas = m.GetUint("count_logging_replicas")
		a.CountStatementBasedLoggingReplicas = m.GetUint("count_statement_based_logging_replicas")
		a.CountMixedBasedLoggingReplicas = m.GetUint("count_mixed_based_logging_replicas")
		a.CountRowBasedLoggingReplicas = m.GetUint("count_row_based_logging_replicas")
		a.CountDistinctMajorVersionsLoggingReplicas = m.GetUint("count_distinct_logging_major_versions")

		a.CountDelayedReplicas = m.GetUint("count_delayed_replicas")
		a.CountLaggingReplicas = m.GetUint("count_lagging_replicas")

		a.IsReadOnly = m.GetUint("read_only") == 1

		if !a.LastCheckValid {
			analysisMessage := fmt.Sprintf("analysis: ClusterName: %+v, IsMaster: %+v, LastCheckValid: %+v, LastCheckPartialSuccess: %+v, CountReplicas: %+v, CountValidReplicas: %+v, CountValidReplicatingReplicas: %+v, CountLaggingReplicas: %+v, CountDelayedReplicas: %+v, CountReplicasFailingToConnectToMaster: %+v",
				a.ClusterDetails.ClusterName, a.IsMaster, a.LastCheckValid, a.LastCheckPartialSuccess, a.CountReplicas, a.CountValidReplicas, a.CountValidReplicatingReplicas, a.CountLaggingReplicas, a.CountDelayedReplicas, a.CountReplicasFailingToConnectToMaster,
			)
			if util.ClearToLog("analysis_dao", analysisMessage) {
				log.Debugf(analysisMessage)
			}
		}
		if !a.IsReplicationGroupMember /* Traditional Async/Semi-sync replication issue detection */ {
			if a.IsMaster && !a.LastCheckValid && a.CountReplicas == 0 {
				a.Analysis = DeadMasterWithoutReplicas
				a.Description = "Master cannot be reached by orchestrator and has no replica"
				//
			} else if a.IsMaster && !a.LastCheckValid && a.CountValidReplicas == a.CountReplicas && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = DeadMaster
				a.Description = "Master cannot be reached by orchestrator and none of its replicas is replicating"
				//
			} else if a.IsMaster && !a.LastCheckValid && a.CountReplicas > 0 && a.CountValidReplicas == 0 && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = DeadMasterAndReplicas
				a.Description = "Master cannot be reached by orchestrator and none of its replicas is replicating"
				//
			} else if a.IsMaster && !a.LastCheckValid && a.CountValidReplicas < a.CountReplicas && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = DeadMasterAndSomeReplicas
				a.Description = "Master cannot be reached by orchestrator; some of its replicas are unreachable and none of its reachable replicas is replicating"
				//
			} else if a.IsMaster && !a.LastCheckValid && a.CountLaggingReplicas == a.CountReplicas && a.CountDelayedReplicas < a.CountReplicas && a.CountValidReplicatingReplicas > 0 {
				a.Analysis = UnreachableMasterWithLaggingReplicas
				a.Description = "Master cannot be reached by orchestrator and all of its replicas are lagging"
				//
			} else if a.IsMaster && !a.LastCheckValid && !a.LastCheckPartialSuccess && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas > 0 {
				// partial success is here to reduce noise
				a.Analysis = UnreachableMaster
				a.Description = "Master cannot be reached by orchestrator but it has replicating replicas; possibly a network/host issue"
				//
			} else if a.IsMaster && !a.LastCheckValid && a.LastCheckPartialSuccess && a.CountReplicasFailingToConnectToMaster > 0 && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas > 0 {
				// there's partial success, but also at least one replica is failing to connect to master
				a.Analysis = UnreachableMaster
				a.Description = "Master cannot be reached by orchestrator but it has replicating replicas; possibly a network/host issue"
				//
			} else if a.IsMaster && a.SemiSyncMasterEnabled && a.SemiSyncMasterStatus && a.SemiSyncMasterWaitForReplicaCount > 0 && a.SemiSyncMasterClients < a.SemiSyncMasterWaitForReplicaCount {
				if isStaleBinlogCoordinates {
					a.Analysis = LockedSemiSyncMaster
					a.Description = "Semi sync master is locked since it doesn't get enough replica acknowledgements"
				} else {
					a.Analysis = LockedSemiSyncMasterHypothesis
					a.Description = "Semi sync master seems to be locked, more samplings needed to validate"
				}
				//
			} else if config.Config.EnforceExactSemiSyncReplicas && a.IsMaster && a.SemiSyncMasterEnabled && a.SemiSyncMasterStatus && a.SemiSyncMasterWaitForReplicaCount > 0 && a.SemiSyncMasterClients > a.SemiSyncMasterWaitForReplicaCount {
				a.Analysis = MasterWithTooManySemiSyncReplicas
				a.Description = "Semi sync master has more semi sync replicas than configured"
				//
			} else if a.IsMaster && a.LastCheckValid && a.IsReadOnly && a.CountValidReplicatingReplicas > 0 && config.Config.RecoverNonWriteableMaster {
				a.Analysis = NoWriteableMasterStructureWarning
				a.Description = "Master with replicas is read_only"
				//
			} else if a.IsMaster && a.LastCheckValid && a.CountReplicas == 1 && a.CountValidReplicas == a.CountReplicas && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = MasterSingleReplicaNotReplicating
				a.Description = "Master is reachable but its single replica is not replicating"
				//
			} else if a.IsMaster && a.LastCheckValid && a.CountReplicas == 1 && a.CountValidReplicas == 0 {
				a.Analysis = MasterSingleReplicaDead
				a.Description = "Master is reachable but its single replica is dead"
				//
			} else if a.IsMaster && a.LastCheckValid && a.CountReplicas > 1 && a.CountValidReplicas == a.CountReplicas && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = AllMasterReplicasNotReplicating
				a.Description = "Master is reachable but none of its replicas is replicating"
				//
			} else if a.IsMaster && a.LastCheckValid && a.CountReplicas > 1 && a.CountValidReplicas < a.CountReplicas && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = AllMasterReplicasNotReplicatingOrDead
				a.Description = "Master is reachable but none of its replicas is replicating"
				//
			} else /* co-master */ if a.IsCoMaster && !a.LastCheckValid && a.CountReplicas > 0 && a.CountValidReplicas == a.CountReplicas && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = DeadCoMaster
				a.Description = "Co-master cannot be reached by orchestrator and none of its replicas is replicating"
				//
			} else if a.IsCoMaster && !a.LastCheckValid && a.CountReplicas > 0 && a.CountValidReplicas < a.CountReplicas && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = DeadCoMasterAndSomeReplicas
				a.Description = "Co-master cannot be reached by orchestrator; some of its replicas are unreachable and none of its reachable replicas is replicating"
				//
			} else if a.IsCoMaster && !a.LastCheckValid && !a.LastCheckPartialSuccess && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas > 0 {
				a.Analysis = UnreachableCoMaster
				a.Description = "Co-master cannot be reached by orchestrator but it has replicating replicas; possibly a network/host issue"
				//
			} else if a.IsCoMaster && a.LastCheckValid && a.CountReplicas > 0 && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = AllCoMasterReplicasNotReplicating
				a.Description = "Co-master is reachable but none of its replicas is replicating"
				//
			} else /* intermediate-master */ if !a.IsMaster && !a.LastCheckValid && a.CountReplicas == 1 && a.CountValidReplicas == a.CountReplicas && a.CountReplicasFailingToConnectToMaster == a.CountReplicas && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = DeadIntermediateMasterWithSingleReplicaFailingToConnect
				a.Description = "Intermediate master cannot be reached by orchestrator and its (single) replica is failing to connect"
				//
			} else if !a.IsMaster && !a.LastCheckValid && a.CountReplicas == 1 && a.CountValidReplicas == a.CountReplicas && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = DeadIntermediateMasterWithSingleReplica
				a.Description = "Intermediate master cannot be reached by orchestrator and its (single) replica is not replicating"
				//
			} else if !a.IsMaster && !a.LastCheckValid && a.CountReplicas > 1 && a.CountValidReplicas == a.CountReplicas && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = DeadIntermediateMaster
				a.Description = "Intermediate master cannot be reached by orchestrator and none of its replicas is replicating"
				//
			} else if !a.IsMaster && !a.LastCheckValid && a.CountValidReplicas < a.CountReplicas && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = DeadIntermediateMasterAndSomeReplicas
				a.Description = "Intermediate master cannot be reached by orchestrator; some of its replicas are unreachable and none of its reachable replicas is replicating"
				//
			} else if !a.IsMaster && !a.LastCheckValid && a.CountReplicas > 0 && a.CountValidReplicas == 0 {
				a.Analysis = DeadIntermediateMasterAndReplicas
				a.Description = "Intermediate master cannot be reached by orchestrator and all of its replicas are unreachable"
				//
			} else if !a.IsMaster && !a.LastCheckValid && a.CountLaggingReplicas == a.CountReplicas && a.CountDelayedReplicas < a.CountReplicas && a.CountValidReplicatingReplicas > 0 {
				a.Analysis = UnreachableIntermediateMasterWithLaggingReplicas
				a.Description = "Intermediate master cannot be reached by orchestrator and all of its replicas are lagging"
				//
			} else if !a.IsMaster && !a.LastCheckValid && !a.LastCheckPartialSuccess && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas > 0 {
				a.Analysis = UnreachableIntermediateMaster
				a.Description = "Intermediate master cannot be reached by orchestrator but it has replicating replicas; possibly a network/host issue"
				//
			} else if !a.IsMaster && a.LastCheckValid && a.CountReplicas > 1 && a.CountValidReplicatingReplicas == 0 &&
				a.CountReplicasFailingToConnectToMaster > 0 && a.CountReplicasFailingToConnectToMaster == a.CountValidReplicas {
				// All replicas are either failing to connect to master (and at least one of these have to exist)
				// or completely dead.
				// Must have at least two replicas to reach such conclusion -- do note that the intermediate master is still
				// reachable to orchestrator, so we base our conclusion on replicas only at this point.
				a.Analysis = AllIntermediateMasterReplicasFailingToConnectOrDead
				a.Description = "Intermediate master is reachable but all of its replicas are failing to connect"
				//
			} else if !a.IsMaster && a.LastCheckValid && a.CountReplicas > 0 && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = AllIntermediateMasterReplicasNotReplicating
				a.Description = "Intermediate master is reachable but none of its replicas is replicating"
				//
			} else if a.IsBinlogServer && a.IsFailingToConnectToMaster {
				a.Analysis = BinlogServerFailingToConnectToMaster
				a.Description = "Binlog server is unable to connect to its master"
				//
			} else if a.ReplicationDepth == 1 && a.IsFailingToConnectToMaster {
				a.Analysis = FirstTierReplicaFailingToConnectToMaster
				a.Description = "1st tier replica (directly replicating from topology master) is unable to connect to the master"
				//
			}
			//		 else if a.IsMaster && a.CountReplicas == 0 {
			//			a.Analysis = MasterWithoutReplicas
			//			a.Description = "Master has no replicas"
			//		}

		} else /* Group replication issue detection */ {
			// Group member is not reachable, has replicas, and none of its reachable replicas can replicate from it
			if !a.LastCheckValid && a.CountReplicas > 0 && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = DeadReplicationGroupMemberWithReplicas
				a.Description = "Group member is unreachable and all its reachable replicas are not replicating"
			}

		}
		appendAnalysis := func(analysis *ReplicationAnalysis) {
			if a.Analysis == NoProblem && len(a.StructureAnalysis) == 0 && !hints.IncludeNoProblem {
				return
			}
			for _, filter := range config.Config.RecoveryIgnoreHostnameFilters {
				if matched, _ := regexp.MatchString(filter, a.AnalyzedInstanceKey.Hostname); matched {
					return
				}
			}
			if a.IsDowntimed {
				a.SkippableDueToDowntime = true
			}
			if a.CountReplicas == a.CountDowntimedReplicas {
				switch a.Analysis {
				case AllMasterReplicasNotReplicating,
					AllMasterReplicasNotReplicatingOrDead,
					MasterSingleReplicaDead,
					AllCoMasterReplicasNotReplicating,
					DeadIntermediateMasterWithSingleReplica,
					DeadIntermediateMasterWithSingleReplicaFailingToConnect,
					DeadIntermediateMasterAndReplicas,
					DeadIntermediateMasterAndSomeReplicas,
					AllIntermediateMasterReplicasFailingToConnectOrDead,
					AllIntermediateMasterReplicasNotReplicating:
					a.IsReplicasDowntimed = true
					a.SkippableDueToDowntime = true
				}
			}
			if a.SkippableDueToDowntime && !hints.IncludeDowntimed {
				return
			}
			result = append(result, a)
		}

		{
			// Moving on to structure analysis
			// We also do structural checks. See if there's potential danger in promotions
			if a.IsMaster && a.CountLoggingReplicas == 0 && a.CountReplicas > 1 {
				a.StructureAnalysis = append(a.StructureAnalysis, NoLoggingReplicasStructureWarning)
			}
			if a.IsMaster && a.CountReplicas > 1 &&
				!a.OracleGTIDImmediateTopology &&
				!a.MariaDBGTIDImmediateTopology &&
				!a.BinlogServerImmediateTopology &&
				!a.PseudoGTIDImmediateTopology {
				a.StructureAnalysis = append(a.StructureAnalysis, NoFailoverSupportStructureWarning)
			}
			if a.IsMaster && a.CountStatementBasedLoggingReplicas > 0 && a.CountMixedBasedLoggingReplicas > 0 {
				a.StructureAnalysis = append(a.StructureAnalysis, StatementAndMixedLoggingReplicasStructureWarning)
			}
			if a.IsMaster && a.CountStatementBasedLoggingReplicas > 0 && a.CountRowBasedLoggingReplicas > 0 {
				a.StructureAnalysis = append(a.StructureAnalysis, StatementAndRowLoggingReplicasStructureWarning)
			}
			if a.IsMaster && a.CountMixedBasedLoggingReplicas > 0 && a.CountRowBasedLoggingReplicas > 0 {
				a.StructureAnalysis = append(a.StructureAnalysis, MixedAndRowLoggingReplicasStructureWarning)
			}
			if a.IsMaster && a.CountDistinctMajorVersionsLoggingReplicas > 1 {
				a.StructureAnalysis = append(a.StructureAnalysis, MultipleMajorVersionsLoggingReplicasStructureWarning)
			}

			if a.CountReplicas > 0 && (a.GTIDMode != a.MinReplicaGTIDMode || a.GTIDMode != a.MaxReplicaGTIDMode) {
				a.StructureAnalysis = append(a.StructureAnalysis, DifferentGTIDModesStructureWarning)
			}
			if a.MaxReplicaGTIDErrant != "" {
				a.StructureAnalysis = append(a.StructureAnalysis, ErrantGTIDStructureWarning)
			}

			if a.IsMaster && a.IsReadOnly {
				a.StructureAnalysis = append(a.StructureAnalysis, NoWriteableMasterStructureWarning)
			}

			if a.IsMaster && a.SemiSyncMasterEnabled && !a.SemiSyncMasterStatus && a.SemiSyncMasterWaitForReplicaCount > 0 && a.SemiSyncMasterClients < a.SemiSyncMasterWaitForReplicaCount {
				a.StructureAnalysis = append(a.StructureAnalysis, NotEnoughValidSemiSyncReplicasStructureWarning)
			}
		}
		appendAnalysis(&a)

		if a.CountReplicas > 0 && hints.AuditAnalysis {
			// Interesting enough for analysis
			go auditInstanceAnalysisInChangelog(&a.AnalyzedInstanceKey, a.Analysis)
		}
		return nil
	})

	if err != nil {
		return result, log.Errore(err)
	}
	// TODO: result, err = getConcensusReplicationAnalysis(result)
	return result, log.Errore(err)
}

func getConcensusReplicationAnalysis(analysisEntries []ReplicationAnalysis) ([]ReplicationAnalysis, error) {
	if !orcraft.IsRaftEnabled() {
		return analysisEntries, nil
	}
	if !config.Config.ExpectFailureAnalysisConcensus {
		return analysisEntries, nil
	}
	concensusAnalysisEntries := []ReplicationAnalysis{}
	peerAnalysisMap, err := ReadPeerAnalysisMap()
	if err != nil {
		return analysisEntries, err
	}
	quorumSize, err := orcraft.QuorumSize()
	if err != nil {
		return analysisEntries, err
	}

	for _, analysisEntry := range analysisEntries {
		instanceAnalysis := NewInstanceAnalysis(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
		analysisKey := instanceAnalysis.String()

		peerAnalysisCount := peerAnalysisMap[analysisKey]
		if 1+peerAnalysisCount >= quorumSize {
			// this node and enough other nodes in agreement
			concensusAnalysisEntries = append(concensusAnalysisEntries, analysisEntry)
		}
	}
	return concensusAnalysisEntries, nil
}

// auditInstanceAnalysisInChangelog will write down an instance's analysis in the database_instance_analysis_changelog table.
// To not repeat recurring analysis code, the database_instance_last_analysis table is used, so that only changes to
// analysis codes are written.
func auditInstanceAnalysisInChangelog(instanceKey *InstanceKey, analysisCode AnalysisCode) error {
	if lastWrittenAnalysis, found := recentInstantAnalysis.Get(instanceKey.DisplayString()); found {
		if lastWrittenAnalysis == analysisCode {
			// Surely nothing new.
			// And let's expand the timeout
			recentInstantAnalysis.Set(instanceKey.DisplayString(), analysisCode, cache.DefaultExpiration)
			return nil
		}
	}
	// Passed the cache; but does database agree that there's a change? Here's a persistent cache; this comes here
	// to verify no two orchestrator services are doing this without coordinating (namely, one dies, the other taking its place
	// and has no familiarity of the former's cache)
	analysisChangeWriteAttemptCounter.Inc(1)

	lastAnalysisChanged := false
	{
		sqlResult, err := db.ExecOrchestrator(`
			update database_instance_last_analysis set
				analysis = ?,
				analysis_timestamp = now()
			where
				hostname = ?
				and port = ?
				and analysis != ?
			`,
			string(analysisCode), instanceKey.Hostname, instanceKey.Port, string(analysisCode),
		)
		if err != nil {
			return log.Errore(err)
		}
		rows, err := sqlResult.RowsAffected()
		if err != nil {
			return log.Errore(err)
		}
		lastAnalysisChanged = (rows > 0)
	}
	if !lastAnalysisChanged {
		_, err := db.ExecOrchestrator(`
			insert ignore into database_instance_last_analysis (
					hostname, port, analysis_timestamp, analysis
				) values (
					?, ?, now(), ?
				)
			`,
			instanceKey.Hostname, instanceKey.Port, string(analysisCode),
		)
		if err != nil {
			return log.Errore(err)
		}
	}
	recentInstantAnalysis.Set(instanceKey.DisplayString(), analysisCode, cache.DefaultExpiration)
	if !lastAnalysisChanged {
		return nil
	}

	_, err := db.ExecOrchestrator(`
			insert into database_instance_analysis_changelog (
					hostname, port, analysis_timestamp, analysis
				) values (
					?, ?, now(), ?
				)
			`,
		instanceKey.Hostname, instanceKey.Port, string(analysisCode),
	)
	if err == nil {
		analysisChangeWriteCounter.Inc(1)
	}
	return log.Errore(err)
}

// ExpireInstanceAnalysisChangelog removes old-enough analysis entries from the changelog 从变更日志中删除足够旧的分析数据
func ExpireInstanceAnalysisChangelog() error {
	_, err := db.ExecOrchestrator(`
			delete
				from database_instance_analysis_changelog
			where
				analysis_timestamp < now() - interval ? hour
			`,
		config.Config.UnseenInstanceForgetHours,
	)
	return log.Errore(err)
}

// ReadReplicationAnalysisChangelog
func ReadReplicationAnalysisChangelog() (res [](*ReplicationAnalysisChangelog), err error) {
	query := `
		select
      hostname,
      port,
			analysis_timestamp,
			analysis
		from
			database_instance_analysis_changelog
		order by
			hostname, port, changelog_id
		`
	analysisChangelog := &ReplicationAnalysisChangelog{}
	err = db.QueryOrchestratorRowsMap(query, func(m sqlutils.RowMap) error {
		key := InstanceKey{Hostname: m.GetString("hostname"), Port: m.GetInt("port")}

		if !analysisChangelog.AnalyzedInstanceKey.Equals(&key) {
			analysisChangelog = &ReplicationAnalysisChangelog{AnalyzedInstanceKey: key, Changelog: []string{}}
			res = append(res, analysisChangelog)
		}
		analysisEntry := fmt.Sprintf("%s;%s,", m.GetString("analysis_timestamp"), m.GetString("analysis"))
		analysisChangelog.Changelog = append(analysisChangelog.Changelog, analysisEntry)

		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	return res, err
}

// ReadPeerAnalysisMap reads raft-peer failure analysis, and returns a PeerAnalysisMap,
// indicating how many peers see which analysis
func ReadPeerAnalysisMap() (peerAnalysisMap PeerAnalysisMap, err error) {
	peerAnalysisMap = make(PeerAnalysisMap)
	query := `
		select
      hostname,
      port,
			analysis
		from
			database_instance_peer_analysis
		order by
			peer, hostname, port
		`
	err = db.QueryOrchestratorRowsMap(query, func(m sqlutils.RowMap) error {
		instanceKey := InstanceKey{Hostname: m.GetString("hostname"), Port: m.GetInt("port")}
		analysis := m.GetString("analysis")
		instanceAnalysis := NewInstanceAnalysis(&instanceKey, AnalysisCode(analysis))
		mapKey := instanceAnalysis.String()
		peerAnalysisMap[mapKey] = peerAnalysisMap[mapKey] + 1

		return nil
	})
	return peerAnalysisMap, log.Errore(err)
}
