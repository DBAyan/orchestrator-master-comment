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

package logic

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	goos "os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/openark/golib/log"
	"github.com/openark/orchestrator/go/attributes"
	"github.com/openark/orchestrator/go/config"
	"github.com/openark/orchestrator/go/inst"
	"github.com/openark/orchestrator/go/kv"
	ometrics "github.com/openark/orchestrator/go/metrics"
	"github.com/openark/orchestrator/go/os"
	"github.com/openark/orchestrator/go/process"
	orcraft "github.com/openark/orchestrator/go/raft"
	"github.com/openark/orchestrator/go/util"
	"github.com/patrickmn/go-cache"
	"github.com/rcrowley/go-metrics"
)

var countPendingRecoveries int64

type RecoveryType string

const (
	MasterRecovery                 RecoveryType = "MasterRecovery"
	CoMasterRecovery                            = "CoMasterRecovery"
	IntermediateMasterRecovery                  = "IntermediateMasterRecovery"
	ReplicationGroupMemberRecovery              = "ReplicationGroupMemberRecovery"
)

type RecoveryAcknowledgement struct {
	CreatedAt time.Time
	Owner     string
	Comment   string

	Key           inst.InstanceKey
	ClusterName   string
	Id            int64
	UID           string
	AllRecoveries bool
}

func NewRecoveryAcknowledgement(owner string, comment string) *RecoveryAcknowledgement {
	return &RecoveryAcknowledgement{
		CreatedAt: time.Now(),
		Owner:     owner,
		Comment:   comment,
	}
}

func NewInternalAcknowledgement() *RecoveryAcknowledgement {
	return &RecoveryAcknowledgement{
		CreatedAt: time.Now(),
		Owner:     "orchestrator",
		Comment:   "internal",
	}
}

// BlockedTopologyRecovery represents an entry in the blocked_topology_recovery table
type BlockedTopologyRecovery struct {
	FailedInstanceKey    inst.InstanceKey
	ClusterName          string
	Analysis             inst.AnalysisCode
	LastBlockedTimestamp string
	BlockingRecoveryId   int64
}

// TopologyRecovery represents an entry in the topology_recovery table
type TopologyRecovery struct {
	inst.PostponedFunctionsContainer

	Id                         int64
	UID                        string
	AnalysisEntry              inst.ReplicationAnalysis
	SuccessorKey               *inst.InstanceKey
	SuccessorAlias             string
	SuccessorBinlogCoordinates *inst.BinlogCoordinates
	IsActive                   bool
	IsSuccessful               bool
	LostReplicas               inst.InstanceKeyMap
	ParticipatingInstanceKeys  inst.InstanceKeyMap
	AllErrors                  []string
	RecoveryStartTimestamp     string
	RecoveryEndTimestamp       string
	ProcessingNodeHostname     string
	ProcessingNodeToken        string
	Acknowledged               bool
	AcknowledgedAt             string
	AcknowledgedBy             string
	AcknowledgedComment        string
	LastDetectionId            int64
	RelatedRecoveryId          int64
	Type                       RecoveryType
	RecoveryType               MasterRecoveryType
}

func NewTopologyRecovery(replicationAnalysis inst.ReplicationAnalysis) *TopologyRecovery {
	topologyRecovery := &TopologyRecovery{}
	topologyRecovery.UID = util.PrettyUniqueToken()
	topologyRecovery.AnalysisEntry = replicationAnalysis
	topologyRecovery.SuccessorKey = nil
	topologyRecovery.SuccessorBinlogCoordinates = nil
	topologyRecovery.LostReplicas = *inst.NewInstanceKeyMap()
	topologyRecovery.ParticipatingInstanceKeys = *inst.NewInstanceKeyMap()
	topologyRecovery.AllErrors = []string{}
	topologyRecovery.RecoveryType = NotMasterRecovery
	return topologyRecovery
}

func (this *TopologyRecovery) AddError(err error) error {
	if err != nil {
		this.AllErrors = append(this.AllErrors, err.Error())
	}
	return err
}

func (this *TopologyRecovery) AddErrors(errs []error) {
	for _, err := range errs {
		this.AddError(err)
	}
}

type TopologyRecoveryStep struct {
	Id          int64
	RecoveryUID string
	AuditAt     string
	Message     string
}

func NewTopologyRecoveryStep(uid string, message string) *TopologyRecoveryStep {
	return &TopologyRecoveryStep{
		RecoveryUID: uid,
		Message:     message,
	}
}

type MasterRecoveryType string

const (
	NotMasterRecovery          MasterRecoveryType = "NotMasterRecovery"
	MasterRecoveryGTID                            = "MasterRecoveryGTID"
	MasterRecoveryPseudoGTID                      = "MasterRecoveryPseudoGTID"
	MasterRecoveryBinlogServer                    = "MasterRecoveryBinlogServer"
)

var emergencyReadTopologyInstanceMap *cache.Cache
var emergencyRestartReplicaTopologyInstanceMap *cache.Cache
var emergencyOperationGracefulPeriodMap *cache.Cache

// InstancesByCountReplicas sorts instances by umber of replicas, descending
type InstancesByCountReplicas [](*inst.Instance)

func (this InstancesByCountReplicas) Len() int      { return len(this) }
func (this InstancesByCountReplicas) Swap(i, j int) { this[i], this[j] = this[j], this[i] }
func (this InstancesByCountReplicas) Less(i, j int) bool {
	if len(this[i].Replicas) == len(this[j].Replicas) {
		// Secondary sorting: prefer more advanced replicas
		return !this[i].ExecBinlogCoordinates.SmallerThan(&this[j].ExecBinlogCoordinates)
	}
	return len(this[i].Replicas) < len(this[j].Replicas)
}

var recoverDeadMasterCounter = metrics.NewCounter()
var recoverDeadMasterSuccessCounter = metrics.NewCounter()
var recoverDeadMasterFailureCounter = metrics.NewCounter()
var recoverDeadIntermediateMasterCounter = metrics.NewCounter()
var recoverDeadIntermediateMasterSuccessCounter = metrics.NewCounter()
var recoverDeadIntermediateMasterFailureCounter = metrics.NewCounter()
var recoverDeadCoMasterCounter = metrics.NewCounter()
var recoverDeadCoMasterSuccessCounter = metrics.NewCounter()
var recoverDeadCoMasterFailureCounter = metrics.NewCounter()
var recoverDeadReplicationGroupMemberCounter = metrics.NewCounter()
var recoverDeadReplicationGroupMemberSuccessCounter = metrics.NewCounter()
var recoverDeadReplicationGroupMemberFailureCounter = metrics.NewCounter()
var countPendingRecoveriesGauge = metrics.NewGauge()

func init() {
	metrics.Register("recover.dead_master.start", recoverDeadMasterCounter)
	metrics.Register("recover.dead_master.success", recoverDeadMasterSuccessCounter)
	metrics.Register("recover.dead_master.fail", recoverDeadMasterFailureCounter)
	metrics.Register("recover.dead_intermediate_master.start", recoverDeadIntermediateMasterCounter)
	metrics.Register("recover.dead_intermediate_master.success", recoverDeadIntermediateMasterSuccessCounter)
	metrics.Register("recover.dead_intermediate_master.fail", recoverDeadIntermediateMasterFailureCounter)
	metrics.Register("recover.dead_co_master.start", recoverDeadCoMasterCounter)
	metrics.Register("recover.dead_co_master.success", recoverDeadCoMasterSuccessCounter)
	metrics.Register("recover.dead_co_master.fail", recoverDeadCoMasterFailureCounter)
	metrics.Register("recover.dead_replication_group_member.start", recoverDeadReplicationGroupMemberCounter)
	metrics.Register("recover.dead_replication_group_member.success", recoverDeadReplicationGroupMemberSuccessCounter)
	metrics.Register("recover.dead_replication_group_member.fail", recoverDeadReplicationGroupMemberFailureCounter)
	metrics.Register("recover.pending", countPendingRecoveriesGauge)

	go initializeTopologyRecoveryPostConfiguration()

	ometrics.OnMetricsTick(func() {
		countPendingRecoveriesGauge.Update(getCountPendingRecoveries())
	})
}

func getCountPendingRecoveries() int64 {
	return atomic.LoadInt64(&countPendingRecoveries)
}

func initializeTopologyRecoveryPostConfiguration() {
	config.WaitForConfigurationToBeLoaded()

	emergencyReadTopologyInstanceMap = cache.New(time.Second, time.Millisecond*250)
	emergencyRestartReplicaTopologyInstanceMap = cache.New(time.Second*30, time.Second)
	emergencyOperationGracefulPeriodMap = cache.New(time.Second*5, time.Millisecond*500)
}

// AuditTopologyRecovery audits a single step in a topology recovery process.
func AuditTopologyRecovery(topologyRecovery *TopologyRecovery, message string) error {
	log.Infof("topology_recovery: %s", message)
	if topologyRecovery == nil {
		return nil
	}

	recoveryStep := NewTopologyRecoveryStep(topologyRecovery.UID, message)
	if orcraft.IsRaftEnabled() {
		_, err := orcraft.PublishCommand("write-recovery-step", recoveryStep)
		return err
	} else {
		return writeTopologyRecoveryStep(recoveryStep)
	}
}

func resolveRecovery(topologyRecovery *TopologyRecovery, successorInstance *inst.Instance) error {
	if successorInstance != nil {
		topologyRecovery.SuccessorKey = &successorInstance.Key
		topologyRecovery.SuccessorAlias = successorInstance.InstanceAlias
		topologyRecovery.IsSuccessful = true
		// Assign the current Binlog Coordinates of Successor Instance
		topologyRecovery.SuccessorBinlogCoordinates = &successorInstance.SelfBinlogCoordinates
	}
	if orcraft.IsRaftEnabled() {
		_, err := orcraft.PublishCommand("resolve-recovery", topologyRecovery)
		return err
	} else {
		return writeResolveRecovery(topologyRecovery)
	}
}

// prepareCommand replaces agreed-upon placeholders with analysis data
// prepareCommand 通过分析得到的数据替换脚本的占位符
func prepareCommand(command string, topologyRecovery *TopologyRecovery) (result string, async bool) {
	analysisEntry := &topologyRecovery.AnalysisEntry
	// 从 command 字符串中移除前后的空白字符。
	command = strings.TrimSpace(command)
	// 检查 command 字符串是否以 "&" 结尾。
	if strings.HasSuffix(command, "&") {
		// 如果是，移除末尾的 "&" 并将 async 设置为 true。
		command = strings.TrimRight(command, "&")
		async = true
	}

	// 将命令中的 占位符替换为 analysisEntry 的字符串表示形式。
	command = strings.Replace(command, "{failureType}", string(analysisEntry.Analysis), -1)
	command = strings.Replace(command, "{instanceType}", string(analysisEntry.GetAnalysisInstanceType()), -1)
	command = strings.Replace(command, "{isMaster}", fmt.Sprintf("%t", analysisEntry.IsMaster), -1)
	command = strings.Replace(command, "{isCoMaster}", fmt.Sprintf("%t", analysisEntry.IsCoMaster), -1)
	command = strings.Replace(command, "{failureDescription}", analysisEntry.Description, -1)
	command = strings.Replace(command, "{command}", analysisEntry.CommandHint, -1)
	command = strings.Replace(command, "{failedHost}", analysisEntry.AnalyzedInstanceKey.Hostname, -1)
	command = strings.Replace(command, "{failedPort}", fmt.Sprintf("%d", analysisEntry.AnalyzedInstanceKey.Port), -1)
	command = strings.Replace(command, "{failureCluster}", analysisEntry.ClusterDetails.ClusterName, -1)
	command = strings.Replace(command, "{failureClusterAlias}", analysisEntry.ClusterDetails.ClusterAlias, -1)
	command = strings.Replace(command, "{failureClusterDomain}", analysisEntry.ClusterDetails.ClusterDomain, -1)
	command = strings.Replace(command, "{countSlaves}", fmt.Sprintf("%d", analysisEntry.CountReplicas), -1)
	command = strings.Replace(command, "{countReplicas}", fmt.Sprintf("%d", analysisEntry.CountReplicas), -1)
	command = strings.Replace(command, "{isDowntimed}", fmt.Sprint(analysisEntry.IsDowntimed), -1)
	command = strings.Replace(command, "{autoMasterRecovery}", fmt.Sprint(analysisEntry.ClusterDetails.HasAutomatedMasterRecovery), -1)
	command = strings.Replace(command, "{autoIntermediateMasterRecovery}", fmt.Sprint(analysisEntry.ClusterDetails.HasAutomatedIntermediateMasterRecovery), -1)
	command = strings.Replace(command, "{orchestratorHost}", process.ThisHostname, -1)
	command = strings.Replace(command, "{recoveryUID}", topologyRecovery.UID, -1)

	command = strings.Replace(command, "{isSuccessful}", fmt.Sprint(topologyRecovery.SuccessorKey != nil), -1)
	if topologyRecovery.SuccessorKey != nil {
		command = strings.Replace(command, "{successorHost}", topologyRecovery.SuccessorKey.Hostname, -1)
		command = strings.Replace(command, "{successorPort}", fmt.Sprintf("%d", topologyRecovery.SuccessorKey.Port), -1)
		// As long as SuccessorBinlogCoordinates != nil, we replace {successorBinlogCoordinates}
		// Format of the display string of binlog coordinates would be LogFile:LogPositon
		if topologyRecovery.SuccessorBinlogCoordinates != nil {
			command = strings.Replace(command, "{successorBinlogCoordinates}", topologyRecovery.SuccessorBinlogCoordinates.DisplayString(), -1)
		}
		// As long as SucesssorKey != nil, we replace {successorAlias}.
		// If SucessorAlias is "", it's fine. We'll replace {successorAlias} with "".
		command = strings.Replace(command, "{successorAlias}", topologyRecovery.SuccessorAlias, -1)
	}

	command = strings.Replace(command, "{lostSlaves}", topologyRecovery.LostReplicas.ToCommaDelimitedList(), -1)
	command = strings.Replace(command, "{lostReplicas}", topologyRecovery.LostReplicas.ToCommaDelimitedList(), -1)
	command = strings.Replace(command, "{countLostReplicas}", fmt.Sprintf("%d", len(topologyRecovery.LostReplicas)), -1)
	command = strings.Replace(command, "{slaveHosts}", analysisEntry.Replicas.ToCommaDelimitedList(), -1)
	command = strings.Replace(command, "{replicaHosts}", analysisEntry.Replicas.ToCommaDelimitedList(), -1)

	return command, async
}

// applyEnvironmentVariables sets the relevant environment variables for a recovery
func applyEnvironmentVariables(topologyRecovery *TopologyRecovery) []string {
	analysisEntry := &topologyRecovery.AnalysisEntry
	env := goos.Environ()
	env = append(env, fmt.Sprintf("ORC_FAILURE_TYPE=%s", string(analysisEntry.Analysis)))
	env = append(env, fmt.Sprintf("ORC_INSTANCE_TYPE=%s", string(analysisEntry.GetAnalysisInstanceType())))
	env = append(env, fmt.Sprintf("ORC_IS_MASTER=%t", analysisEntry.IsMaster))
	env = append(env, fmt.Sprintf("ORC_IS_CO_MASTER=%t", analysisEntry.IsCoMaster))
	env = append(env, fmt.Sprintf("ORC_FAILURE_DESCRIPTION=%s", analysisEntry.Description))
	env = append(env, fmt.Sprintf("ORC_COMMAND=%s", analysisEntry.CommandHint))
	env = append(env, fmt.Sprintf("ORC_FAILED_HOST=%s", analysisEntry.AnalyzedInstanceKey.Hostname))
	env = append(env, fmt.Sprintf("ORC_FAILED_PORT=%d", analysisEntry.AnalyzedInstanceKey.Port))
	env = append(env, fmt.Sprintf("ORC_FAILURE_CLUSTER=%s", analysisEntry.ClusterDetails.ClusterName))
	env = append(env, fmt.Sprintf("ORC_FAILURE_CLUSTER_ALIAS=%s", analysisEntry.ClusterDetails.ClusterAlias))
	env = append(env, fmt.Sprintf("ORC_FAILURE_CLUSTER_DOMAIN=%s", analysisEntry.ClusterDetails.ClusterDomain))
	env = append(env, fmt.Sprintf("ORC_COUNT_REPLICAS=%d", analysisEntry.CountReplicas))
	env = append(env, fmt.Sprintf("ORC_IS_DOWNTIMED=%v", analysisEntry.IsDowntimed))
	env = append(env, fmt.Sprintf("ORC_AUTO_MASTER_RECOVERY=%v", analysisEntry.ClusterDetails.HasAutomatedMasterRecovery))
	env = append(env, fmt.Sprintf("ORC_AUTO_INTERMEDIATE_MASTER_RECOVERY=%v", analysisEntry.ClusterDetails.HasAutomatedIntermediateMasterRecovery))
	env = append(env, fmt.Sprintf("ORC_ORCHESTRATOR_HOST=%s", process.ThisHostname))
	env = append(env, fmt.Sprintf("ORC_IS_SUCCESSFUL=%v", (topologyRecovery.SuccessorKey != nil)))
	env = append(env, fmt.Sprintf("ORC_LOST_REPLICAS=%s", topologyRecovery.LostReplicas.ToCommaDelimitedList()))
	env = append(env, fmt.Sprintf("ORC_REPLICA_HOSTS=%s", analysisEntry.Replicas.ToCommaDelimitedList()))
	env = append(env, fmt.Sprintf("ORC_RECOVERY_UID=%s", topologyRecovery.UID))

	if topologyRecovery.SuccessorKey != nil {
		env = append(env, fmt.Sprintf("ORC_SUCCESSOR_HOST=%s", topologyRecovery.SuccessorKey.Hostname))
		env = append(env, fmt.Sprintf("ORC_SUCCESSOR_PORT=%d", topologyRecovery.SuccessorKey.Port))
		// As long as SuccessorBinlogCoordinates != nil, we set ORC_SUCCESSOR_BINLOG_COORDINATES
		// Format of the display string of binlog coordinates would be LogFile:LogPositon
		if topologyRecovery.SuccessorBinlogCoordinates != nil {
			env = append(env, fmt.Sprintf("ORC_SUCCESSOR_BINLOG_COORDINATES=%s", topologyRecovery.SuccessorBinlogCoordinates.DisplayString()))
		}
		// As long as SucesssorKey != nil, we replace {successorAlias}.
		// If SucessorAlias is "", it's fine. We'll replace {successorAlias} with "".
		env = append(env, fmt.Sprintf("ORC_SUCCESSOR_ALIAS=%s", topologyRecovery.SuccessorAlias))
	}

	return env
}

func executeProcess(command string, env []string, topologyRecovery *TopologyRecovery, fullDescription string) (err error) {
	// Log the command to be run and record how long it takes as this may be useful 记录命令运行的日志 然后还有运行时间。
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Running %s: %s", fullDescription, command))
	start := time.Now() // 当前时间
	var info string
	if err = os.CommandRun(command, env); err == nil {
		info = fmt.Sprintf("Completed %s in %v", fullDescription, time.Since(start))
	} else {
		info = fmt.Sprintf("Execution of %s failed in %v with error: %v", fullDescription, time.Since(start), err)
		log.Errorf(info)
	}
	AuditTopologyRecovery(topologyRecovery, info)
	return err
}

// executeProcesses executes a list of processes
func executeProcesses(processes []string, description string, topologyRecovery *TopologyRecovery, failOnError bool) (err error) {
	// 如果脚本数组的长度为0  ，则打印日志 ，没有外挂脚本需要执行
	if len(processes) == 0 {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("No %s hooks to run", description))
		return nil
	}
	// 如果脚本数组不为0，则遍历脚本切片，顺序执行。 len(processes) 脚本个数，
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Running %d %s hooks", len(processes), description))
	for i, command := range processes {
		// 组装需要执行的脚本命令 如果脚本命令后面有 &  则将异步执行的标志设置为true
		command, async := prepareCommand(command, topologyRecovery)
		env := applyEnvironmentVariables(topologyRecovery)

		fullDescription := fmt.Sprintf("%s hook %d of %d", description, i+1, len(processes))
		if async {
			fullDescription = fmt.Sprintf("%s (async)", fullDescription)
		}
		if async {
			// Ignore errors
			go executeProcess(command, env, topologyRecovery, fullDescription)
		} else {
			if cmdErr := executeProcess(command, env, topologyRecovery, fullDescription); cmdErr != nil {
				if failOnError {
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Not running further %s hooks", description))
					return cmdErr
				}
				if err == nil {
					// Keep first error encountered
					err = cmdErr
				}
			}
		}
	}
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("done running %s hooks", description))
	return err
}

func recoverDeadMasterInBinlogServerTopology(topologyRecovery *TopologyRecovery) (promotedReplica *inst.Instance, err error) {
	failedMasterKey := &topologyRecovery.AnalysisEntry.AnalyzedInstanceKey

	var promotedBinlogServer *inst.Instance

	_, promotedBinlogServer, err = inst.RegroupReplicasBinlogServers(failedMasterKey, true)
	if err != nil {
		return nil, log.Errore(err)
	}
	promotedBinlogServer, err = inst.StopReplication(&promotedBinlogServer.Key)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	// Find candidate replica
	promotedReplica, err = inst.GetCandidateReplicaOfBinlogServerTopology(&promotedBinlogServer.Key)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	// Align it with binlog server coordinates
	promotedReplica, err = inst.StopReplication(&promotedReplica.Key)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	promotedReplica, err = inst.StartReplicationUntilMasterCoordinates(&promotedReplica.Key, &promotedBinlogServer.ExecBinlogCoordinates)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	promotedReplica, err = inst.StopReplication(&promotedReplica.Key)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	// Detach, flush binary logs forward
	promotedReplica, err = inst.ResetReplication(&promotedReplica.Key)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	promotedReplica, err = inst.FlushBinaryLogsTo(&promotedReplica.Key, promotedBinlogServer.ExecBinlogCoordinates.LogFile)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	promotedReplica, err = inst.FlushBinaryLogs(&promotedReplica.Key, 1)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	promotedReplica, err = inst.PurgeBinaryLogsToLatest(&promotedReplica.Key, false)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	// Reconnect binlog servers to promoted replica (now master):
	promotedBinlogServer, err = inst.SkipToNextBinaryLog(&promotedBinlogServer.Key)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	promotedBinlogServer, err = inst.Repoint(&promotedBinlogServer.Key, &promotedReplica.Key, inst.GTIDHintDeny)
	if err != nil {
		return nil, log.Errore(err)
	}

	func() {
		// Move binlog server replicas up to replicate from master.
		// This can only be done once a BLS has skipped to the next binlog
		// We postpone this operation. The master is already promoted and we're happy.
		binlogServerReplicas, err := inst.ReadBinlogServerReplicaInstances(&promotedBinlogServer.Key)
		if err != nil {
			return
		}
		maxBinlogServersToPromote := 3
		for i, binlogServerReplica := range binlogServerReplicas {
			binlogServerReplica := binlogServerReplica
			if i >= maxBinlogServersToPromote {
				return
			}
			postponedFunction := func() error {
				binlogServerReplica, err := inst.StopReplication(&binlogServerReplica.Key)
				if err != nil {
					return err
				}
				// Make sure the BLS has the "next binlog" -- the one the master flushed & purged to. Otherwise the BLS
				// will request a binlog the master does not have
				if binlogServerReplica.ExecBinlogCoordinates.SmallerThan(&promotedBinlogServer.ExecBinlogCoordinates) {
					binlogServerReplica, err = inst.StartReplicationUntilMasterCoordinates(&binlogServerReplica.Key, &promotedBinlogServer.ExecBinlogCoordinates)
					if err != nil {
						return err
					}
				}
				_, err = inst.Repoint(&binlogServerReplica.Key, &promotedReplica.Key, inst.GTIDHintDeny)
				return err
			}
			topologyRecovery.AddPostponedFunction(postponedFunction, fmt.Sprintf("recoverDeadMasterInBinlogServerTopology, moving binlog server %+v", binlogServerReplica.Key))
		}
	}()

	return promotedReplica, err
}

func GetMasterRecoveryType(analysisEntry *inst.ReplicationAnalysis) (masterRecoveryType MasterRecoveryType) {
	masterRecoveryType = MasterRecoveryPseudoGTID
	if analysisEntry.OracleGTIDImmediateTopology || analysisEntry.MariaDBGTIDImmediateTopology {
		masterRecoveryType = MasterRecoveryGTID
	} else if analysisEntry.BinlogServerImmediateTopology {
		masterRecoveryType = MasterRecoveryBinlogServer
	}
	return masterRecoveryType
}

// recoverDeadMaster recovers a dead master, complete logic inside  用于恢复一个已经宕机（dead）的主节点，其中包含了完整的逻辑。
func recoverDeadMaster(topologyRecovery *TopologyRecovery, candidateInstanceKey *inst.InstanceKey, skipProcesses bool) (recoveryAttempted bool, promotedReplica *inst.Instance, lostReplicas [](*inst.Instance), err error) {
	topologyRecovery.Type = MasterRecovery
	analysisEntry := &topologyRecovery.AnalysisEntry
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey
	var cannotReplicateReplicas [](*inst.Instance)
	postponedAll := false

	inst.AuditOperation("recover-dead-master", failedInstanceKey, "problem found; will recover")
	if !skipProcesses { // 是否跳过钩子脚本执行
		if err := executeProcesses(config.Config.PreFailoverProcesses, "PreFailoverProcesses", topologyRecovery, true); err != nil {
			return false, nil, lostReplicas, topologyRecovery.AddError(err)
		}
	}

	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: will recover %+v", *failedInstanceKey))

	topologyRecovery.RecoveryType = GetMasterRecoveryType(analysisEntry) // 获取 主库的恢复类型： 有三种 分别为
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: masterRecoveryType=%+v", topologyRecovery.RecoveryType))

	// 定义匿名函数 接受两个参数 返回布尔类型, 在下面的逻辑中被调用 判断被提升的副本是最理想的
	promotedReplicaIsIdeal := func(promoted *inst.Instance, hasBestPromotionRule bool) bool {
		// 如果没有待提升的副本，则返回false
		if promoted == nil {
			return false
		}
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: promotedReplicaIsIdeal(%+v)", promoted.Key))

		if candidateInstanceKey != nil { //explicit request to promote a specific server 意味着存在一个显式的请求，要求提升一个特定的服务器
			return promoted.Key.Equals(candidateInstanceKey) // 返回比较  promoted的 instanceKey  和 candidateInstanceKey
		}
		if promoted.DataCenter == topologyRecovery.AnalysisEntry.AnalyzedInstanceDataCenter && // 如果被提升的主的数据中心（机房） == AnalyzedInstanceDataCenter
			promoted.PhysicalEnvironment == topologyRecovery.AnalysisEntry.AnalyzedInstancePhysicalEnvironment { //  如果被提升的主的物理环境） == AnalyzedInstancePhysicalEnvironment
			if promoted.PromotionRule == inst.MustPromoteRule || promoted.PromotionRule == inst.PreferPromoteRule || // 提升规则为MustPromoteRule 或 PreferPromoteRule
				(hasBestPromotionRule && promoted.PromotionRule != inst.MustNotPromoteRule) { //  或者有最好的提升规则 而且不能为 MustNotPromoteRule
				AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: found %+v to be ideal candidate; will optimize recovery", promoted.Key))
				postponedAll = true
				return true
			}
		}
		return false
	}
	switch topologyRecovery.RecoveryType { // 根据拓扑恢复的类型进行判断，不同类型执行不同逻辑
	case MasterRecoveryGTID: // GTID
		{
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: regrouping replicas via GTID"))
			lostReplicas, _, cannotReplicateReplicas, promotedReplica, err = inst.RegroupReplicasGTID(failedInstanceKey, true, false, nil, &topologyRecovery.PostponedFunctionsContainer, promotedReplicaIsIdeal)
		}
	case MasterRecoveryPseudoGTID: // 伪GTID
		{
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: regrouping replicas via Pseudo-GTID"))
			lostReplicas, _, _, cannotReplicateReplicas, promotedReplica, err = inst.RegroupReplicasPseudoGTIDIncludingSubReplicasOfBinlogServers(failedInstanceKey, true, nil, &topologyRecovery.PostponedFunctionsContainer, promotedReplicaIsIdeal)
		}
	case MasterRecoveryBinlogServer: // binlog SERVER
		{
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: recovering via binlog servers"))
			promotedReplica, err = recoverDeadMasterInBinlogServerTopology(topologyRecovery)
		}
	}
	topologyRecovery.AddError(err)
	lostReplicas = append(lostReplicas, cannotReplicateReplicas...) // 将不能复制的从副本添加到丢弃的从副本切片中
	for _, replica := range lostReplicas { // 遍历丢弃的从副本
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: - lost replica: %+v", replica.Key))
	}

	if promotedReplica != nil && len(lostReplicas) > 0 && config.Config.DetachLostReplicasAfterMasterFailover { // 被提升的主不为空 且 丢弃的从副本不为0 且 配置了参数DetachLostReplicasAfterMasterFailover
		postponedFunction := func() error { // 定义一个匿名函数
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: lost %+v replicas during recovery process; detaching them", len(lostReplicas)))
			for _, replica := range lostReplicas {
				replica := replica
				inst.DetachReplicaMasterHost(&replica.Key) // 分离丢弃的从
			}
			return nil
		}
		topologyRecovery.AddPostponedFunction(postponedFunction, fmt.Sprintf("RecoverDeadMaster, detach %+v lost replicas", len(lostReplicas))) // 延迟执行了匿名函数
	}

	func() error {
		inst.BeginDowntime(inst.NewDowntime(failedInstanceKey, inst.GetMaintenanceOwner(), inst.DowntimeLostInRecoveryMessage, time.Duration(config.LostInRecoveryDowntimeSeconds)*time.Second))
		acknowledgeInstanceFailureDetection(&analysisEntry.AnalyzedInstanceKey)
		for _, replica := range lostReplicas { // 遍历丢弃的从副本
			replica := replica // 设置维护
			inst.BeginDowntime(inst.NewDowntime(&replica.Key, inst.GetMaintenanceOwner(), inst.DowntimeLostInRecoveryMessage, time.Duration(config.LostInRecoveryDowntimeSeconds)*time.Second))
		}
		return nil
	}() // 这段代码是一个匿名函数（闭包），它会立即执行。这是通过在声明匿名函数的尾部添加 () 来实现的。这种模式称为"立即执行函数表达式"（Immediately Invoked Function Expression，简称 IIFE）

	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: %d postponed functions", topologyRecovery.PostponedFunctionsContainer.Len()))

	if promotedReplica != nil && !postponedAll { // 二次选主 postponedAll false
		promotedReplica, err = replacePromotedReplicaWithCandidate(topologyRecovery, &analysisEntry.AnalyzedInstanceKey, promotedReplica, candidateInstanceKey)
		topologyRecovery.AddError(err)
	}

	if promotedReplica == nil { // 如果没有被提升的从副本
		message := "Failure: no replica promoted."
		AuditTopologyRecovery(topologyRecovery, message)
		inst.AuditOperation("recover-dead-master", failedInstanceKey, message)
	} else {
		message := fmt.Sprintf("promoted replica: %+v", promotedReplica.Key)
		AuditTopologyRecovery(topologyRecovery, message)
		inst.AuditOperation("recover-dead-master", failedInstanceKey, message)
	}
	return true, promotedReplica, lostReplicas, err
}

// MasterFailoverGeographicConstraintSatisfied 用于判断在主节点故障转移中是否满足地理约束条件。
func MasterFailoverGeographicConstraintSatisfied(analysisEntry *inst.ReplicationAnalysis, suggestedInstance *inst.Instance) (satisfied bool, dissatisfiedReason string) {
	if config.Config.PreventCrossDataCenterMasterFailover { // 禁止跨机房进行故障转移
		if suggestedInstance.DataCenter != analysisEntry.AnalyzedInstanceDataCenter {
			return false, fmt.Sprintf("PreventCrossDataCenterMasterFailover: will not promote server in %s when failed server in %s", suggestedInstance.DataCenter, analysisEntry.AnalyzedInstanceDataCenter)
		}
	}
	if config.Config.PreventCrossRegionMasterFailover {
		if suggestedInstance.Region != analysisEntry.AnalyzedInstanceRegion {
			return false, fmt.Sprintf("PreventCrossRegionMasterFailover: will not promote server in %s when failed server in %s", suggestedInstance.Region, analysisEntry.AnalyzedInstanceRegion)
		}
	}
	return true, ""
}

// SuggestReplacementForPromotedReplica returns a server to take over the already
// promoted replica, if such server is found and makes an improvement over the promoted replica. 目的是在已经提升的从属节点上找到一个服务器来接管，如果找到的服务器比已提升的从属节点更好，则返回该服务器。
func SuggestReplacementForPromotedReplica(topologyRecovery *TopologyRecovery, deadInstanceKey *inst.InstanceKey, promotedReplica *inst.Instance, candidateInstanceKey *inst.InstanceKey) (replacement *inst.Instance, actionRequired bool, err error) {
	candidateReplicas, _ := inst.ReadClusterCandidateInstances(promotedReplica.ClusterName) // 查询集群中提升规则为'must', 'prefer'的实例
	candidateReplicas = inst.RemoveInstance(candidateReplicas, deadInstanceKey) // 从候选实例candidateReplicas中移除宕机实例 deadInstanceKey
	deadInstance, _, err := inst.ReadInstance(deadInstanceKey) // 读取宕机实例的信息
	if err != nil {
		deadInstance = nil
	}
	// So we've already promoted a replica.
	// However, can we improve on our choice? Are there any replicas marked with "is_candidate"?
	// Maybe we actually promoted such a replica. Does that mean we should keep it?
	// Maybe we promoted a "neutral", and some "prefer" server is available.
	// Maybe we promoted a "prefer_not"
	// Maybe we promoted a server in a different DC than the master
	// There's many options. We may wish to replace the server we promoted with a better one.
	// 这段注释描述了代码在尝试寻找更好的从属节点来接管已提升的节点时要考虑的多种情况和策略。
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("checking if should replace promoted replica with a better candidate")) // 使用更好的候选节点替换已经被提升的复制实例。
	if candidateInstanceKey == nil { // 没有指定新的候选主
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ checking if promoted replica is the ideal candidate")) // 检查提升的副本是否是理想的候选者
		if deadInstance != nil {
			for _, candidateReplica := range candidateReplicas { // 遍历候选实例列表
				if promotedReplica.Key.Equals(&candidateReplica.Key) && // 第一次被提升的实例的key == 新的候选实例的key
					promotedReplica.DataCenter == deadInstance.DataCenter && // 第一次被提升实例的数据中心 == 老主库的数据中心
					promotedReplica.PhysicalEnvironment == deadInstance.PhysicalEnvironment { //第一次被提升实例的数据中心 == 老主库的数据中心
					// Seems like we promoted a candidate in the same DC & ENV as dead IM! Ideal! We're happy! 被提升的候选者的机房和物理环境 和宕机IM都相同。是理想的提升者
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("promoted replica %+v is the ideal candidate", promotedReplica.Key))
					return promotedReplica, false, nil
				}
			}
		}
	}
	// We didn't pick the ideal candidate; let's see if we can replace with a candidate from same DC and ENV 没有得到理想的候选实例。看能否可以用相同DC 和 ENV的实例替代
	if candidateInstanceKey == nil { // 还没有选出候选实例
		// Try a candidate replica that is in same DC & env as the dead instance
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ searching for an ideal candidate")) // 寻找一个更理想的候选者
		if deadInstance != nil {//
			for _, candidateReplica := range candidateReplicas { // 遍历候选实例列表
				if canTakeOverPromotedServerAsMaster(candidateReplica, promotedReplica) && // 第一次提升的主库可以change到该实例上
					candidateReplica.DataCenter == deadInstance.DataCenter && //  候选的实例的数据中心 == 宕机老主库的数据中心
					candidateReplica.PhysicalEnvironment == deadInstance.PhysicalEnvironment {  // 候选实例的物理环境 == 宕机实例的物理环境
					// This would make a great candidate
					candidateInstanceKey = &candidateReplica.Key
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but orchestrator picks %+v as candidate replacement, based on being in same DC & env as failed instance", *deadInstanceKey, candidateReplica.Key))
				}
			}
		}
	}
	if candidateInstanceKey == nil {// 还没有选出候选实例
		// We cannot find a candidate in same DC and ENV as dead master 我们没有在候选实例列表中 找到与 宕机主实例相同的 DC 和 ENV 中找到候选者
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ checking if promoted replica is an OK candidate"))
		for _, candidateReplica := range candidateReplicas { // 遍历候选实例列表
			if promotedReplica.Key.Equals(&candidateReplica.Key) { // 如果被提升的实例的key 与 候选实例的key相同
				// Seems like we promoted a candidate replica (though not in same DC and ENV as dead master) 我们提升了一个候选从副本， 即使与宕机 主实例不在同一个 DC 或 ENV
				if satisfied, reason := MasterFailoverGeographicConstraintSatisfied(&topologyRecovery.AnalysisEntry, candidateReplica); satisfied { // 检查是否可以跨机房故障转移
					// Good enough. No further action required.
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("promoted replica %+v is a good candidate", promotedReplica.Key))
					return promotedReplica, false, nil
				} else {
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("skipping %+v; %s", candidateReplica.Key, reason))
				}
			}
		}
	}
	// Still nothing? 还没有选出 候选实例
	if candidateInstanceKey == nil { // 还没有选出候选实例
		// Try a candidate replica that is in same DC & env as the promoted replica (our promoted replica is not an "is_candidate")
		// 尝试寻找一个和 被提升的副本在同一个DC 和 ENV的 候选副本 。（被提升的副本不是is_candidate）
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ searching for a candidate"))
		for _, candidateReplica := range candidateReplicas {
			if canTakeOverPromotedServerAsMaster(candidateReplica, promotedReplica) &&
				promotedReplica.DataCenter == candidateReplica.DataCenter &&
				promotedReplica.PhysicalEnvironment == candidateReplica.PhysicalEnvironment {
				// OK, better than nothing
				candidateInstanceKey = &candidateReplica.Key
				AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but orchestrator picks %+v as candidate replacement, based on being in same DC & env as promoted instance", promotedReplica.Key, candidateReplica.Key))
			}
		}
	}
	// Still nothing? 还没有选出 候选实例
	if candidateInstanceKey == nil {// 还没有选出候选实例
		// Try a candidate replica (our promoted replica is not an "is_candidate")
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ searching for a candidate"))
		for _, candidateReplica := range candidateReplicas {
			if canTakeOverPromotedServerAsMaster(candidateReplica, promotedReplica) {
				if satisfied, reason := MasterFailoverGeographicConstraintSatisfied(&topologyRecovery.AnalysisEntry, candidateReplica); satisfied {
					// OK, better than nothing
					candidateInstanceKey = &candidateReplica.Key
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but orchestrator picks %+v as candidate replacement", promotedReplica.Key, candidateReplica.Key))
				} else {
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("skipping %+v; %s", candidateReplica.Key, reason))
				}
			}
		}
	}

	keepSearchingHint := ""
	if satisfied, reason := MasterFailoverGeographicConstraintSatisfied(&topologyRecovery.AnalysisEntry, promotedReplica); !satisfied { // 第一次被提升的主库 跨机房 进行了切换
		keepSearchingHint = fmt.Sprintf("Will keep searching; %s", reason)
	} else if promotedReplica.PromotionRule == inst.PreferNotPromoteRule { // 第一次被提升的主库 提升规则设置了 prefer_not
		keepSearchingHint = fmt.Sprintf("Will keep searching because we have promoted a server with prefer_not rule: %+v", promotedReplica.Key)
	}
	if keepSearchingHint != "" {
		AuditTopologyRecovery(topologyRecovery, keepSearchingHint)
		neutralReplicas, _ := inst.ReadClusterNeutralPromotionRuleInstances(promotedReplica.ClusterName) // 读取集群中设置为neutral实例，组成 neutralReplicas 切片

		if candidateInstanceKey == nil { // 还没有选出候选实例
			// Still nothing? Then we didn't find a replica marked as "candidate". OK, further down the stream we have:
			// find neutral instance in same dv&env as dead master
			//遍历neutralReplicas切片：
			//如果第一次被提升的主库可以change到该实例，且
			//该实例和宕机主库的DC以及ENV都相同。
			//则该实例为候选实例。
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ searching for a neutral server to replace promoted server, in same DC and env as dead master"))
			for _, neutralReplica := range neutralReplicas {
				if canTakeOverPromotedServerAsMaster(neutralReplica, promotedReplica) &&
					deadInstance.DataCenter == neutralReplica.DataCenter &&
					deadInstance.PhysicalEnvironment == neutralReplica.PhysicalEnvironment {
					candidateInstanceKey = &neutralReplica.Key
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but orchestrator picks %+v as candidate replacement, based on being in same DC & env as dead master", promotedReplica.Key, neutralReplica.Key))
				}
			}
		}
		if candidateInstanceKey == nil { // 还没有选出候选实例
			// find neutral instance in same dv&env as promoted replica
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ searching for a neutral server to replace promoted server, in same DC and env as promoted replica"))
			for _, neutralReplica := range neutralReplicas {
				if canTakeOverPromotedServerAsMaster(neutralReplica, promotedReplica) &&
					promotedReplica.DataCenter == neutralReplica.DataCenter &&
					promotedReplica.PhysicalEnvironment == neutralReplica.PhysicalEnvironment {
					candidateInstanceKey = &neutralReplica.Key
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but orchestrator picks %+v as candidate replacement, based on being in same DC & env as promoted instance", promotedReplica.Key, neutralReplica.Key))
				}
			}
		}
		if candidateInstanceKey == nil { // 还没有选出候选实例
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ searching for a neutral server to replace a prefer_not"))
			for _, neutralReplica := range neutralReplicas {
				if canTakeOverPromotedServerAsMaster(neutralReplica, promotedReplica) {
					if satisfied, reason := MasterFailoverGeographicConstraintSatisfied(&topologyRecovery.AnalysisEntry, neutralReplica); satisfied {
						// OK, better than nothing
						candidateInstanceKey = &neutralReplica.Key
						AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but orchestrator picks %+v as candidate replacement, based on promoted instance having prefer_not promotion rule", promotedReplica.Key, neutralReplica.Key))
					} else {
						AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("skipping %+v; %s", neutralReplica.Key, reason))
					}
				}
			}
		}
	}

	// So do we have a candidate?
	if candidateInstanceKey == nil {
		// Found nothing. Stick with promoted replica
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ found no server to promote on top promoted replica"))
		return promotedReplica, false, nil
	}
	if promotedReplica.Key.Equals(candidateInstanceKey) {
		// Sanity. It IS the candidate, nothing to promote...
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ sanity check: found our very own server to promote; doing nothing"))
		return promotedReplica, false, nil
	}
	replacement, _, err = inst.ReadInstance(candidateInstanceKey)
	return replacement, true, err
}

// replacePromotedReplicaWithCandidate is called after a master (or co-master) replacePromotedReplicaWithCandidate  当主库宕机被其他实例替代后这个函数会被调用执行、
// died and was replaced by some promotedReplica.
// But, is there an even better replica to promote? 但是 ，这里有更适合的从实例被提升吗？
// if candidateInstanceKey is given, then it is forced to be promoted over the promotedReplica 如果候选实例的Key的被提供，这个实例会被强制提升
// Otherwise, search for the best to promote!其他情况 会搜索一个更好的去提升
func replacePromotedReplicaWithCandidate(topologyRecovery *TopologyRecovery, deadInstanceKey *inst.InstanceKey, promotedReplica *inst.Instance, candidateInstanceKey *inst.InstanceKey) (*inst.Instance, error) {
	candidateInstance, actionRequired, err := SuggestReplacementForPromotedReplica(topologyRecovery, deadInstanceKey, promotedReplica, candidateInstanceKey)
	if err != nil {// 有报错
		return promotedReplica, log.Errore(err)
	}
	if !actionRequired { // 不需要改变
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("replace-promoted-replica-with-candidate: promoted instance %+v requires no further action", promotedReplica.Key))
		return promotedReplica, nil
	}

	// Try and promote suggested candidate, if applicable and possible 尝试提升新的候选实例为新主
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("replace-promoted-replica-with-candidate: promoted instance %+v is not the suggested candidate %+v. Will see what can be done", promotedReplica.Key, candidateInstance.Key))

	if candidateInstance.MasterKey.Equals(&promotedReplica.Key) { // 候选实例为第一次被提升主库的从库 ，将候选实例提升为主库
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("replace-promoted-replica-with-candidate: suggested candidate %+v is replica of promoted instance %+v. Will try and take its master", candidateInstance.Key, promotedReplica.Key))
		candidateInstance, err = inst.TakeMaster(&candidateInstance.Key, topologyRecovery.Type == CoMasterRecovery)
		if err != nil {
			return promotedReplica, log.Errore(err)
		}
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("success promoting %+v over %+v", candidateInstance.Key, promotedReplica.Key))

		// As followup to taking over, let's relocate all the rest of the replicas under the candidate instance
		relocateReplicasFunc := func() error { // 将其他从库 变为新主库的 从库
			log.Debugf("replace-promoted-replica-with-candidate: relocating replicas of %+v below %+v", promotedReplica.Key, candidateInstance.Key)

			relocatedReplicas, _, err, _ := inst.RelocateReplicas(&promotedReplica.Key, &candidateInstance.Key, "")
			log.Debugf("replace-promoted-replica-with-candidate: + relocated %+v replicas of %+v below %+v", len(relocatedReplicas), promotedReplica.Key, candidateInstance.Key)
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("relocated %+v replicas of %+v below %+v", len(relocatedReplicas), promotedReplica.Key, candidateInstance.Key))
			return log.Errore(err)
		}
		postponedFunctionsContainer := &topologyRecovery.PostponedFunctionsContainer
		if postponedFunctionsContainer != nil {
			postponedFunctionsContainer.AddPostponedFunction(relocateReplicasFunc, fmt.Sprintf("replace-promoted-replica-with-candidate: relocate replicas of %+v", promotedReplica.Key))
		} else {
			_ = relocateReplicasFunc()
			// We do not propagate the error. It is logged, but otherwise should not fail the entire failover operation
		}
		return candidateInstance, nil
	}

	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("could not manage to promoted suggested candidate %+v", candidateInstance.Key))
	return promotedReplica, nil
}

// checkAndRecoverDeadMaster checks a given analysis, decides whether to take action, and possibly takes action
// Returns true when action was taken.
// checkAndRecoverDeadMaster 检查给定的分析情况，决定是否采取行动，然后可能采取行动。
// 当采取了行动时返回 true。
func checkAndRecoverDeadMaster(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	if !(forceInstanceRecovery || analysisEntry.ClusterDetails.HasAutomatedMasterRecovery) {
		return false, nil, nil
	}
	topologyRecovery, err = AttemptRecoveryRegistration(&analysisEntry, !forceInstanceRecovery, !forceInstanceRecovery)
	if topologyRecovery == nil {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another RecoverDeadMaster.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}

	// That's it! We must do recovery! 代码已经经过一系列的检查和条件判断，最终决定必须执行恢复操作。
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("will handle DeadMaster event on %+v", analysisEntry.ClusterDetails.ClusterName))
	recoverDeadMasterCounter.Inc(1)
	recoveryAttempted, promotedReplica, lostReplicas, err := recoverDeadMaster(topologyRecovery, candidateInstanceKey, skipProcesses)
	if err != nil {
		AuditTopologyRecovery(topologyRecovery, err.Error())
	}
	topologyRecovery.LostReplicas.AddInstances(lostReplicas)
	if !recoveryAttempted {
		return false, topologyRecovery, err
	}

	overrideMasterPromotion := func() (*inst.Instance, error) {// 匿名函数
		if promotedReplica == nil {
			// No promotion; nothing to override.
			return promotedReplica, err
		}
		// Scenarios where we might cancel the promotion. 我们可能会取消这次 提升 的场景
		if satisfied, reason := MasterFailoverGeographicConstraintSatisfied(&analysisEntry, promotedReplica); !satisfied { // 跨机房 切换
			return nil, fmt.Errorf("RecoverDeadMaster: failed %+v promotion; %s", promotedReplica.Key, reason)
		}
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: promoted replica lag seconds: %+v", promotedReplica.ReplicationLagSeconds.Int64))
		if config.Config.FailMasterPromotionOnLagMinutes > 0 && // FailMasterPromotionOnLagMinutes 设置了大于0 的值
		// 被提升的实例有延迟  超过 FailMasterPromotionOnLagMinutes 设置
			time.Duration(promotedReplica.ReplicationLagSeconds.Int64)*time.Second >= time.Duration(config.Config.FailMasterPromotionOnLagMinutes)*time.Minute {
			// candidate replica lags too much 候选从副本延迟太多
			return nil, fmt.Errorf("RecoverDeadMaster: failed promotion. FailMasterPromotionOnLagMinutes is set to %d (minutes) and promoted replica %+v 's lag is %d (seconds)", config.Config.FailMasterPromotionOnLagMinutes, promotedReplica.Key, promotedReplica.ReplicationLagSeconds.Int64)
		}
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: promoted replica sql thread up-to-date: %+v", promotedReplica.SQLThreadUpToDate()))
		// 如果设置了参数FailMasterPromotionIfSQLThreadNotUpToDate， 并且sql thread不是最新的，则提升失败
		if config.Config.FailMasterPromotionIfSQLThreadNotUpToDate && !promotedReplica.SQLThreadUpToDate() {
			return nil, fmt.Errorf("RecoverDeadMaster: failed promotion. FailMasterPromotionIfSQLThreadNotUpToDate is set and promoted replica %+v 's sql thread is not up to date (relay logs still unapplied). Aborting promotion", promotedReplica.Key)
		}
		// 如果设置了参数 DelayMasterPromotionIfSQLThreadNotUpToDate ，并且 sql thread不是最新的，则等待SQL thread 追赶应用中继日志
		if config.Config.DelayMasterPromotionIfSQLThreadNotUpToDate && !promotedReplica.SQLThreadUpToDate() {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("DelayMasterPromotionIfSQLThreadNotUpToDate: waiting for SQL thread on %+v", promotedReplica.Key))
			if _, err := inst.WaitForSQLThreadUpToDate(&promotedReplica.Key, 0, 0); err != nil {
				return nil, fmt.Errorf("DelayMasterPromotionIfSQLThreadNotUpToDate error: %+v", err)
			}
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("DelayMasterPromotionIfSQLThreadNotUpToDate: SQL thread caught up on %+v", promotedReplica.Key))
		}
		// All seems well. No override done.
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: found no reason to override promotion of %+v", promotedReplica.Key))
		return promotedReplica, err
	}
	if promotedReplica, err = overrideMasterPromotion(); err != nil { // 执行匿名函数
		AuditTopologyRecovery(topologyRecovery, err.Error())
	}
	// And this is the end; whether successful or not, we're done. 已经做了切换
	resolveRecovery(topologyRecovery, promotedReplica)
	// Now, see whether we are successful or not. From this point there's no going back. 无法回头
	if promotedReplica != nil {
		// Success! 成功
		recoverDeadMasterSuccessCounter.Inc(1)
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: successfully promoted %+v", promotedReplica.Key))
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: promoted server coordinates: %+v", promotedReplica.SelfBinlogCoordinates))

		if config.Config.ApplyMySQLPromotionAfterMasterFailover || analysisEntry.CommandHint == inst.GracefulMasterTakeoverCommandHint {
			// on GracefulMasterTakeoverCommandHint it makes utter sense to RESET SLAVE ALL and read_only=0, and there is no sense in not doing so.
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: will apply MySQL changes to promoted master"))
			{
				_, err := inst.ResetReplicationOperation(&promotedReplica.Key)
				if err != nil {
					// Ugly, but this is important. Let's give it another try
					_, err = inst.ResetReplicationOperation(&promotedReplica.Key)
				}
				AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: applying RESET SLAVE ALL on promoted master: success=%t", (err == nil)))
				if err != nil {
					AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: NOTE that %+v is promoted even though SHOW SLAVE STATUS may still show it has a master", promotedReplica.Key))
				}
			}
			{
				_, err := inst.SetReadOnly(&promotedReplica.Key, false)
				AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: applying read-only=0 on promoted master: success=%t", (err == nil)))
			}
			// Let's attempt, though we won't necessarily succeed, to set old master as read-only
			go func() {
				_, err := inst.SetReadOnly(&analysisEntry.AnalyzedInstanceKey, true)
				AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: applying read-only=1 on demoted master: success=%t", (err == nil)))
			}()
		}

		kvPairs := inst.GetClusterMasterKVPairs(analysisEntry.ClusterDetails.ClusterAlias, &promotedReplica.Key)
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Writing KV %+v", kvPairs))
		if orcraft.IsRaftEnabled() {
			for _, kvPair := range kvPairs {
				_, err := orcraft.PublishCommand("put-key-value", kvPair)
				log.Errore(err)
			}
			// since we'll be affecting 3rd party tools here, we _prefer_ to mitigate re-applying
			// of the put-key-value event upon startup. We _recommend_ a snapshot in the near future.
			go orcraft.PublishCommand("async-snapshot", "")
		} else {
			err := kv.PutKVPairs(kvPairs)
			log.Errore(err)
		}
		{
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Distributing KV %+v", kvPairs))
			err := kv.DistributePairs(kvPairs)
			log.Errore(err)
		}
		if config.Config.MasterFailoverDetachReplicaMasterHost {
			postponedFunction := func() error {
				AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: detaching master host on promoted master"))
				inst.DetachReplicaMasterHost(&promotedReplica.Key)
				return nil
			}
			topologyRecovery.AddPostponedFunction(postponedFunction, fmt.Sprintf("RecoverDeadMaster, detaching promoted master host %+v", promotedReplica.Key))
		}
		func() error {
			before := analysisEntry.AnalyzedInstanceKey.StringCode()
			after := promotedReplica.Key.StringCode()
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: updating cluster_alias: %v -> %v", before, after))
			//~~~inst.ReplaceClusterName(before, after)
			if alias := analysisEntry.ClusterDetails.ClusterAlias; alias != "" {
				inst.SetClusterAlias(promotedReplica.Key.StringCode(), alias)
			} else {
				inst.ReplaceAliasClusterName(before, after)
			}
			return nil
		}()

		attributes.SetGeneralAttribute(analysisEntry.ClusterDetails.ClusterDomain, promotedReplica.Key.StringCode())

		if !skipProcesses {
			// Execute post master-failover processes
			executeProcesses(config.Config.PostMasterFailoverProcesses, "PostMasterFailoverProcesses", topologyRecovery, false)
		}
	} else {
		recoverDeadMasterFailureCounter.Inc(1)
	}

	return true, topologyRecovery, err
}

// isGenerallyValidAsCandidateSiblingOfIntermediateMaster sees that basic server configuration and state are valid
func isGenerallyValidAsCandidateSiblingOfIntermediateMaster(sibling *inst.Instance) bool {
	if !sibling.LogBinEnabled {
		return false
	}
	if !sibling.LogReplicationUpdatesEnabled {
		return false
	}
	if !sibling.ReplicaRunning() {
		return false
	}
	if !sibling.IsLastCheckValid {
		return false
	}
	return true
}

// isValidAsCandidateSiblingOfIntermediateMaster checks to see that the given sibling is capable to take over instance's replicas
func isValidAsCandidateSiblingOfIntermediateMaster(intermediateMasterInstance *inst.Instance, sibling *inst.Instance) bool {
	if sibling.Key.Equals(&intermediateMasterInstance.Key) {
		// same instance
		return false
	}
	if !isGenerallyValidAsCandidateSiblingOfIntermediateMaster(sibling) {
		return false
	}
	if inst.IsBannedFromBeingCandidateReplica(sibling) {
		return false
	}
	if sibling.HasReplicationFilters != intermediateMasterInstance.HasReplicationFilters {
		return false
	}
	if sibling.IsBinlogServer() != intermediateMasterInstance.IsBinlogServer() {
		// When both are binlog servers, failover is trivial.
		// When failed IM is binlog server, its sibling is still valid, but we catually prefer to just repoint the replica up -- simplest!
		return false
	}
	if sibling.ExecBinlogCoordinates.SmallerThan(&intermediateMasterInstance.ExecBinlogCoordinates) {
		return false
	}
	return true
}

func isGenerallyValidAsWouldBeMaster(replica *inst.Instance, requireLogReplicationUpdates bool) bool {
	if !replica.IsLastCheckValid {
		// something wrong with this replica right now. We shouldn't hope to be able to promote it
		return false
	}
	if !replica.LogBinEnabled {
		return false
	}
	if requireLogReplicationUpdates && !replica.LogReplicationUpdatesEnabled {
		return false
	}
	if replica.IsBinlogServer() {
		return false
	}
	if inst.IsBannedFromBeingCandidateReplica(replica) {
		return false
	}

	return true
}
// canTakeOverPromotedServerAsMaster 替换被提升的实例 成为 主实例
func canTakeOverPromotedServerAsMaster(wantToTakeOver *inst.Instance, toBeTakenOver *inst.Instance) bool {
	if !isGenerallyValidAsWouldBeMaster(wantToTakeOver, true) {
		return false
	}
	if !wantToTakeOver.MasterKey.Equals(&toBeTakenOver.Key) {
		return false
	}
	if canReplicate, _ := toBeTakenOver.CanReplicateFrom(wantToTakeOver); !canReplicate {
		return false
	}
	return true
}

// GetCandidateSiblingOfIntermediateMaster chooses the best sibling of a dead intermediate master
// to whom the IM's replicas can be moved.
func GetCandidateSiblingOfIntermediateMaster(topologyRecovery *TopologyRecovery, intermediateMasterInstance *inst.Instance) (*inst.Instance, error) {

	siblings, err := inst.ReadReplicaInstances(&intermediateMasterInstance.MasterKey)
	if err != nil {
		return nil, err
	}
	if len(siblings) <= 1 {
		return nil, log.Errorf("topology_recovery: no siblings found for %+v", intermediateMasterInstance.Key)
	}

	sort.Sort(sort.Reverse(InstancesByCountReplicas(siblings)))

	// In the next series of steps we attempt to return a good replacement.
	// None of the below attempts is sure to pick a winning server. Perhaps picked server is not enough up-todate -- but
	// this has small likelihood in the general case, and, well, it's an attempt. It's a Plan A, but we have Plan B & C if this fails.

	// At first, we try to return an "is_candidate" server in same dc & env
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("searching for the best candidate sibling of dead intermediate master %+v", intermediateMasterInstance.Key))
	for _, sibling := range siblings {
		sibling := sibling
		if isValidAsCandidateSiblingOfIntermediateMaster(intermediateMasterInstance, sibling) &&
			sibling.IsCandidate &&
			sibling.DataCenter == intermediateMasterInstance.DataCenter &&
			sibling.PhysicalEnvironment == intermediateMasterInstance.PhysicalEnvironment {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found %+v as the ideal candidate", sibling.Key))
			return sibling, nil
		}
	}
	// No candidate in same DC & env, let's search for a candidate anywhere
	for _, sibling := range siblings {
		sibling := sibling
		if isValidAsCandidateSiblingOfIntermediateMaster(intermediateMasterInstance, sibling) && sibling.IsCandidate {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found %+v as a replacement for %+v [candidate sibling]", sibling.Key, intermediateMasterInstance.Key))
			return sibling, nil
		}
	}
	// Go for some valid in the same DC & ENV
	for _, sibling := range siblings {
		sibling := sibling
		if isValidAsCandidateSiblingOfIntermediateMaster(intermediateMasterInstance, sibling) &&
			sibling.DataCenter == intermediateMasterInstance.DataCenter &&
			sibling.PhysicalEnvironment == intermediateMasterInstance.PhysicalEnvironment {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found %+v as a replacement for %+v [same dc & environment]", sibling.Key, intermediateMasterInstance.Key))
			return sibling, nil
		}
	}
	// Just whatever is valid.
	for _, sibling := range siblings {
		sibling := sibling
		if isValidAsCandidateSiblingOfIntermediateMaster(intermediateMasterInstance, sibling) {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found %+v as a replacement for %+v [any sibling]", sibling.Key, intermediateMasterInstance.Key))
			return sibling, nil
		}
	}
	return nil, log.Errorf("topology_recovery: cannot find candidate sibling of %+v", intermediateMasterInstance.Key)
}

// RecoverDeadIntermediateMaster performs intermediate master recovery; complete logic inside
func RecoverDeadIntermediateMaster(topologyRecovery *TopologyRecovery, skipProcesses bool) (successorInstance *inst.Instance, err error) {
	topologyRecovery.Type = IntermediateMasterRecovery
	analysisEntry := &topologyRecovery.AnalysisEntry
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey
	recoveryResolved := false

	inst.AuditOperation("recover-dead-intermediate-master", failedInstanceKey, "problem found; will recover")
	if !skipProcesses {
		if err := executeProcesses(config.Config.PreFailoverProcesses, "PreFailoverProcesses", topologyRecovery, true); err != nil {
			return nil, topologyRecovery.AddError(err)
		}
	}

	intermediateMasterInstance, _, err := inst.ReadInstance(failedInstanceKey)
	if err != nil {
		return nil, topologyRecovery.AddError(err)
	}
	// Find possible candidate
	candidateSiblingOfIntermediateMaster, _ := GetCandidateSiblingOfIntermediateMaster(topologyRecovery, intermediateMasterInstance)
	relocateReplicasToCandidateSibling := func() {
		if candidateSiblingOfIntermediateMaster == nil {
			return
		}
		// We have a candidate
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMaster: will attempt a candidate intermediate master: %+v", candidateSiblingOfIntermediateMaster.Key))
		relocatedReplicas, candidateSibling, err, errs := inst.RelocateReplicas(failedInstanceKey, &candidateSiblingOfIntermediateMaster.Key, "")
		topologyRecovery.AddErrors(errs)
		topologyRecovery.ParticipatingInstanceKeys.AddKey(candidateSiblingOfIntermediateMaster.Key)

		if len(relocatedReplicas) == 0 {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMaster: failed to move any replica to candidate intermediate master (%+v)", candidateSibling.Key))
			return
		}
		if err != nil || len(errs) > 0 {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMaster: move to candidate intermediate master (%+v) did not complete: err: %+v, errs: %+v", candidateSibling.Key, err, errs))
			return
		}
		if err == nil {
			recoveryResolved = true
			successorInstance = candidateSibling

			inst.AuditOperation("recover-dead-intermediate-master", failedInstanceKey, fmt.Sprintf("Relocated %d replicas under candidate sibling: %+v; %d errors: %+v", len(relocatedReplicas), candidateSibling.Key, len(errs), errs))
		}
	}
	// Plan A: find a replacement intermediate master in same Data Center
	if candidateSiblingOfIntermediateMaster != nil && candidateSiblingOfIntermediateMaster.DataCenter == intermediateMasterInstance.DataCenter {
		relocateReplicasToCandidateSibling()
	}
	if !recoveryResolved {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMaster: will next attempt regrouping of replicas"))
		// Plan B: regroup (we wish to reduce cross-DC replication streams)
		lostReplicas, _, _, _, regroupPromotedReplica, regroupError := inst.RegroupReplicas(failedInstanceKey, true, nil, nil)
		if regroupError != nil {
			topologyRecovery.AddError(regroupError)
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMaster: regroup failed on: %+v", regroupError))
		}
		if regroupPromotedReplica != nil {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMaster: regrouped under %+v, with %d lost replicas", regroupPromotedReplica.Key, len(lostReplicas)))
			topologyRecovery.ParticipatingInstanceKeys.AddKey(regroupPromotedReplica.Key)
			if len(lostReplicas) == 0 && regroupError == nil {
				// Seems like the regroup worked flawlessly. The local replica took over all of its siblings.
				// We can consider this host to be the successor.
				successorInstance = regroupPromotedReplica
			}
		}
		// Plan C: try replacement intermediate master in other DC...
		if candidateSiblingOfIntermediateMaster != nil && candidateSiblingOfIntermediateMaster.DataCenter != intermediateMasterInstance.DataCenter {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMaster: will next attempt relocating to another DC server"))
			relocateReplicasToCandidateSibling()
		}
	}
	if !recoveryResolved {
		// Do we still have leftovers? some replicas couldn't move? Couldn't regroup? Only left with regroup's resulting leader?
		// nothing moved?
		// We don't care much if regroup made it or not. We prefer that it made it, in which case we only need to relocate up
		// one replica, but the operation is still valid if regroup partially/completely failed. We just promote anything
		// not regrouped.
		// So, match up all that's left, plan D
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMaster: will next attempt to relocate up from %+v", *failedInstanceKey))

		relocatedReplicas, masterInstance, err, errs := inst.RelocateReplicas(failedInstanceKey, &analysisEntry.AnalyzedInstanceMasterKey, "")
		topologyRecovery.AddErrors(errs)
		topologyRecovery.ParticipatingInstanceKeys.AddKey(analysisEntry.AnalyzedInstanceMasterKey)

		if len(relocatedReplicas) > 0 {
			recoveryResolved = true
			if successorInstance == nil {
				// There could have been a local replica taking over its siblings. We'd like to consider that one as successor.
				successorInstance = masterInstance
			}
			inst.AuditOperation("recover-dead-intermediate-master", failedInstanceKey, fmt.Sprintf("Relocated replicas under: %+v %d errors: %+v", successorInstance.Key, len(errs), errs))
		} else {
			err = log.Errorf("topology_recovery: RecoverDeadIntermediateMaster failed to match up any replica from %+v", *failedInstanceKey)
			topologyRecovery.AddError(err)
		}
	}
	if !recoveryResolved {
		successorInstance = nil
	}
	resolveRecovery(topologyRecovery, successorInstance)
	return successorInstance, err
}

// RecoverDeadReplicationGroupMemberWithReplicas performs dead group member recovery. It does so by finding members of
// the same replication group of the one of the failed instance, picking a random one and relocating replicas to it.
func RecoverDeadReplicationGroupMemberWithReplicas(topologyRecovery *TopologyRecovery, skipProcesses bool) (successorInstance *inst.Instance, err error) {
	topologyRecovery.Type = ReplicationGroupMemberRecovery
	analysisEntry := &topologyRecovery.AnalysisEntry
	failedGroupMemberInstanceKey := &analysisEntry.AnalyzedInstanceKey
	inst.AuditOperation("recover-dead-replication-group-member-with-replicas", failedGroupMemberInstanceKey, "problem found; will recover")
	if !skipProcesses {
		if err := executeProcesses(config.Config.PreFailoverProcesses, "PreFailoverProcesses", topologyRecovery, true); err != nil {
			return nil, topologyRecovery.AddError(err)
		}
	}
	failedGroupMember, _, err := inst.ReadInstance(failedGroupMemberInstanceKey)
	if err != nil {
		return nil, topologyRecovery.AddError(err)
	}
	// Find a group member under which we can relocate the replicas of the failed one.
	groupMembers := failedGroupMember.ReplicationGroupMembers.GetInstanceKeys()
	if len(groupMembers) == 0 {
		return nil, topologyRecovery.AddError(errors.New("RecoverDeadReplicationGroupMemberWithReplicas: unable to find a candidate group member to relocate replicas to"))
	}
	// We have a group member to move replicas to, go ahead and do that
	AuditTopologyRecovery(topologyRecovery, "Finding a candidate group member to relocate replicas to")
	candidateGroupMemberInstanceKey := &groupMembers[rand.Intn(len(failedGroupMember.ReplicationGroupMembers.GetInstanceKeys()))]
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Found group member %+v", candidateGroupMemberInstanceKey))
	relocatedReplicas, successorInstance, err, errs := inst.RelocateReplicas(failedGroupMemberInstanceKey, candidateGroupMemberInstanceKey, "")
	topologyRecovery.AddErrors(errs)
	if len(relocatedReplicas) != len(failedGroupMember.Replicas.GetInstanceKeys()) {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadReplicationGroupMemberWithReplicas: failed to move all replicas to candidate group member (%+v)", candidateGroupMemberInstanceKey))
		return nil, topologyRecovery.AddError(errors.New(fmt.Sprintf("RecoverDeadReplicationGroupMemberWithReplicas: Unable to relocate replicas to +%v", candidateGroupMemberInstanceKey)))
	}
	AuditTopologyRecovery(topologyRecovery, "All replicas successfully relocated")
	resolveRecovery(topologyRecovery, successorInstance)
	return successorInstance, err
}

// checkAndRecoverDeadIntermediateMaster checks a given analysis, decides whether to take action, and possibly takes action
// Returns true when action was taken.
func checkAndRecoverDeadIntermediateMaster(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *TopologyRecovery, error) {
	if !(forceInstanceRecovery || analysisEntry.ClusterDetails.HasAutomatedIntermediateMasterRecovery) {
		return false, nil, nil
	}
	topologyRecovery, err := AttemptRecoveryRegistration(&analysisEntry, !forceInstanceRecovery, !forceInstanceRecovery)
	if topologyRecovery == nil {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMaster: found an active or recent recovery on %+v. Will not issue another RecoverDeadIntermediateMaster.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}

	// That's it! We must do recovery!
	recoverDeadIntermediateMasterCounter.Inc(1)
	promotedReplica, err := RecoverDeadIntermediateMaster(topologyRecovery, skipProcesses)
	if promotedReplica != nil {
		// success
		recoverDeadIntermediateMasterSuccessCounter.Inc(1)

		if !skipProcesses {
			// Execute post intermediate-master-failover processes
			topologyRecovery.SuccessorKey = &promotedReplica.Key
			topologyRecovery.SuccessorAlias = promotedReplica.InstanceAlias
			executeProcesses(config.Config.PostIntermediateMasterFailoverProcesses, "PostIntermediateMasterFailoverProcesses", topologyRecovery, false)
		}
	} else {
		recoverDeadIntermediateMasterFailureCounter.Inc(1)
	}
	return true, topologyRecovery, err
}

// RecoverDeadCoMaster recovers a dead co-master, complete logic inside
func RecoverDeadCoMaster(topologyRecovery *TopologyRecovery, skipProcesses bool) (promotedReplica *inst.Instance, lostReplicas [](*inst.Instance), err error) {
	topologyRecovery.Type = CoMasterRecovery
	analysisEntry := &topologyRecovery.AnalysisEntry
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey
	otherCoMasterKey := &analysisEntry.AnalyzedInstanceMasterKey
	otherCoMaster, found, _ := inst.ReadInstance(otherCoMasterKey)
	if otherCoMaster == nil || !found {
		return nil, lostReplicas, topologyRecovery.AddError(log.Errorf("RecoverDeadCoMaster: could not read info for co-master %+v of %+v", *otherCoMasterKey, *failedInstanceKey))
	}
	inst.AuditOperation("recover-dead-co-master", failedInstanceKey, "problem found; will recover")
	if !skipProcesses {
		if err := executeProcesses(config.Config.PreFailoverProcesses, "PreFailoverProcesses", topologyRecovery, true); err != nil {
			return nil, lostReplicas, topologyRecovery.AddError(err)
		}
	}

	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadCoMaster: will recover %+v", *failedInstanceKey))

	var coMasterRecoveryType MasterRecoveryType = MasterRecoveryPseudoGTID
	if analysisEntry.OracleGTIDImmediateTopology || analysisEntry.MariaDBGTIDImmediateTopology {
		coMasterRecoveryType = MasterRecoveryGTID
	}

	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadCoMaster: coMasterRecoveryType=%+v", coMasterRecoveryType))

	var cannotReplicateReplicas [](*inst.Instance)
	switch coMasterRecoveryType {
	case MasterRecoveryGTID:
		{
			lostReplicas, _, cannotReplicateReplicas, promotedReplica, err = inst.RegroupReplicasGTID(failedInstanceKey, true, false, nil, &topologyRecovery.PostponedFunctionsContainer, nil)
		}
	case MasterRecoveryPseudoGTID:
		{
			lostReplicas, _, _, cannotReplicateReplicas, promotedReplica, err = inst.RegroupReplicasPseudoGTIDIncludingSubReplicasOfBinlogServers(failedInstanceKey, true, nil, &topologyRecovery.PostponedFunctionsContainer, nil)
		}
	}
	topologyRecovery.AddError(err)
	lostReplicas = append(lostReplicas, cannotReplicateReplicas...)

	mustPromoteOtherCoMaster := config.Config.CoMasterRecoveryMustPromoteOtherCoMaster
	if !otherCoMaster.ReadOnly {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadCoMaster: other co-master %+v is writeable hence has to be promoted", otherCoMaster.Key))
		mustPromoteOtherCoMaster = true
	}
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadCoMaster: mustPromoteOtherCoMaster? %+v", mustPromoteOtherCoMaster))

	if promotedReplica != nil {
		topologyRecovery.ParticipatingInstanceKeys.AddKey(promotedReplica.Key)
		if mustPromoteOtherCoMaster {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadCoMaster: mustPromoteOtherCoMaster. Verifying that %+v is/can be promoted", *otherCoMasterKey))
			promotedReplica, err = replacePromotedReplicaWithCandidate(topologyRecovery, failedInstanceKey, promotedReplica, otherCoMasterKey)
		} else {
			// We are allowed to promote any server
			promotedReplica, err = replacePromotedReplicaWithCandidate(topologyRecovery, failedInstanceKey, promotedReplica, nil)
		}
		topologyRecovery.AddError(err)
	}
	if promotedReplica != nil {
		if mustPromoteOtherCoMaster && !promotedReplica.Key.Equals(otherCoMasterKey) {
			topologyRecovery.AddError(log.Errorf("RecoverDeadCoMaster: could not manage to promote other-co-master %+v; was only able to promote %+v; mustPromoteOtherCoMaster is true (either CoMasterRecoveryMustPromoteOtherCoMaster is true, or co-master is writeable), therefore failing", *otherCoMasterKey, promotedReplica.Key))
			promotedReplica = nil
		}
	}
	if promotedReplica != nil {
		if config.Config.DelayMasterPromotionIfSQLThreadNotUpToDate {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Waiting to ensure the SQL thread catches up on %+v", promotedReplica.Key))
			if _, err := inst.WaitForSQLThreadUpToDate(&promotedReplica.Key, 0, 0); err != nil {
				return promotedReplica, lostReplicas, err
			}
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("SQL thread caught up on %+v", promotedReplica.Key))
		}
		topologyRecovery.ParticipatingInstanceKeys.AddKey(promotedReplica.Key)
	}

	// OK, we may have someone promoted. Either this was the other co-master or another replica.
	// Noting down that we DO NOT attempt to set a new co-master topology. We are good with remaining with a single master.
	// I tried solving the "let's promote a replica and create a new co-master setup" but this turns so complex due to various factors.
	// I see this as risky and not worth the questionable benefit.
	// Maybe future me is a smarter person and finds a simple solution. Unlikely. I'm getting dumber.
	//
	// ...
	// Now that we're convinved, take a look at what we can be left with:
	// Say we started with M1<->M2<-S1, with M2 failing, and we promoted S1.
	// We now have M1->S1 (because S1 is promoted), S1->M2 (because that's what it remembers), M2->M1 (because that's what it remembers)
	// !! This is an evil 3-node circle that must be broken.
	// config.Config.ApplyMySQLPromotionAfterMasterFailover, if true, will cause it to break, because we would RESET SLAVE on S1
	// but we want to make sure the circle is broken no matter what.
	// So in the case we promoted not-the-other-co-master, we issue a detach-replica-master-host, which is a reversible operation
	if promotedReplica != nil && !promotedReplica.Key.Equals(otherCoMasterKey) {
		_, err = inst.DetachReplicaMasterHost(&promotedReplica.Key)
		topologyRecovery.AddError(log.Errore(err))
	}

	if promotedReplica != nil && len(lostReplicas) > 0 && config.Config.DetachLostReplicasAfterMasterFailover {
		postponedFunction := func() error {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadCoMaster: lost %+v replicas during recovery process; detaching them", len(lostReplicas)))
			for _, replica := range lostReplicas {
				replica := replica
				inst.DetachReplicaMasterHost(&replica.Key)
			}
			return nil
		}
		topologyRecovery.AddPostponedFunction(postponedFunction, fmt.Sprintf("RecoverDeadCoMaster, detaching %+v replicas", len(lostReplicas)))
	}

	func() error {
		inst.BeginDowntime(inst.NewDowntime(failedInstanceKey, inst.GetMaintenanceOwner(), inst.DowntimeLostInRecoveryMessage, time.Duration(config.LostInRecoveryDowntimeSeconds)*time.Second))
		acknowledgeInstanceFailureDetection(&analysisEntry.AnalyzedInstanceKey)
		for _, replica := range lostReplicas {
			replica := replica
			inst.BeginDowntime(inst.NewDowntime(&replica.Key, inst.GetMaintenanceOwner(), inst.DowntimeLostInRecoveryMessage, time.Duration(config.LostInRecoveryDowntimeSeconds)*time.Second))
		}
		return nil
	}()

	return promotedReplica, lostReplicas, err
}

// checkAndRecoverDeadCoMaster checks a given analysis, decides whether to take action, and possibly takes action
// Returns true when action was taken.
func checkAndRecoverDeadCoMaster(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *TopologyRecovery, error) {
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey
	if !(forceInstanceRecovery || analysisEntry.ClusterDetails.HasAutomatedMasterRecovery) {
		return false, nil, nil
	}
	topologyRecovery, err := AttemptRecoveryRegistration(&analysisEntry, !forceInstanceRecovery, !forceInstanceRecovery)
	if topologyRecovery == nil {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another RecoverDeadCoMaster.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}

	// That's it! We must do recovery!
	recoverDeadCoMasterCounter.Inc(1)
	promotedReplica, lostReplicas, err := RecoverDeadCoMaster(topologyRecovery, skipProcesses)
	resolveRecovery(topologyRecovery, promotedReplica)
	if promotedReplica == nil {
		inst.AuditOperation("recover-dead-co-master", failedInstanceKey, "Failure: no replica promoted.")
	} else {
		inst.AuditOperation("recover-dead-co-master", failedInstanceKey, fmt.Sprintf("promoted: %+v", promotedReplica.Key))
	}
	topologyRecovery.LostReplicas.AddInstances(lostReplicas)
	if promotedReplica != nil {
		if config.Config.FailMasterPromotionIfSQLThreadNotUpToDate && !promotedReplica.SQLThreadUpToDate() {
			return false, nil, log.Errorf("Promoted replica %+v: sql thread is not up to date (relay logs still unapplied). Aborting promotion", promotedReplica.Key)
		}
		// success
		recoverDeadCoMasterSuccessCounter.Inc(1)

		if config.Config.ApplyMySQLPromotionAfterMasterFailover {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: will apply MySQL changes to promoted master"))
			inst.SetReadOnly(&promotedReplica.Key, false)
		}
		if !skipProcesses {
			// Execute post intermediate-master-failover processes
			topologyRecovery.SuccessorKey = &promotedReplica.Key
			topologyRecovery.SuccessorAlias = promotedReplica.InstanceAlias
			executeProcesses(config.Config.PostMasterFailoverProcesses, "PostMasterFailoverProcesses", topologyRecovery, false)
		}
	} else {
		recoverDeadCoMasterFailureCounter.Inc(1)
	}
	return true, topologyRecovery, err
}

// checkAndRecoverNonWriteableMaster attempts to recover from a read only master by turning it writeable.
// This behavior is feature protected, see config.Config.RecoverNonWriteableMaster
func checkAndRecoverNonWriteableMaster(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	if !config.Config.RecoverNonWriteableMaster {
		return false, nil, nil
	}

	topologyRecovery, err = AttemptRecoveryRegistration(&analysisEntry, true, true)
	if topologyRecovery == nil {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another checkAndRecoverNonWriteableMaster.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}

	inst.AuditOperation("recover-non-writeable-master", &analysisEntry.AnalyzedInstanceKey, "problem found; will recover")
	if !skipProcesses {
		if err := executeProcesses(config.Config.PreFailoverProcesses, "PreFailoverProcesses", topologyRecovery, true); err != nil {
			return false, topologyRecovery, topologyRecovery.AddError(err)
		}
	}

	instance, err := inst.SetReadOnly(&analysisEntry.AnalyzedInstanceKey, false)
	if err == nil {
		resolveRecovery(topologyRecovery, instance)
	}
	return true, topologyRecovery, err
}

// checkAndRecoverLockedSemiSyncMaster
func checkAndRecoverLockedSemiSyncMaster(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	topologyRecovery, err = AttemptRecoveryRegistration(&analysisEntry, true, true)
	if topologyRecovery == nil {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another RecoverLockedSemiSyncMaster.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}
	if config.Config.EnforceExactSemiSyncReplicas {
		return recoverSemiSyncReplicas(topologyRecovery, analysisEntry, true)
	}
	if config.Config.RecoverLockedSemiSyncMaster {
		return recoverSemiSyncReplicas(topologyRecovery, analysisEntry, false)
	}
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no action taken to recover locked semi sync master on %+v. Enable RecoverLockedSemiSyncMaster or EnforceExactSemiSyncReplicas change this behavior.", analysisEntry.AnalyzedInstanceKey))
	return false, nil, err
}

// checkAndRecoverMasterWithTooManySemiSyncReplicas registers and performs a recovery for MasterWithTooManySemiSyncReplicas
func checkAndRecoverMasterWithTooManySemiSyncReplicas(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	topologyRecovery, err = AttemptRecoveryRegistration(&analysisEntry, true, true)
	if topologyRecovery == nil {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another RecoverMasterWithTooManySemiSyncReplicas.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}
	return recoverSemiSyncReplicas(topologyRecovery, analysisEntry, true)
}

// recoverSemiSyncReplicas analyzes the replica topology for the given master and applies to repair it. If exactReplicaTopology is set, it will enable/disable the semi-sync enabled
// variable (rpl_semi_sync_replica_enabled) of the replicas depending on their semi-sync priority and promotion rule. If exactReplicaTopology is not set, the function will only ever
// enable semi-sync on replicas and never disable it. This variable typically corresponds to the EnforceExactSemiSyncReplicas config variable.

// recoverSemiSyncReplicas 分析给定主服务器的复制拓扑，并应用修复。如果设置了 exactReplicaTopology，它将根据半同步优先级和晋升规则启用/禁用副本的半同步启用变量（rpl_semi_sync_replica_enabled）。
// 如果未设置 exactReplicaTopology，则该函数只会在副本上启用半同步，永远不会禁用它。这个变量通常对应于 EnforceExactSemiSyncReplicas 配置变量。

func recoverSemiSyncReplicas(topologyRecovery *TopologyRecovery, analysisEntry inst.ReplicationAnalysis, exactReplicaTopology bool) (recoveryAttempted bool, topologyRecoveryOut *TopologyRecovery, err error) {
	masterInstance, replicas, actions, err := inst.AnalyzeSemiSyncReplicaTopology(&analysisEntry.AnalyzedInstanceKey, nil, exactReplicaTopology)
	if err != nil {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("semi-sync: %s", err.Error()))
		return true, topologyRecovery, log.Errorf("semi-sync: %s", err.Error())
	} else if len(actions) == 0 {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("semi-sync: cannot determine actions based on possible semi-sync replicas; cannot recover on %+v", &analysisEntry.AnalyzedInstanceKey))
		return true, topologyRecovery, log.Errorf("cannot determine actions based on possible semi-sync replicas; cannot recover on %+v", &analysisEntry.AnalyzedInstanceKey)
	}

	// Disable semi-sync master on all replicas; this is to avoid semi-sync failures on the replicas (rpl_semi_sync_master_no_tx)
	// and to make it consistent with the logic in SetReadOnly

	// 在所有副本上禁用半同步主；这是为了避免在副本上出现半同步失败（rpl_semi_sync_master_no_tx），并使其与 SetReadOnly 中的逻辑保持一致

	for _, replica := range replicas {
		inst.MaybeDisableSemiSyncMaster(replica) // it's okay if this fails 如果这失败了也没关系
	}

	// Take action: we first enable and then disable (two loops) in order to avoid "locked master" scenarios
	// 采取行动：我们首先启用，然后禁用（两个循环），以避免“锁定主服务器”的情况
	AuditTopologyRecovery(topologyRecovery, "semi-sync: taking actions:")
	for replica, enable := range actions {
		if enable {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("semi-sync: - %s: setting rpl_semi_sync_slave_enabled=%t, restarting slave_io thread", replica.Key.String(), enable))
			if _, err := inst.SetSemiSyncReplica(&replica.Key, enable); err != nil {
				return true, topologyRecovery, log.Errorf("cannot enable semi sync on replica %+v", replica.Key)
			}
		}
	}
	for replica, enable := range actions {
		if !enable {
			AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("semi-sync: - %s: setting rpl_semi_sync_slave_enabled=%t, restarting slave_io thread", replica.Key.String(), enable))
			if _, err := inst.SetSemiSyncReplica(&replica.Key, enable); err != nil {
				return true, topologyRecovery, fmt.Errorf("cannot disable semi sync on replica %+v", replica.Key)
			}
		}
	}

	resolveRecovery(topologyRecovery, masterInstance)
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("semi-sync: recovery complete; success = %t", topologyRecovery.IsSuccessful))
	return true, topologyRecovery, nil
}

// checkAndRecoverGenericProblem is a general-purpose recovery function
func checkAndRecoverGenericProblem(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *TopologyRecovery, error) {
	return false, nil, nil
}

// checkAndRecoverDeadGroupMemberWithReplicas checks whether action needs to be taken for an analysis involving a dead
// replication group member, and takes the action if applicable. Notice that under our view of the world, a primary
// replication group member is akin to a master in traditional async/semisync replication; whereas secondary group
// members are akin to intermediate masters. Considering also that a failed group member can always be considered as a
// secondary (even if it was primary, the group should have detected its failure and elected a new primary), then
// failure of a group member with replicas is akin to failure of an intermediate master.
func checkAndRecoverDeadGroupMemberWithReplicas(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *TopologyRecovery, error) {
	// Don't proceed with recovery unless it was forced or automatic intermediate source recovery is enabled.
	// We consider failed group members akin to failed intermediate masters, so we re-use the configuration for
	// intermediates.
	if !(forceInstanceRecovery || analysisEntry.ClusterDetails.HasAutomatedIntermediateMasterRecovery) {
		return false, nil, nil
	}
	// Try to record the recovery. It it fails to be recorded, it because it is already being dealt with.
	topologyRecovery, err := AttemptRecoveryRegistration(&analysisEntry, !forceInstanceRecovery, !forceInstanceRecovery)
	if err != nil {
		return false, nil, err
	}
	// Proceed with recovery
	recoverDeadReplicationGroupMemberCounter.Inc(1)

	recoveredToGroupMember, err := RecoverDeadReplicationGroupMemberWithReplicas(topologyRecovery, skipProcesses)

	if recoveredToGroupMember != nil {
		// success
		recoverDeadReplicationGroupMemberSuccessCounter.Inc(1)

		if !skipProcesses {
			// Execute post failover processes
			topologyRecovery.SuccessorKey = &recoveredToGroupMember.Key
			topologyRecovery.SuccessorAlias = recoveredToGroupMember.InstanceAlias
			// For the same reasons that were mentioned above, we re-use the post intermediate master fail-over hooks
			executeProcesses(config.Config.PostIntermediateMasterFailoverProcesses, "PostIntermediateMasterFailoverProcesses", topologyRecovery, false)
		}
	} else {
		recoverDeadReplicationGroupMemberFailureCounter.Inc(1)
	}
	return true, topologyRecovery, err
}

// Force a re-read of a topology instance; this is done because we need to substantiate a suspicion
// that we may have a failover scenario. we want to speed up reading the complete picture.
//即强制重新读取某个拓扑实例的结构。具体而言，这是因为存在某种怀疑，可能涉及到故障转移的情景。通过强制重新读取拓扑结构，希望能够更快地获取完整的信息，以便更好地了解可能存在的故障转移情况。
func emergentlyReadTopologyInstance(instanceKey *inst.InstanceKey, analysisCode inst.AnalysisCode) (instance *inst.Instance, err error) {
	if existsInCacheError := emergencyReadTopologyInstanceMap.Add(instanceKey.StringCode(), true, cache.DefaultExpiration); existsInCacheError != nil {
		// Just recently attempted
		return nil, nil
	}
	instance, err = inst.ReadTopologyInstance(instanceKey)
	inst.AuditOperation("emergently-read-topology-instance", instanceKey, string(analysisCode))
	return instance, err
}

// Force reading of replicas of given instance. This is because we suspect the instance is dead, and want to speed up
// detection of replication failure from its replicas.
func emergentlyReadTopologyInstanceReplicas(instanceKey *inst.InstanceKey, analysisCode inst.AnalysisCode) {
	replicas, err := inst.ReadReplicaInstancesIncludingBinlogServerSubReplicas(instanceKey)
	if err != nil {
		return
	}
	for _, replica := range replicas {
		go emergentlyReadTopologyInstance(&replica.Key, analysisCode)
	}
}

// emergentlyRestartReplicationOnTopologyInstance forces a RestartReplication on a given instance.
func emergentlyRestartReplicationOnTopologyInstance(instanceKey *inst.InstanceKey, analysisCode inst.AnalysisCode) {
	if existsInCacheError := emergencyRestartReplicaTopologyInstanceMap.Add(instanceKey.StringCode(), true, cache.DefaultExpiration); existsInCacheError != nil {
		// Just recently attempted on this specific replica
		return
	}

	go inst.ExecuteOnTopology(func() {
		instance, _, err := inst.ReadInstance(instanceKey)
		if err != nil {
			return
		}
		if instance.UsingMariaDBGTID {
			// In MariaDB GTID, stopping and starting IO thread actually deletes relay logs.
			// This is counter productive to our objective.
			// Specifically, in a situation where the primary is unreachable and where replicas are lagging,
			// we want to restart IO thread to see if lag is actually caused by locked primary. If this results
			// with losing relay logs, then we've lost data.
			// So, unfortunately we avoid this step in MariaDB GTID.
			// See https://github.com/openark/orchestrator/issues/1260
			return
		}

		inst.RestartReplicationQuick(instanceKey)
		inst.AuditOperation("emergently-restart-replication-topology-instance", instanceKey, string(analysisCode))
	})
}

func beginEmergencyOperationGracefulPeriod(instanceKey *inst.InstanceKey) {
	emergencyOperationGracefulPeriodMap.Set(instanceKey.StringCode(), true, cache.DefaultExpiration)
}

func isInEmergencyOperationGracefulPeriod(instanceKey *inst.InstanceKey) bool {
	_, found := emergencyOperationGracefulPeriodMap.Get(instanceKey.StringCode())
	return found
}

// emergentlyRestartReplicationOnTopologyInstanceReplicas forces a stop slave + start slave on
// replicas of a given instance, in an attempt to cause them to re-evaluate their replication state.
// This can be useful in scenarios where the master has Too Many Connections, but long-time connected
// replicas are not seeing this; when they stop+start replication, they need to re-authenticate and
// that's where we hope they realize the master is bad.
// 在拓扑实例的复制品上紧急重启复制，强制执行停止从库并重新开始，以尝试使其重新评估其复制状态。
// 在主服务器存在连接数过多的情况下，但长时间连接的复制品未察觉到这一问题时，这可能很有用；
// 当它们停止并重新开始复制时，它们需要重新进行身份验证，我们希望它们能意识到主服务器出现问题。
func emergentlyRestartReplicationOnTopologyInstanceReplicas(instanceKey *inst.InstanceKey, analysisCode inst.AnalysisCode) {
	if existsInCacheError := emergencyRestartReplicaTopologyInstanceMap.Add(instanceKey.StringCode(), true, cache.DefaultExpiration); existsInCacheError != nil {
		// While each replica's RestartReplication() is throttled on its own, it's also wasteful to
		// iterate all replicas all the time. This is the reason why we do grand-throttle check.
		return
	}
	beginEmergencyOperationGracefulPeriod(instanceKey)

	replicas, err := inst.ReadReplicaInstancesIncludingBinlogServerSubReplicas(instanceKey)
	if err != nil {
		return
	}
	for _, replica := range replicas {
		replicaKey := &replica.Key
		go emergentlyRestartReplicationOnTopologyInstance(replicaKey, analysisCode)
	}
}

func emergentlyRecordStaleBinlogCoordinates(instanceKey *inst.InstanceKey, binlogCoordinates *inst.BinlogCoordinates) {
	err := inst.RecordStaleInstanceBinlogCoordinates(instanceKey, binlogCoordinates)
	log.Errore(err)
}

// checkAndExecuteFailureDetectionProcesses tries to register for failure detection and potentially executes
// failure-detection processes. 尝试注册发现的故障或失败。然后执行故障发现 流程
func checkAndExecuteFailureDetectionProcesses(analysisEntry inst.ReplicationAnalysis, skipProcesses bool) (detectionRegistrationSuccess bool, processesExecutionAttempted bool, err error) {
	if ok, _ := AttemptFailureDetectionRegistration(&analysisEntry); !ok {
		if util.ClearToLog("checkAndExecuteFailureDetectionProcesses", analysisEntry.AnalyzedInstanceKey.StringCode()) {
			log.Infof("checkAndExecuteFailureDetectionProcesses: could not register %+v detection on %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey)
		}
		return false, false, nil
	}
	log.Infof("topology_recovery: detected %+v failure on %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey)
	// Execute on-detection processes  skipProcesses设置的false
	if skipProcesses {
		return true, false, nil
	}
	err = executeProcesses(config.Config.OnFailureDetectionProcesses, "OnFailureDetectionProcesses", NewTopologyRecovery(analysisEntry), true)
	return true, true, err
}

// 这是一个函数 接受两个参数 ，返回一个函数（故障恢复函数） 和 布尔值（该布尔值决定是否会进行真正的恢复）
func getCheckAndRecoverFunction(analysisCode inst.AnalysisCode, analyzedInstanceKey *inst.InstanceKey) (
	checkAndRecoverFunction func(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error),
	isActionableRecovery bool,
) {
	switch analysisCode {
	// master
	case inst.DeadMaster, inst.DeadMasterAndSomeReplicas:
		if isInEmergencyOperationGracefulPeriod(analyzedInstanceKey) {
			return checkAndRecoverGenericProblem, false
		} else {
			return checkAndRecoverDeadMaster, true
		}
	case inst.LockedSemiSyncMaster:
		if isInEmergencyOperationGracefulPeriod(analyzedInstanceKey) {
			return checkAndRecoverGenericProblem, false
		} else {
			return checkAndRecoverLockedSemiSyncMaster, true
		}
	case inst.MasterWithTooManySemiSyncReplicas:
		return checkAndRecoverMasterWithTooManySemiSyncReplicas, true
	// intermediate master
	case inst.DeadIntermediateMaster:
		return checkAndRecoverDeadIntermediateMaster, true
	case inst.DeadIntermediateMasterAndSomeReplicas:
		return checkAndRecoverDeadIntermediateMaster, true
	case inst.DeadIntermediateMasterWithSingleReplicaFailingToConnect:
		return checkAndRecoverDeadIntermediateMaster, true
	case inst.AllIntermediateMasterReplicasFailingToConnectOrDead:
		return checkAndRecoverDeadIntermediateMaster, true
	case inst.DeadIntermediateMasterAndReplicas:
		return checkAndRecoverGenericProblem, false
	// co-master
	case inst.DeadCoMaster:
		return checkAndRecoverDeadCoMaster, true
	case inst.DeadCoMasterAndSomeReplicas:
		return checkAndRecoverDeadCoMaster, true
	// master, non actionable
	case inst.DeadMasterAndReplicas:
		return checkAndRecoverGenericProblem, false
	case inst.UnreachableMaster:
		return checkAndRecoverGenericProblem, false
	case inst.UnreachableMasterWithLaggingReplicas:
		return checkAndRecoverGenericProblem, false
	case inst.AllMasterReplicasNotReplicating:
		return checkAndRecoverGenericProblem, false
	case inst.AllMasterReplicasNotReplicatingOrDead:
		return checkAndRecoverGenericProblem, false
	case inst.UnreachableIntermediateMasterWithLaggingReplicas:
		return checkAndRecoverGenericProblem, false
	// replication group members
	case inst.DeadReplicationGroupMemberWithReplicas:
		return checkAndRecoverDeadGroupMemberWithReplicas, true
	// recoverable structure analysis
	case inst.NoWriteableMasterStructureWarning:
		return checkAndRecoverNonWriteableMaster, true
	}
	// Right now this is mostly causing noise with no clear action.
	// Will revisit this in the future.
	// case inst.AllMasterReplicasStale:
	//   return checkAndRecoverGenericProblem, false

	return nil, false
}

func runEmergentOperations(analysisEntry *inst.ReplicationAnalysis) {
	switch analysisEntry.Analysis {
	case inst.DeadMasterAndReplicas:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceMasterKey, analysisEntry.Analysis)
	case inst.UnreachableMaster:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
		go emergentlyReadTopologyInstanceReplicas(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
	case inst.UnreachableMasterWithLaggingReplicas:
		go emergentlyRestartReplicationOnTopologyInstanceReplicas(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
	case inst.LockedSemiSyncMasterHypothesis:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
		go emergentlyRecordStaleBinlogCoordinates(&analysisEntry.AnalyzedInstanceKey, &analysisEntry.AnalyzedInstanceBinlogCoordinates)
	case inst.UnreachableIntermediateMasterWithLaggingReplicas:
		go emergentlyRestartReplicationOnTopologyInstanceReplicas(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
	case inst.AllMasterReplicasNotReplicating:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
	case inst.AllMasterReplicasNotReplicatingOrDead:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
	case inst.FirstTierReplicaFailingToConnectToMaster:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceMasterKey, analysisEntry.Analysis)
	}
}

// executeCheckAndRecoverFunction will choose the correct check & recovery function based on analysis.
// It executes the function synchronuously
// 根据分析结果选择正确的检查和恢复函数。它会同步执行该函数。

func executeCheckAndRecoverFunction(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	atomic.AddInt64(&countPendingRecoveries, 1)
	defer atomic.AddInt64(&countPendingRecoveries, -1)

	// 获取检查和恢复函数 。 返回值：checkAndRecoverFunction 恢复函数， isActionableRecovery 是否真正进行恢复 bool
	checkAndRecoverFunction, isActionableRecovery := getCheckAndRecoverFunction(analysisEntry.Analysis, &analysisEntry.AnalyzedInstanceKey)
	analysisEntry.IsActionableRecovery = isActionableRecovery
	runEmergentOperations(&analysisEntry)

	if checkAndRecoverFunction == nil {
		// Unhandled problem type 如果根据 故障code 没有获取到 故障类型,
		// 但该结果没有对应的操作计划，因此程序选择忽略这个特定的 analysisEntry。
		// 这样的情况可能表示程序无法找到或确定适当的操作来处理分析结果，或者分析结果本身不需要执行任何操作。
		// 如果故障类型 不是 NOProblem
		if analysisEntry.Analysis != inst.NoProblem {
			if util.ClearToLog("executeCheckAndRecoverFunction", analysisEntry.AnalyzedInstanceKey.StringCode()) {
				log.Warningf("executeCheckAndRecoverFunction: ignoring analysisEntry that has no action plan: %+v; key: %+v",
					analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey)
			}
		}

		return false, nil, nil
	}
	// we have a recovery function; its execution still depends on filters if not disabled.
	// 根据故障code 获取到了对应的故障恢复函数，但是该函数仍然受到返回的 isActionableRecovery （是否真正的执行恢复）的影响，有些故障函数不用真正的执行
	if isActionableRecovery || util.ClearToLog("executeCheckAndRecoverFunction: detection", analysisEntry.AnalyzedInstanceKey.StringCode()) {
		log.Infof("executeCheckAndRecoverFunction: proceeding with %+v detection on %+v; isActionable?: %+v; skipProcesses: %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey, isActionableRecovery, skipProcesses)
	}

	// At this point we have validated there's a failure scenario for which we have a recovery path.
	// 代码执行到这一点时，已经确认存在一个故障场景，并且已经有了对应的恢复路径。

	if orcraft.IsRaftEnabled() {
		// with raft, all nodes can (and should) run analysis, 如果开启了Raft协议，所有的节点都可以执行分析
		// but only the leader proceeds to execute detection hooks and then to failover. 但是只有Leader角色可以执行 钩子脚本 和 故障恢复。
		if !orcraft.IsLeader() {
			log.Infof("CheckAndRecover: Analysis: %+v, InstanceKey: %+v, candidateInstanceKey: %+v, "+
				"skipProcesses: %v: NOT detecting/recovering host (raft non-leader)",
				analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey, candidateInstanceKey, skipProcesses)
			return false, nil, err
		}
	}

	// Initiate detection: 将启动检测操作
	registrationSuccess, _, err := checkAndExecuteFailureDetectionProcesses(analysisEntry, skipProcesses)
	// 如果注册成功（往数据库中插入记录成功）
	if registrationSuccess {
		// 如果开启了raft
		if orcraft.IsRaftEnabled() {
			_, err := orcraft.PublishCommand("register-failure-detection", analysisEntry)
			log.Errore(err)
		}
	}
	if err != nil {
		log.Errorf("executeCheckAndRecoverFunction: error on failure detection: %+v", err)
		return false, nil, err
	}
	// We don't mind whether detection really executed the processes or not
	// (it may have been silenced due to previous detection). We only care there's no error. 这说明不关心检测是否真正执行了过程（可能由于之前的检测而被抑制）。关注的是是否有错误产生。

	// We're about to embark on recovery shortly... 即将开始执行恢复操作。

	// Check for recovery being disabled globally 检查全局禁用恢复是否开启 。
	if recoveryDisabledGlobally, err := IsRecoveryDisabled(); err != nil {
		// Unexpected. Shouldn't get this 表示某一情况发生时是意外的，不应该出现这种情况
		log.Errorf("Unable to determine if recovery is disabled globally: %v", err)
		// 如果为1  则表示 全局禁用恢复开关 已经开启
	} else if recoveryDisabledGlobally {
		// forceInstanceRecovery 传入的true
		if !forceInstanceRecovery {
			log.Infof("CheckAndRecover: Analysis: %+v, InstanceKey: %+v, candidateInstanceKey: %+v, "+
				"skipProcesses: %v: NOT Recovering host (disabled globally)",
				analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey, candidateInstanceKey, skipProcesses)

			return false, nil, err
		}
		log.Infof("CheckAndRecover: Analysis: %+v, InstanceKey: %+v, candidateInstanceKey: %+v, "+
			"skipProcesses: %v: recoveries disabled globally but forcing this recovery",
			analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey, candidateInstanceKey, skipProcesses)
	}

	// Actually attempt recovery: 接下来的代码将实际尝试执行恢复操作。
	// isActionableRecovery 是根据 故障修复函数 决定，有些故障需要真正的去执行修复函数，有些故障不需要。
	if isActionableRecovery || util.ClearToLog("executeCheckAndRecoverFunction: recovery", analysisEntry.AnalyzedInstanceKey.StringCode()) {
		log.Infof("executeCheckAndRecoverFunction: proceeding with %+v recovery on %+v; isRecoverable?: %+v; skipProcesses: %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey, isActionableRecovery, skipProcesses)
	}
	// 执行恢复的函数 checkAndRecoverFunction 该变量是一个函数 ，上面返回的
	recoveryAttempted, topologyRecovery, err = checkAndRecoverFunction(analysisEntry, candidateInstanceKey, forceInstanceRecovery, skipProcesses)
	if !recoveryAttempted {
		return recoveryAttempted, topologyRecovery, err
	}
	if topologyRecovery == nil {
		return recoveryAttempted, topologyRecovery, err
	}
	if b, err := json.Marshal(topologyRecovery); err == nil {
		log.Infof("Topology recovery: %+v", string(b))
	} else {
		log.Infof("Topology recovery: %+v", *topologyRecovery)
	}
	if !skipProcesses {
		if topologyRecovery.SuccessorKey == nil {
			// Execute general unsuccessful post failover processes
			executeProcesses(config.Config.PostUnsuccessfulFailoverProcesses, "PostUnsuccessfulFailoverProcesses", topologyRecovery, false)
		} else {
			// Execute general post failover processes
			inst.EndDowntime(topologyRecovery.SuccessorKey)
			executeProcesses(config.Config.PostFailoverProcesses, "PostFailoverProcesses", topologyRecovery, false)
		}
	}
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Waiting for %d postponed functions", topologyRecovery.PostponedFunctionsContainer.Len()))
	topologyRecovery.Wait()
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Executed %d postponed functions", topologyRecovery.PostponedFunctionsContainer.Len()))
	if topologyRecovery.PostponedFunctionsContainer.Len() > 0 {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Executed postponed functions: %+v", strings.Join(topologyRecovery.PostponedFunctionsContainer.Descriptions(), ", ")))
	}
	return recoveryAttempted, topologyRecovery, err
}

// CheckAndRecover is the main entry point for the recovery mechanism 恢复的入口函数
func CheckAndRecover(specificInstance *inst.InstanceKey, candidateInstanceKey *inst.InstanceKey, skipProcesses bool) (recoveryAttempted bool, promotedReplicaKey *inst.InstanceKey, err error) {
	// Allow the analysis to run even if we don't want to recover 即使我们不想恢复也允许分析运行
	replicationAnalysis, err := inst.GetReplicationAnalysis("", &inst.ReplicationAnalysisHints{IncludeDowntimed: true, AuditAnalysis: true})
	if err != nil {
		return false, nil, log.Errore(err)
	}
	// 如果提供了 --noop 标志，不执行任何进程
	if *config.RuntimeCLIFlags.Noop {
		log.Infof("--noop provided; will not execute processes")
		skipProcesses = true
	}
	// intentionally iterating entries in random order
	//  故意以随机顺序迭代分析条目
	for _, j := range rand.Perm(len(replicationAnalysis)) {
		analysisEntry := replicationAnalysis[j]
		if specificInstance != nil {
			// We are looking for a specific instance; if this is not the one, skip!
			// 如果指定了特定实例，但当前不是该实例，则跳过
			if !specificInstance.Equals(&analysisEntry.AnalyzedInstanceKey) {
				continue
			}
		}
		if analysisEntry.SkippableDueToDowntime && specificInstance == nil {
			// Only recover a downtimed server if explicitly requested
			// 如果由于下线而可以跳过，并且未指定特定实例，则跳过
			continue
		}

		if specificInstance != nil {
			// force mode. Keep it synchronuous
			// 强制模式。保持同步执行
			var topologyRecovery *TopologyRecovery
			recoveryAttempted, topologyRecovery, err = executeCheckAndRecoverFunction(analysisEntry, candidateInstanceKey, true, skipProcesses)
			log.Errore(err)
			if topologyRecovery != nil {
				promotedReplicaKey = topologyRecovery.SuccessorKey
			}
		} else {
			// 异步执行检查和恢复函数
			go func() {
				_, _, err := executeCheckAndRecoverFunction(analysisEntry, candidateInstanceKey, false, skipProcesses)
				log.Errore(err)
			}()
		}
	}
	return recoveryAttempted, promotedReplicaKey, err
}

func forceAnalysisEntry(clusterName string, analysisCode inst.AnalysisCode, commandHint string, failedInstanceKey *inst.InstanceKey) (analysisEntry inst.ReplicationAnalysis, err error) {
	clusterInfo, err := inst.ReadClusterInfo(clusterName)
	if err != nil {
		return analysisEntry, err
	}

	clusterAnalysisEntries, err := inst.GetReplicationAnalysis(clusterInfo.ClusterName, &inst.ReplicationAnalysisHints{IncludeDowntimed: true, IncludeNoProblem: true})
	if err != nil {
		return analysisEntry, err
	}

	for _, entry := range clusterAnalysisEntries {
		if entry.AnalyzedInstanceKey.Equals(failedInstanceKey) {
			analysisEntry = entry
		}
	}
	analysisEntry.Analysis = analysisCode // we force this analysis
	analysisEntry.CommandHint = commandHint
	analysisEntry.ClusterDetails = *clusterInfo
	analysisEntry.AnalyzedInstanceKey = *failedInstanceKey

	return analysisEntry, nil
}

// ForceExecuteRecovery can be called to issue a recovery process even if analysis says there is no recovery case. 即使通过拓扑分析 该集群没有故障类型，也可以注入这一种故障类型，然后去执行
// The caller of this function injects the type of analysis it wishes the function to assume.
// By calling this function one takes responsibility for one's actions.
func ForceExecuteRecovery(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	return executeCheckAndRecoverFunction(analysisEntry, candidateInstanceKey, true, skipProcesses)
}

// ForceMasterFailover *trusts* master of given cluster is dead and initiates a failover
func ForceMasterFailover(clusterName string) (topologyRecovery *TopologyRecovery, err error) {
	clusterMasters, err := inst.ReadClusterMaster(clusterName)
	if err != nil {
		return nil, fmt.Errorf("Cannot deduce cluster master for %+v", clusterName)
	}
	if len(clusterMasters) != 1 {
		return nil, fmt.Errorf("Cannot deduce cluster master for %+v", clusterName)
	}
	clusterMaster := clusterMasters[0]

	analysisEntry, err := forceAnalysisEntry(clusterName, inst.DeadMaster, inst.ForceMasterFailoverCommandHint, &clusterMaster.Key)
	if err != nil {
		return nil, err
	}
	recoveryAttempted, topologyRecovery, err := ForceExecuteRecovery(analysisEntry, nil, false)
	if err != nil {
		return nil, err
	}
	if !recoveryAttempted {
		return nil, fmt.Errorf("Unexpected error: recovery not attempted. This should not happen")
	}
	if topologyRecovery == nil {
		return nil, fmt.Errorf("Recovery attempted but with no results. This should not happen")
	}
	if topologyRecovery.SuccessorKey == nil {
		return nil, fmt.Errorf("Recovery attempted yet no replica promoted")
	}
	return topologyRecovery, nil
}

// ForceMasterTakeover *trusts* master of given cluster is dead and fails over to designated instance,
// which has to be its direct child.
func ForceMasterTakeover(clusterName string, destination *inst.Instance) (topologyRecovery *TopologyRecovery, err error) {
	clusterMasters, err := inst.ReadClusterWriteableMaster(clusterName)
	if err != nil {
		return nil, fmt.Errorf("Cannot deduce cluster master for %+v", clusterName)
	}
	if len(clusterMasters) != 1 {
		return nil, fmt.Errorf("Cannot deduce cluster master for %+v", clusterName)
	}
	clusterMaster := clusterMasters[0]

	if !destination.MasterKey.Equals(&clusterMaster.Key) {
		return nil, fmt.Errorf("You may only promote a direct child of the master %+v. The master of %+v is %+v.", clusterMaster.Key, destination.Key, destination.MasterKey)
	}
	log.Infof("Will demote %+v and promote %+v instead", clusterMaster.Key, destination.Key)

	analysisEntry, err := forceAnalysisEntry(clusterName, inst.DeadMaster, inst.ForceMasterTakeoverCommandHint, &clusterMaster.Key)
	if err != nil {
		return nil, err
	}
	recoveryAttempted, topologyRecovery, err := ForceExecuteRecovery(analysisEntry, &destination.Key, false)
	if err != nil {
		return nil, err
	}
	if !recoveryAttempted {
		return nil, fmt.Errorf("Unexpected error: recovery not attempted. This should not happen")
	}
	if topologyRecovery == nil {
		return nil, fmt.Errorf("Recovery attempted but with no results. This should not happen")
	}
	if topologyRecovery.SuccessorKey == nil {
		return nil, fmt.Errorf("Recovery attempted yet no replica promoted")
	}
	return topologyRecovery, nil
}

func getGracefulMasterTakeoverDesignatedInstance(clusterMasterKey *inst.InstanceKey, designatedKey *inst.InstanceKey, clusterMasterDirectReplicas [](*inst.Instance), auto bool) (designatedInstance *inst.Instance, err error) {
	if designatedKey == nil {
		// User did not specify a replica to promote // 用户没有提供用于提升的从副本
		if len(clusterMasterDirectReplicas) == 1 {
			// Single replica. That's the one we'll promote // 只有一个从副本
			return clusterMasterDirectReplicas[0], nil
		}
		// More than one replica.//多于一个从副本
		if !auto {
			return nil, fmt.Errorf("GracefulMasterTakeover: target instance not indicated, auto=false, and master %+v has %+v replicas. orchestrator cannot choose where to failover to. Aborting", *clusterMasterKey, len(clusterMasterDirectReplicas))
		}
		log.Debugf("GracefulMasterTakeover: request takeover for master %+v, no designated replica indicated. orchestrator will attempt to auto deduce replica.", *clusterMasterKey)
		designatedInstance, _, _, _, _, err = inst.GetCandidateReplica(clusterMasterKey, false)
		if err != nil || designatedInstance == nil {
			return nil, fmt.Errorf("GracefulMasterTakeover: no target instance indicated, failed to auto-detect candidate replica for master %+v. Aborting", *clusterMasterKey)
		}
		log.Debugf("GracefulMasterTakeover: candidateReplica=%+v", designatedInstance.Key)
		if _, err := inst.StartReplication(&designatedInstance.Key); err != nil {  // 启动新主的复制 流程参考 DeadMaster 选主的流程
			return nil, fmt.Errorf("GracefulMasterTakeover:cannot start replication on designated replica %+v. Aborting", designatedKey)
		}
		log.Infof("GracefulMasterTakeover: designated master deduced to be %+v", designatedInstance.Key)
		return designatedInstance, nil
	}

	// Verify designated instance is a direct replica of master 遍历clusterMasterDirectReplicas ，如果该从副本是老主的直接从副本，则作为新主
	for _, directReplica := range clusterMasterDirectReplicas {
		if directReplica.Key.Equals(designatedKey) {
			designatedInstance = directReplica
		}
	}
	if designatedInstance == nil {
		return nil, fmt.Errorf("GracefulMasterTakeover: indicated designated instance %+v must be directly replicating from the master %+v", *designatedKey, *clusterMasterKey)
	}
	log.Infof("GracefulMasterTakeover: designated master instructed to be %+v", designatedInstance.Key)
	return designatedInstance, nil
}

// GracefulMasterTakeover will demote master of existing topology and promote its
// direct replica instead.
// It expects that replica to have no siblings.
// This function is graceful in that it will first lock down the master, then wait
// for the designated replica to catch up with last position.
// It will point old master at the newly promoted master at the correct coordinates, but will not start replication.
// GracefulMasterTakeover  函数会降级现有拓扑中的主服务器。 它将会晋升主服务器的直接副本（replica）为新的主服务器。
// 先锁定主服务器： 该函数首先会锁定当前的主服务器，以确保在进行主从切换的过程中不会有其他操作。
// 等待指定的副本追赶最后的位置： 接着，它会等待指定的副本赶上当前主服务器的最后位置，以确保数据同步。
// 设置旧主服务器的指向： 一旦副本追赶上最后的位置，函数会将旧主服务器指向新主服务器的正确位置，但不会启动复制过程。
func GracefulMasterTakeover(clusterName string, designatedKey *inst.InstanceKey, auto bool) (topologyRecovery *TopologyRecovery, promotedMasterCoordinates *inst.BinlogCoordinates, err error) {
	clusterMasters, err := inst.ReadClusterMaster(clusterName) // 读取出来的是集群的主库
	if err != nil {
		return nil, nil, fmt.Errorf("Cannot deduce cluster master for %+v; error: %+v", clusterName, err)
	}
	if len(clusterMasters) != 1 {
		return nil, nil, fmt.Errorf("Cannot deduce cluster master for %+v. Found %+v potential masters", clusterName, len(clusterMasters))
	}
	clusterMaster := clusterMasters[0]

	clusterMasterDirectReplicas, err := inst.ReadReplicaInstances(&clusterMaster.Key)  // 根据主查询得到所有从副本
	if err != nil {
		return nil, nil, log.Errore(err)
	}

	if len(clusterMasterDirectReplicas) == 0 {
		return nil, nil, fmt.Errorf("Master %+v doesn't seem to have replicas", clusterMaster.Key)
	}

	if designatedKey != nil && !designatedKey.IsValid() {
		// An empty or invalid key is as good as no key 指定key为 空 或者 无效 等同于 没有
		designatedKey = nil
	}
	designatedInstance, err := getGracefulMasterTakeoverDesignatedInstance(&clusterMaster.Key, designatedKey, clusterMasterDirectReplicas, auto)  //  获取指定的新主
	if err != nil {
		return nil, nil, log.Errore(err)
	}

	if inst.IsBannedFromBeingCandidateReplica(designatedInstance) {
		return nil, nil, fmt.Errorf("GracefulMasterTakeover: designated instance %+v cannot be promoted due to promotion rule or it is explicitly ignored in PromotionIgnoreHostnameFilters configuration", designatedInstance.Key)
	}

	masterOfDesignatedInstance, err := inst.GetInstanceMaster(designatedInstance)  // 获取新主的实例信息
	if err != nil {
		return nil, nil, err
	}
	if !masterOfDesignatedInstance.Key.Equals(&clusterMaster.Key) { //  检查新主的主库 是否 和该集群的主库相同
		return nil, nil, fmt.Errorf("Sanity check failure. It seems like the designated instance %+v does not replicate from the master %+v (designated instance's master key is %+v). This error is strange. Panicking", designatedInstance.Key, clusterMaster.Key, designatedInstance.MasterKey)
	}
	if !designatedInstance.HasReasonableMaintenanceReplicationLag() {  // 检查新主延迟是否过大
		return nil, nil, fmt.Errorf("Desginated instance %+v seems to be lagging too much for this operation. Aborting.", designatedInstance.Key)
	}

	if len(clusterMasterDirectReplicas) > 1 { // 如果老主的直接复制从副本多于1个
		log.Infof("GracefulMasterTakeover: Will let %+v take over its siblings", designatedInstance.Key)
		relocatedReplicas, _, err, _ := inst.RelocateReplicas(&clusterMaster.Key, &designatedInstance.Key, "")
		if len(relocatedReplicas) != len(clusterMasterDirectReplicas)-1 {
			// We are unable to make designated instance master of all its siblings 并没有将老主的所有从副本 移动 到新主下
			relocatedReplicasKeyMap := inst.NewInstanceKeyMap()
			relocatedReplicasKeyMap.AddInstances(relocatedReplicas)
			// Let's see which replicas have not been relocated
			for _, directReplica := range clusterMasterDirectReplicas {
				if relocatedReplicasKeyMap.HasKey(directReplica.Key) {
					// relocated, good
					continue
				}
				if directReplica.Key.Equals(&designatedInstance.Key) {
					// obviously we skip this one
					continue
				}
				if directReplica.IsDowntimed {
					// obviously we skip this one // 设置了 Downtime 维护
					log.Warningf("GracefulMasterTakeover: unable to relocate %+v below designated %+v, but since it is downtimed (downtime reason: %s) I will proceed", directReplica.Key, designatedInstance.Key, directReplica.DowntimeReason)
					continue
				}
				return nil, nil, fmt.Errorf("Desginated instance %+v cannot take over all of its siblings. Error: %+v", designatedInstance.Key, err)
			}
		}
	}
	log.Infof("GracefulMasterTakeover: Will demote %+v and promote %+v instead", clusterMaster.Key, designatedInstance.Key)

	replicationCreds, replicationCredentialsError := inst.ReadReplicationCredentials(&designatedInstance.Key) // mysql.slave_master_info 获取复制用的账号密码

	analysisEntry, err := forceAnalysisEntry(clusterName, inst.DeadMaster, inst.GracefulMasterTakeoverCommandHint, &clusterMaster.Key)
	if err != nil {
		return nil, nil, err
	}
	preGracefulTakeoverTopologyRecovery := &TopologyRecovery{
		SuccessorKey:  &designatedInstance.Key,
		AnalysisEntry: analysisEntry,
	}
	if err := executeProcesses(config.Config.PreGracefulTakeoverProcesses, "PreGracefulTakeoverProcesses", preGracefulTakeoverTopologyRecovery, true); err != nil {
		return nil, nil, fmt.Errorf("Failed running PreGracefulTakeoverProcesses: %+v", err)
	}

	log.Infof("GracefulMasterTakeover: Will set %+v as read_only", clusterMaster.Key) // 设置老库只读
	if clusterMaster, err = inst.SetReadOnly(&clusterMaster.Key, true); err != nil {
		return nil, nil, err
	}
	demotedMasterSelfBinlogCoordinates := &clusterMaster.SelfBinlogCoordinates // 老主的binlog坐标
	log.Infof("GracefulMasterTakeover: Will wait for %+v to reach master coordinates %+v", designatedInstance.Key, *demotedMasterSelfBinlogCoordinates)
	// 是个for 循环 会一直等待 ，新主的执行位点 与 老主的binlog位点比较 ，没有追赶上会等待500ms
	if designatedInstance, _, err = inst.WaitForExecBinlogCoordinatesToReach(&designatedInstance.Key, demotedMasterSelfBinlogCoordinates, time.Duration(config.Config.ReasonableMaintenanceReplicationLagSeconds)*time.Second); err != nil {
		return nil, nil, err
	}
	promotedMasterCoordinates = &designatedInstance.SelfBinlogCoordinates // 指定的新主的binlog坐标

	log.Infof("GracefulMasterTakeover: attempting recovery")
		recoveryAttempted, topologyRecovery, err := ForceExecuteRecovery(analysisEntry, &designatedInstance.Key, false)
	if err != nil {
		log.Errorf("GracefulMasterTakeover: noting an error, and for now proceeding: %+v", err)
	}
	if !recoveryAttempted {
		return nil, nil, fmt.Errorf("GracefulMasterTakeover: unexpected error: recovery not attempted. This should not happen")
	}
	if topologyRecovery == nil {
		return nil, nil, fmt.Errorf("GracefulMasterTakeover: recovery attempted but with no results. This should not happen")
	}
	if topologyRecovery.SuccessorKey == nil {
		// Promotion fails. 新主提升失败
		// Undo setting read-only on original master. 在老主上关闭只读
		inst.SetReadOnly(&clusterMaster.Key, false)
		return nil, nil, fmt.Errorf("GracefulMasterTakeover: Recovery attempted yet no replica promoted; err=%+v", err)
	}
	var gtidHint inst.OperationGTIDHint = inst.GTIDHintNeutral
	if topologyRecovery.RecoveryType == MasterRecoveryGTID {
		gtidHint = inst.GTIDHintForce
	}
	clusterMaster, err = inst.ChangeMasterTo(&clusterMaster.Key, &designatedInstance.Key, promotedMasterCoordinates, false, gtidHint)
	if !clusterMaster.SelfBinlogCoordinates.Equals(demotedMasterSelfBinlogCoordinates) {
		log.Errorf("GracefulMasterTakeover: sanity problem. Demoted master's coordinates changed from %+v to %+v while supposed to have been frozen", *demotedMasterSelfBinlogCoordinates, clusterMaster.SelfBinlogCoordinates)
	}
	if !clusterMaster.HasReplicationCredentials && replicationCredentialsError == nil {
		_, credentialsErr := inst.ChangeMasterCredentials(&clusterMaster.Key, replicationCreds)
		if err == nil {
			err = credentialsErr
		}
	}
	if designatedInstance.AllowTLS {
		_, enableSSLErr := inst.EnableMasterSSL(&clusterMaster.Key)
		if err == nil {
			err = enableSSLErr
		}
	}
	if auto {
		_, startReplicationErr := inst.StartReplication(&clusterMaster.Key)
		if err == nil {
			err = startReplicationErr
		}
	}
	executeProcesses(config.Config.PostGracefulTakeoverProcesses, "PostGracefulTakeoverProcesses", topologyRecovery, false)

	return topologyRecovery, promotedMasterCoordinates, err
}
