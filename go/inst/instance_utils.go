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
	"net"
	"regexp"
	"strconv"
	"strings"
)

var (
	DowntimeLostInRecoveryMessage = "lost-in-recovery"
)

// majorVersionsSortedByCount sorts (major) versions:
// - primary sort: by count appearances
// - secondary sort: by version
type majorVersionsSortedByCount struct {
	versionsCount map[string]int
	versions      []string
}

func NewMajorVersionsSortedByCount(versionsCount map[string]int) *majorVersionsSortedByCount {
	versions := []string{}
	for v := range versionsCount {
		versions = append(versions, v)
	}
	return &majorVersionsSortedByCount{
		versionsCount: versionsCount,
		versions:      versions,
	}
}

func (this *majorVersionsSortedByCount) Len() int { return len(this.versions) }
func (this *majorVersionsSortedByCount) Swap(i, j int) {
	this.versions[i], this.versions[j] = this.versions[j], this.versions[i]
}
func (this *majorVersionsSortedByCount) Less(i, j int) bool {
	if this.versionsCount[this.versions[i]] == this.versionsCount[this.versions[j]] {
		return this.versions[i] > this.versions[j]
	}
	return this.versionsCount[this.versions[i]] < this.versionsCount[this.versions[j]]
}
func (this *majorVersionsSortedByCount) First() string {
	return this.versions[0]
}

// majorVersionsSortedByCount sorts (major) versions:
// - primary sort: by count appearances
// - secondary sort: by version
type binlogFormatSortedByCount struct {
	formatsCount map[string]int
	formats      []string
}

func NewBinlogFormatSortedByCount(formatsCount map[string]int) *binlogFormatSortedByCount {
	formats := []string{}
	for v := range formatsCount {
		formats = append(formats, v)
	}
	return &binlogFormatSortedByCount{
		formatsCount: formatsCount,
		formats:      formats,
	}
}

func (this *binlogFormatSortedByCount) Len() int { return len(this.formats) }
func (this *binlogFormatSortedByCount) Swap(i, j int) {
	this.formats[i], this.formats[j] = this.formats[j], this.formats[i]
}
func (this *binlogFormatSortedByCount) Less(i, j int) bool {
	if this.formatsCount[this.formats[i]] == this.formatsCount[this.formats[j]] {
		return IsSmallerBinlogFormat(this.formats[j], this.formats[i])
	}
	return this.formatsCount[this.formats[i]] < this.formatsCount[this.formats[j]]
}
func (this *binlogFormatSortedByCount) First() string {
	return this.formats[0]
}

// InstancesSorterByExec sorts instances by executed binlog coordinates
// InstancesSorterByExec 按照已执行的 binlog 坐标对实例进行排序的辅助函数
type InstancesSorterByExec struct {
	instances  [](*Instance)
	dataCenter string
}

func NewInstancesSorterByExec(instances [](*Instance), dataCenter string) *InstancesSorterByExec {
	return &InstancesSorterByExec{
		instances:  instances,
		dataCenter: dataCenter,
	}
}

func (this *InstancesSorterByExec) Len() int { return len(this.instances) }
func (this *InstancesSorterByExec) Swap(i, j int) {
	this.instances[i], this.instances[j] = this.instances[j], this.instances[i]
}
func (this *InstancesSorterByExec) Less(i, j int) bool {
	// Returning "true" in this function means [i] is "smaller" than [j],
	// which will lead to [j] be a better candidate for promotion

	// Sh*t happens. We just might get nil while attempting to discover/recover
	if this.instances[i] == nil {
		return false
	}
	if this.instances[j] == nil {
		return true
	}
	if this.instances[i].ExecBinlogCoordinates.Equals(&this.instances[j].ExecBinlogCoordinates) {
		// Secondary sorting: "smaller" if not logging replica updates 次要排序：如果不记录副本更新，则 "小"
		if this.instances[j].LogReplicationUpdatesEnabled && !this.instances[i].LogReplicationUpdatesEnabled {
			return true
		}
		// Next sorting: "smaller" if of higher version (this will be reversed eventually)
		// Idea is that given 5.6 a& 5.7 both of the exact position, we will want to promote
		// the 5.6 on top of 5.7, as the other way around is invalid
		// 下一个排序：如果版本更高，则 "小"（最终将被颠倒）
		// 思路是，在给定精确位置的 5.6 和 5.7，我们将希望在 5.7 之上提升 5.6，因为反过来是无效的
		if this.instances[j].IsSmallerMajorVersion(this.instances[i]) {
			return true
		}
		// Next sorting: "smaller" if of larger binlog-format (this will be reversed eventually)
		// Idea is that given ROW & STATEMENT both of the exact position, we will want to promote
		// the STATEMENT on top of ROW, as the other way around is invalid
		// 下一个排序：如果 binlog 格式更大，则 "小"（最终将被颠倒）
		// 思路是，在给定精确位置的 ROW 和 STATEMENT，我们将希望在 STATEMENT 之上提升 ROW，因为反过来是无效的
		if this.instances[j].IsSmallerBinlogFormat(this.instances[i]) {
			return true
		}
		// Prefer local datacenter: 优先本地数据中心：
		if this.instances[j].DataCenter == this.dataCenter && this.instances[i].DataCenter != this.dataCenter {
			return true
		}
		// Prefer if not having errant GTID 优先没有错误的 GTID
		if this.instances[j].GtidErrant == "" && this.instances[i].GtidErrant != "" {
			return true
		}
		// Prefer candidates: 比较提升优先级
		if this.instances[j].PromotionRule.BetterThan(this.instances[i].PromotionRule) {
			return true
		}
	}
	return this.instances[i].ExecBinlogCoordinates.SmallerThan(&this.instances[j].ExecBinlogCoordinates)
}

// filterInstancesByPattern will filter given array of instances according to regular expression pattern
func filterInstancesByPattern(instances [](*Instance), pattern string) [](*Instance) {
	if pattern == "" {
		return instances
	}
	filtered := [](*Instance){}
	for _, instance := range instances {
		if matched, _ := regexp.MatchString(pattern, instance.Key.DisplayString()); matched {
			filtered = append(filtered, instance)
		}
	}
	return filtered
}

// RemoveInstance will remove an instance from a list of instances 其目的是从给定的实例列表中移除具有特定键（instanceKey）的实例。
func RemoveInstance(instances [](*Instance), instanceKey *InstanceKey) [](*Instance) {
	if instanceKey == nil {
		return instances
	}
	for i := len(instances) - 1; i >= 0; i-- {
		if instances[i].Key.Equals(instanceKey) {
			instances = append(instances[:i], instances[i+1:]...)
		}
	}
	return instances
}

// removeBinlogServerInstances will remove all binlog servers from given lsit
func RemoveBinlogServerInstances(instances [](*Instance)) [](*Instance) {
	for i := len(instances) - 1; i >= 0; i-- {
		if instances[i].IsBinlogServer() {
			instances = append(instances[:i], instances[i+1:]...)
		}
	}
	return instances
}

// RemoveNilInstances 其功能是从给定的实例列表中移除所有值为 nil 的实例。
func RemoveNilInstances(instances [](*Instance)) [](*Instance) {
	for i := len(instances) - 1; i >= 0; i-- {
		if instances[i] == nil {
			instances = append(instances[:i], instances[i+1:]...)
		}
	}
	return instances
}

// SemicolonTerminated is a utility function that makes sure a statement is terminated with
// a semicolon, if it isn't already
func SemicolonTerminated(statement string) string {
	statement = strings.TrimSpace(statement)
	statement = strings.TrimRight(statement, ";")
	statement = statement + ";"
	return statement
}

// MajorVersion returns a MySQL major version number (e.g. given "5.5.36" it returns "5.5")
func MajorVersion(version string) []string {
	tokens := strings.Split(version, ".")
	if len(tokens) < 2 {
		return []string{"0", "0"}
	}
	return tokens[:2]
}

// IsSmallerMajorVersion tests two versions against another and returns true if
// the former is a smaller "major" varsion than the latter. 比较两个版本号，并在前者的 "major" 部分小于后者时返回 true。
// e.g. 5.5.36 is NOT a smaller major version as comapred to 5.5.40, but IS as compared to 5.6.9
// 5.5.36的主版本号不比 5.5.40小 ,但是和 5.6.9 版本比 是小的
func IsSmallerMajorVersion(version string, otherVersion string) bool {
	thisMajorVersion := MajorVersion(version)
	otherMajorVersion := MajorVersion(otherVersion)
	for i := 0; i < len(thisMajorVersion); i++ {
		thisToken, _ := strconv.Atoi(thisMajorVersion[i])
		otherToken, _ := strconv.Atoi(otherMajorVersion[i])
		if thisToken < otherToken {
			return true
		}
		if thisToken > otherToken {
			return false
		}
	}
	return false
}

// IsSmallerBinlogFormat tests two binlog formats and sees if one is "smaller" than the other.
// "smaller" binlog format means you can replicate from the smaller to the larger.
// 用于比较两个 binlog 格式，判断一个是否"小于"另一个。
// 如果 binlogFormat 为 "STATEMENT"，则它被认为是"小于"另一个 binlog 格式，当且仅当 otherBinlogFormat 是 "ROW" 或 "MIXED" 时。
// 如果 binlogFormat 为 "MIXED"，则它被认为是"小于"另一个 binlog 格式，当且仅当 otherBinlogFormat 是 "ROW" 时。
// 在其他情况下，返回 false。
func IsSmallerBinlogFormat(binlogFormat string, otherBinlogFormat string) bool {
	if binlogFormat == "STATEMENT" {
		return (otherBinlogFormat == "ROW" || otherBinlogFormat == "MIXED")
	}
	if binlogFormat == "MIXED" {
		return otherBinlogFormat == "ROW"
	}
	return false
}

// FiltersMatchInstanceKey returns true if given instance key matches any one of given filters.
// A filter could be:
// - An IP address, in which case we compare exact value
// - Any other string, in which case we compare via regular expression
// FiltersMatchInstanceKey。如果给定的实例键（Instance Key）与给定的过滤器（Filters）中的任何一个匹配，该函数将返回 true。
// 在这个函数中，过滤器可以是：
// - IP 地址，此时我们会进行精确值比较。
// - 任何其他字符串，此时我们会通过正则表达式进行比较
func FiltersMatchInstanceKey(instanceKey *InstanceKey, filters []string) bool {
	for _, filter := range filters {
		switch {
		case net.ParseIP(filter) != nil:
			// If the filter is an IP address, expect complete match.
			// This is to avoid matching 10.0.0.3 with 10.0.0.38 if we
			// were to compare via regexp
			if filter == instanceKey.Hostname {
				return true
			}
		default:
			if matched, _ := regexp.MatchString(filter, instanceKey.StringCode()); matched {
				return true
			}
		}
	}
	return false
}
