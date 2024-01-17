package config

import "time"

type Config struct {
	StoreAddr     string
	Raft          bool
	SchedulerAddr string
	LogLevel      string

	DBPath string

	RaftBaseTickInterval     time.Duration
	RaftHeartbeatTicks       int
	RaftElectionTimeoutTicks int

	RaftLogGCTickInterval time.Duration
	RaftLogGcCountLimit   uint64

	SplitRegionCheckTickInterval       time.Duration
	ScheduleHeartbeatTickInterval      time.Duration
	ScheduleStoreHeartbeatTickInterval time.Duration
}
