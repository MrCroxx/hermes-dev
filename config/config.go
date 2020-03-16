package config

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

// Hermes configuration struct

type HermesConfig struct {
	PodID      uint64            `yaml:"PodID"`      // local pod id
	Pods       map[uint64]string `yaml:"Pods"`       // pod id -> url
	StorageDir string            `yaml:"StorageDir"` // local storage path
	Meta       Meta              `yaml:"Meta"`
}

type Meta struct {
	TriggerSnapshotEntriesN uint64 `yaml:"TriggerSnapshotEntriesN"` // entries count when trigger snapshot
	SnapshotCatchUpEntriesN uint64 `yaml:"SnapshotCatchUpEntriesN"` // entries count for slow followers to catch up before compacting
}

func ParseHermesConfigFromFile(path string) (cfg *HermesConfig, err error) {
	var f []byte
	if f, err = ioutil.ReadFile(path); err != nil {
		return nil, err
	} else if err = yaml.Unmarshal(f, &cfg); err != nil {
		return nil, err
	}
	return
}

// TODO : 重构
type ConfChangeContext struct {
	ZoneID uint64
	PodID  uint64
	NodeID uint64
	URL    string
}
