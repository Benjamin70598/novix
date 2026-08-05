// Package policy implements the Novix policy engine: the declared-intent
// layer that decides how each topic behaves. Policy is loaded from a YAML
// file at startup. Events are classified at write time (DECISIONS.md 006):
// the class stamped on an event is its topic's sync mode at append time.
//
// Sync modes:
//
//	all           sync every raw event, in priority order
//	immediate     like all, but an append kicks the sync loop right away
//	aggregates_D  raw events stay on site; only windowed aggregates sync.
//	              D is a Go-style duration with m/s/h units, e.g.
//	              aggregates_5m, aggregates_10s
//	none          never leaves the site
//
// Priority: lower number drains first on reconnect. 0 is reserved by
// convention for faults and safety events.
package policy

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	yaml "gopkg.in/yaml.v3"
)

// TopicPolicy is the declared behavior for one topic.
type TopicPolicy struct {
	Sync           string `yaml:"sync"`
	Priority       int    `yaml:"priority"`
	RetentionLocal string `yaml:"retention_local"`
}

// Policy is the full site policy.
type Policy struct {
	Site         string                 `yaml:"site"`
	Hub          string                 `yaml:"hub"`
	BandwidthCap string                 `yaml:"bandwidth_cap"`
	Topics       map[string]TopicPolicy `yaml:"topics"`
	Offline      struct {
		Continue bool   `yaml:"continue"`
		Buffer   string `yaml:"buffer"`
	} `yaml:"offline"`
}

// DefaultTopic is applied to topics not declared in the policy file:
// sync everything at a low priority. Undeclared data is never silently
// dropped; dropping requires an explicit sync: none.
var DefaultTopic = TopicPolicy{Sync: "all", Priority: 50}

// Load reads and validates a policy file.
func Load(path string) (*Policy, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var p Policy
	if err := yaml.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	for name, tp := range p.Topics {
		if err := validateSync(tp.Sync); err != nil {
			return nil, fmt.Errorf("topic %q: %w", name, err)
		}
	}
	if _, err := p.CapBytesPerSec(); err != nil {
		return nil, err
	}
	return &p, nil
}

func validateSync(mode string) error {
	switch {
	case mode == "" || mode == "all" || mode == "immediate" || mode == "none":
		return nil
	case strings.HasPrefix(mode, "aggregates_"):
		_, err := AggWindow(mode)
		return err
	default:
		return fmt.Errorf("unknown sync mode %q", mode)
	}
}

// Topic returns the policy for a topic, falling back to DefaultTopic.
func (p *Policy) Topic(name string) TopicPolicy {
	if tp, ok := p.Topics[name]; ok {
		if tp.Sync == "" {
			tp.Sync = "all"
		}
		return tp
	}
	return DefaultTopic
}

// Class returns the class to stamp on an event written to topic now.
// M2: class == the topic's sync mode. Content-based classification
// (anomalies_only) is where this function grows later.
func (p *Policy) Class(topic string) string {
	return p.Topic(topic).Sync
}

// AggWindow parses an aggregates_<dur> mode into its window size.
func AggWindow(mode string) (time.Duration, error) {
	s := strings.TrimPrefix(mode, "aggregates_")
	if s == mode {
		return 0, fmt.Errorf("%q is not an aggregates mode", mode)
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("bad aggregation window %q", s)
	}
	return d, nil
}

// CapBytesPerSec parses bandwidth_cap ("2mbps", "500kbps", "" = unlimited)
// into bytes per second. 0 means unlimited.
func (p *Policy) CapBytesPerSec() (int64, error) {
	s := strings.ToLower(strings.TrimSpace(p.BandwidthCap))
	if s == "" {
		return 0, nil
	}
	var mult int64
	switch {
	case strings.HasSuffix(s, "mbps"):
		mult, s = 1_000_000/8, strings.TrimSuffix(s, "mbps")
	case strings.HasSuffix(s, "kbps"):
		mult, s = 1_000/8, strings.TrimSuffix(s, "kbps")
	default:
		return 0, fmt.Errorf("bandwidth_cap %q: use mbps or kbps", p.BandwidthCap)
	}
	n, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("bandwidth_cap %q: bad number", p.BandwidthCap)
	}
	return int64(n * float64(mult)), nil
}
