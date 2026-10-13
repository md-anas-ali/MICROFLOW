package yta

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	maxAuditKept = 50
	maxRunsKept  = 20
)

var validChangeStatus = map[string]bool{"pending_verification": true, "verified": true, "rolled_back": true, "closed_by_repair": true}

// AuditEntry records one decision with the settings and policy in force, so a
// later reader can reconstruct what was active when (configuration snapshot).
type AuditEntry struct {
	At       time.Time `json:"at"`
	Kind     string    `json:"kind"`
	Detail   string    `json:"detail"`
	Settings string    `json:"settings"` // adopted settings at that moment
	Policy   string    `json:"policy"`   // evolve policy at that moment (no secrets)
}

// RunRecord is the health record of one analyze run (monitoring history).
type RunRecord struct {
	At         time.Time      `json:"at"`
	Status     string         `json:"status"`
	Mode       string         `json:"mode,omitempty"`
	DurationMs int64          `json:"durationMs"`
	Calls      int            `json:"apiCalls"`
	Errors     map[string]int `json:"errorClasses,omitempty"`
	HeapMB     float64        `json:"heapMB"`
	CPUMs      float64        `json:"cpuMs"` // -1 when the platform cannot report it
	Cached     bool           `json:"cached"`
}

// Snapshot renders the non-secret evolution policy.
func (c EvoConfig) Snapshot() string {
	var allow []string
	for k := range c.Allow {
		allow = append(allow, k)
	}
	sort.Strings(allow)
	mode := "recommend-only"
	if len(allow) > 0 {
		mode = "auto:" + strings.Join(allow, ",")
	}
	return fmt.Sprintf("%s minPerArm=%d evalDays=%d maxDays=%d minEffect=%.2f minDominance=%.2f",
		mode, c.minPerArm(), c.evalDays(), c.maxDays(), c.minEffect(), c.minDomin())
}

func (s *State) settingsString() string {
	var parts []string
	for k, v := range s.Settings {
		parts = append(parts, k+"="+v)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// AddAudit appends a bounded, truncated audit entry.
func (s *State) AddAudit(kind, detail string, cfg EvoConfig, now time.Time) {
	if len(detail) > 220 {
		detail = detail[:220]
	}
	s.Audit = append(s.Audit, AuditEntry{At: now, Kind: kind, Detail: detail, Settings: s.settingsString(), Policy: cfg.Snapshot()})
	if n := len(s.Audit); n > maxAuditKept {
		s.Audit = append([]AuditEntry(nil), s.Audit[n-maxAuditKept:]...)
	}
}

// AddRun appends a bounded run record.
func (s *State) AddRun(r RunRecord) {
	s.Runs = append(s.Runs, r)
	if n := len(s.Runs); n > maxRunsKept {
		s.Runs = append([]RunRecord(nil), s.Runs[n-maxRunsKept:]...)
	}
}

func (s *State) payload() []byte {
	b, _ := json.Marshal(struct {
		S map[string]string `json:"s"`
		A *Experiment       `json:"a"`
		C []Change          `json:"c"`
		B []Block           `json:"b"`
	}{s.Settings, s.Active, s.Changes, s.Blocks})
	return b
}

// Seal stores a checksum of the decision-critical fields (settings, active
// experiment, changes, blocks). It detects corruption and out-of-band edits;
// it is not a security boundary.
func (s *State) Seal() {
	h := sha256.Sum256(s.payload())
	s.Sum = hex.EncodeToString(h[:8])
}

// IntegrityOK is true when no seal exists yet (first run / legacy state) or
// the seal matches.
func (s *State) IntegrityOK() bool {
	if s.Sum == "" {
		return true
	}
	h := sha256.Sum256(s.payload())
	return s.Sum == hex.EncodeToString(h[:8])
}

// Repair removes anything that is invalid and returns what it changed. It
// never invents state: it only drops or closes unsafe entries.
func (s *State) Repair(now time.Time) []string {
	var fixed []string
	for k, v := range s.Settings {
		if !ValidVariant(k, v) {
			delete(s.Settings, k)
			fixed = append(fixed, fmt.Sprintf("dropped invalid setting %s=%q", k, v))
		}
	}
	if a := s.Active; a != nil && (!ValidVariant(a.Setting, a.Variant) || a.MinPerArm <= 0 || a.ID == "") {
		s.Active = nil
		fixed = append(fixed, "cleared invalid active experiment")
	}
	kept := s.Changes[:0]
	for _, c := range s.Changes {
		if !validChangeStatus[c.Status] || c.Setting == "" {
			fixed = append(fixed, "dropped malformed change record")
			continue
		}
		kept = append(kept, c)
	}
	s.Changes = kept
	pending := -1
	for i := range s.Changes {
		if s.Changes[i].Status == "pending_verification" {
			if pending >= 0 {
				s.Changes[pending].Status = "closed_by_repair"
				s.Changes[pending].ClosedAt = now
				fixed = append(fixed, "closed an older duplicate pending verification")
			}
			pending = i
		}
	}
	s.pruneBlocks(now)
	return fixed
}
