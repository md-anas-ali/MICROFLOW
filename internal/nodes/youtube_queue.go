package nodes

// YouTube Publishing Queue & Schedule Normalizer.
//
// Used ONLY by the existing YouTubeExecutor (google.go) when a youTube
// "upload" node opts in with options.publishQueue=true (or the
// YT_PUBLISH_QUEUE=true environment variable). No new runner, scheduler
// or API route is introduced: the queue is a small piece of state kept in
// the workflow's existing static-data row (workflow_static_data, the
// same Postgres row/lock that $getWorkflowStaticData uses), so it
// survives restarts and is serialized by the existing SELECT ... FOR
// UPDATE row lock (engine.StaticDataStore.WithLock).
//
// One MicroFlow Service has exactly one Workflow and one connected
// YouTube channel, so "one workflow's static data" == "one channel's
// queue".
//
// Fixed slots (defaults, all overridable via Service/Global Environment):
//
//	YT_PUBLISH_TIMEZONE            Asia/Dhaka
//	YT_PUBLISH_SLOTS               07:00,22:00   (max videos/day == number of slots)
//	YT_PUBLISH_MIN_LEAD_SECONDS    120           (a slot closer than this is treated as passed)
//	YT_PUBLISH_SCAN_MAX_VIDEOS     500           (newest uploads inspected when reconciling)
//	YT_PUBLISH_MAX_MOVES_PER_RUN   25            (videos.update calls per run, 50 quota units each)
//	YT_PUBLISH_QUEUE               true|false    (force on/off regardless of the node option)

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"microflow/internal/engine"
	"microflow/internal/model"
)

const (
	pubQueueStateKey = "youtubePublishQueue"

	pubReserved  = "reserved"  // slot held for an upload that is in flight
	pubScheduled = "scheduled" // publishAt confirmed (or already compliant) on YouTube
	pubPending   = "pending"   // existing video; move to Slot planned, not yet confirmed
	pubFailed    = "failed"    // last attempt failed; the slot is NOT held
	pubSkipped   = "skipped"   // existing video deliberately left alone (reason in LastError)

	defaultPubTimezone = "Asia/Dhaka"
	defaultPubSlots    = "07:00,22:00"
)

// ---------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------

type slotHM struct{ H, M int }

type pubConfig struct {
	Loc        *time.Location
	Slots      []slotHM
	MinLead    time.Duration
	StaleAfter time.Duration // an in-flight reservation older than this is considered abandoned
	ScanMax    int
	MaxMoves   int
}

func pubEnvInt(rc *engine.RunContext, key string, def, min int) int {
	raw := strings.TrimSpace(rc.Env(key))
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < min {
		return def
	}
	return n
}

func parsePubSlots(raw string) ([]slotHM, error) {
	seen := map[slotHM]bool{}
	var slots []slotHM
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		t, err := time.Parse("15:04", part)
		if err != nil {
			return nil, fmt.Errorf("YT_PUBLISH_SLOTS entry %q is not HH:MM", part)
		}
		s := slotHM{H: t.Hour(), M: t.Minute()}
		if !seen[s] {
			seen[s] = true
			slots = append(slots, s)
		}
	}
	if len(slots) == 0 {
		return nil, errors.New("YT_PUBLISH_SLOTS has no valid HH:MM entry")
	}
	sort.Slice(slots, func(i, j int) bool {
		if slots[i].H != slots[j].H {
			return slots[i].H < slots[j].H
		}
		return slots[i].M < slots[j].M
	})
	return slots, nil
}

func loadPubConfig(rc *engine.RunContext) (pubConfig, error) {
	cfg := pubConfig{StaleAfter: 45 * time.Minute}
	tzName := strings.TrimSpace(rc.Env("YT_PUBLISH_TIMEZONE"))
	if tzName == "" {
		tzName = defaultPubTimezone
	}
	loc, err := time.LoadLocation(tzName)
	if err != nil {
		if tzName != defaultPubTimezone {
			return cfg, fmt.Errorf("YT_PUBLISH_TIMEZONE %q is not a known timezone: %w", tzName, err)
		}
		// Slim container images may ship without a tz database. Bangladesh
		// has had no DST since 2009, so a fixed UTC+6 offset is exact.
		loc = time.FixedZone(defaultPubTimezone, 6*60*60)
	}
	cfg.Loc = loc

	slotsRaw := strings.TrimSpace(rc.Env("YT_PUBLISH_SLOTS"))
	if slotsRaw == "" {
		slotsRaw = defaultPubSlots
	}
	if cfg.Slots, err = parsePubSlots(slotsRaw); err != nil {
		return cfg, err
	}
	// publishAt must be in the future when YouTube receives it.
	cfg.MinLead = time.Duration(pubEnvInt(rc, "YT_PUBLISH_MIN_LEAD_SECONDS", 120, 60)) * time.Second
	cfg.ScanMax = pubEnvInt(rc, "YT_PUBLISH_SCAN_MAX_VIDEOS", 500, 50)
	cfg.MaxMoves = pubEnvInt(rc, "YT_PUBLISH_MAX_MOVES_PER_RUN", 25, 1)
	return cfg, nil
}

// publishQueueEnabled: env YT_PUBLISH_QUEUE (true/false) wins, otherwise
// the node's options.publishQueue. Default OFF so every other workflow
// that uses a youTube node keeps its old behaviour.
func publishQueueEnabled(rc *engine.RunContext, node *model.Node) bool {
	switch strings.ToLower(strings.TrimSpace(rc.Env("YT_PUBLISH_QUEUE"))) {
	case "true":
		return true
	case "false":
		return false
	}
	opts, _ := node.Parameters["options"].(map[string]any)
	switch v := opts["publishQueue"].(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(strings.TrimSpace(v), "true")
	}
	return false
}

// ---------------------------------------------------------------------
// Slot calendar + occupancy (pure functions, no I/O)
// ---------------------------------------------------------------------

type slotCalendar struct {
	loc   *time.Location
	slots []slotHM // ascending, unique
}

// firstAtOrAfter returns the first slot instant (UTC) that is >= x.
func (c slotCalendar) firstAtOrAfter(x time.Time) time.Time {
	lx := x.In(c.loc)
	y, mo, d := lx.Date()
	for off := 0; off < 4000; off++ {
		for _, s := range c.slots {
			t := time.Date(y, mo, d+off, s.H, s.M, 0, 0, c.loc)
			if !t.Before(x) {
				return t.UTC()
			}
		}
	}
	return time.Time{}
}

func (c slotCalendar) isSlot(t time.Time) bool {
	lt := t.In(c.loc)
	if lt.Second() != 0 || lt.Nanosecond() != 0 {
		return false
	}
	for _, s := range c.slots {
		if lt.Hour() == s.H && lt.Minute() == s.M {
			return true
		}
	}
	return false
}

func (c slotCalendar) dayKey(t time.Time) string { return t.In(c.loc).Format("2006-01-02") }

// slotOccupancy tracks which slot instants are taken and how many videos
// are already going public on each local calendar day. A slot is free
// only if it is on the grid, unused, AND its day still has capacity --
// this is what enforces "max 1 per slot, max len(slots) per day".
type slotOccupancy struct {
	cal    slotCalendar
	used   map[int64]bool
	perDay map[string]int
}

func newSlotOccupancy(cal slotCalendar) *slotOccupancy {
	return &slotOccupancy{cal: cal, used: map[int64]bool{}, perDay: map[string]int{}}
}

func (o *slotOccupancy) take(t time.Time) {
	k := t.Unix()
	if o.used[k] {
		return
	}
	o.used[k] = true
	o.perDay[o.cal.dayKey(t)]++
}

// takeLocked records a video that will go public at t but must not be
// moved (too close to its publish time). Unlike take it ALWAYS counts
// toward the day's total, even when two such videos share one instant.
func (o *slotOccupancy) takeLocked(t time.Time) {
	o.used[t.Unix()] = true
	o.perDay[o.cal.dayKey(t)]++
}

func (o *slotOccupancy) isFree(t time.Time) bool {
	return o.cal.isSlot(t) && !o.used[t.Unix()] && o.perDay[o.cal.dayKey(t)] < len(o.cal.slots)
}

// allocate takes and returns the first free slot >= notBefore (zero Time
// if none could be found).
func (o *slotOccupancy) allocate(notBefore time.Time) time.Time {
	t := o.cal.firstAtOrAfter(notBefore)
	for i := 0; i < 8000 && !t.IsZero(); i++ {
		if o.isFree(t) {
			o.take(t)
			return t
		}
		t = o.cal.firstAtOrAfter(t.Add(time.Second))
	}
	return time.Time{}
}

// ---------------------------------------------------------------------
// Reconciliation planner for already-scheduled videos (pure)
// ---------------------------------------------------------------------

type schedVideo struct {
	ID        string
	Title     string
	PublishAt time.Time
}

type slotMove struct {
	VideoID  string
	Title    string
	From, To time.Time
}

type existingPlan struct {
	Moves []slotMove
	Kept  []schedVideo // already compliant (or too close to publish to touch)
}

// planExisting keeps the chronological order of the existing scheduled
// videos (ties broken by video ID, so the result is deterministic) and
// gives every one its own slot:
//   - a video already exactly on a free slot, after the previous one,
//     keeps it (no API call);
//   - otherwise it moves to the first free slot at/after its old time
//     (never earlier than the previous video's slot);
//   - a video publishing within MinLead is left untouched but still
//     counted against its slot/day.
func planExisting(occ *slotOccupancy, minStart time.Time, vids []schedVideo) existingPlan {
	sorted := append([]schedVideo(nil), vids...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if !sorted[i].PublishAt.Equal(sorted[j].PublishAt) {
			return sorted[i].PublishAt.Before(sorted[j].PublishAt)
		}
		return sorted[i].ID < sorted[j].ID
	})
	var plan existingPlan
	floor := minStart
	for _, v := range sorted {
		if !v.PublishAt.After(minStart) {
			occ.takeLocked(v.PublishAt)
			plan.Kept = append(plan.Kept, v)
			continue
		}
		if v.PublishAt.After(floor) && occ.isFree(v.PublishAt) {
			occ.take(v.PublishAt)
			floor = v.PublishAt
			plan.Kept = append(plan.Kept, v)
			continue
		}
		nb := occ.cal.firstAtOrAfter(v.PublishAt)
		if !nb.After(floor) {
			nb = floor.Add(time.Second)
		}
		to := occ.allocate(nb)
		if to.IsZero() {
			log.Printf("youtube publish queue: no free slot found for video %s; left unchanged", v.ID)
			plan.Kept = append(plan.Kept, v)
			continue
		}
		floor = to
		plan.Moves = append(plan.Moves, slotMove{VideoID: v.ID, Title: v.Title, From: v.PublishAt.UTC(), To: to})
	}
	return plan
}

// ---------------------------------------------------------------------
// Persistent state (lives inside workflow static data)
// ---------------------------------------------------------------------

type pubEntry struct {
	OpKey         string `json:"opKey,omitempty"` // idempotency key of the upload that created it
	VideoID       string `json:"videoId,omitempty"`
	Title         string `json:"title,omitempty"`
	Source        string `json:"source"` // "new" | "existing"
	Status        string `json:"status"`
	Slot          string `json:"slot,omitempty"`              // target publishAt, RFC3339 UTC
	PrevPublishAt string `json:"previousPublishAt,omitempty"` // for planned moves
	Seq           int64  `json:"seq"`
	Attempts      int    `json:"attempts"`
	LastError     string `json:"lastError,omitempty"`
	CreatedAt     string `json:"createdAt,omitempty"`
	UpdatedAt     string `json:"updatedAt,omitempty"`
}

type pubState struct {
	Version int        `json:"version"`
	Seq     int64      `json:"seq"`
	Entries []pubEntry `json:"entries"`
}

func fmtPubTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02T15:04:05Z")
}

func parsePubTime(s string) (time.Time, bool) {
	if strings.TrimSpace(s) == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, false
	}
	return t.UTC(), true
}

func loadPubState(data map[string]any) (pubState, error) {
	st := pubState{Version: 1}
	raw, ok := data[pubQueueStateKey]
	if !ok || raw == nil {
		return st, nil
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return st, fmt.Errorf("publish queue state unreadable: %w", err)
	}
	if err := json.Unmarshal(b, &st); err != nil {
		// Fail loudly instead of overwriting (and losing) a queue we can't read.
		return st, fmt.Errorf("publish queue state is corrupt: %w", err)
	}
	if st.Version == 0 {
		st.Version = 1
	}
	return st, nil
}

func (st *pubState) save(data map[string]any) error {
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	data[pubQueueStateKey] = m
	return nil
}

func (st *pubState) indexByOp(opKey string) int {
	for i := range st.Entries {
		if st.Entries[i].OpKey == opKey {
			return i
		}
	}
	return -1
}

// dropVideoExceptOp removes every entry for videoID except the one that
// belongs to opKey, so a video can never be tracked twice.
func (st *pubState) dropVideoExceptOp(videoID, opKey string) {
	kept := st.Entries[:0]
	for _, e := range st.Entries {
		if e.VideoID == videoID && e.OpKey != opKey {
			continue
		}
		kept = append(kept, e)
	}
	st.Entries = kept
}

// prune keeps the blob small: published/old entries and long-dead
// failures are dropped.
func (st *pubState) prune(now time.Time) {
	kept := st.Entries[:0]
	for _, e := range st.Entries {
		if t, ok := parsePubTime(e.Slot); ok && t.Before(now.Add(-72*time.Hour)) && e.Status != pubReserved {
			continue
		}
		if e.Status == pubFailed && e.VideoID == "" {
			if u, ok := parsePubTime(e.UpdatedAt); ok && u.Before(now.Add(-7*24*time.Hour)) {
				continue
			}
		}
		kept = append(kept, e)
	}
	if len(kept) > 400 {
		kept = kept[len(kept)-400:]
	}
	st.Entries = kept
}

func (st *pubState) recordPlan(plan existingPlan, nowStr string) {
	idx := map[string]int{}
	for i := range st.Entries {
		if st.Entries[i].VideoID != "" {
			idx[st.Entries[i].VideoID] = i
		}
	}
	set := func(id, title, status string, slot time.Time, prev string) {
		i, ok := idx[id]
		if !ok {
			st.Seq++
			st.Entries = append(st.Entries, pubEntry{VideoID: id, Source: "existing", Seq: st.Seq, CreatedAt: nowStr})
			i = len(st.Entries) - 1
			idx[id] = i
		}
		e := &st.Entries[i]
		e.Title = title
		e.Status = status
		e.Slot = fmtPubTime(slot)
		e.PrevPublishAt = prev
		e.LastError = ""
		e.UpdatedAt = nowStr
	}
	for _, v := range plan.Kept {
		set(v.ID, v.Title, pubScheduled, v.PublishAt, "")
	}
	for _, m := range plan.Moves {
		set(m.VideoID, m.Title, pubPending, m.To, fmtPubTime(m.From))
	}
}

type reserveInput struct {
	OpKey      string
	Title      string
	Now        time.Time
	Snapshot   []schedVideo // currently scheduled videos on the channel (valid only if SnapshotOK)
	SnapshotOK bool
}

type reserveResult struct {
	Slot        time.Time
	VideoID     string // set when this operation already uploaded (retry/resume) or was adopted
	AlreadyDone bool
	Adopted     bool
	Moves       []slotMove
	Kept        int
}

// reserve is the whole allocation decision, executed while the static
// data row is locked: idempotency check, duplicate-upload adoption,
// reconciliation plan for existing scheduled videos, and the slot for
// the new video (always a slot not used by anything else).
func (st *pubState) reserve(cfg pubConfig, in reserveInput) (reserveResult, error) {
	var res reserveResult
	cal := slotCalendar{loc: cfg.Loc, slots: cfg.Slots}
	minStart := in.Now.Add(cfg.MinLead)
	nowStr := fmtPubTime(in.Now)
	st.prune(in.Now)

	// 1. Same operation already finished its upload: nothing to allocate.
	oi := st.indexByOp(in.OpKey)
	if oi >= 0 && st.Entries[oi].VideoID != "" {
		res.VideoID = st.Entries[oi].VideoID
		res.Slot, _ = parsePubTime(st.Entries[oi].Slot)
		res.AlreadyDone = true
		return res, nil
	}

	// 2. A previous attempt may have uploaded successfully but lost the
	//    response. If a scheduled video with our slot and title exists,
	//    adopt it instead of uploading a duplicate.
	if oi >= 0 && st.Entries[oi].Attempts > 0 && in.SnapshotOK {
		if slot, ok := parsePubTime(st.Entries[oi].Slot); ok {
			for _, v := range in.Snapshot {
				if v.PublishAt.Equal(slot) && strings.TrimSpace(v.Title) == strings.TrimSpace(in.Title) {
					st.dropVideoExceptOp(v.ID, in.OpKey)
					oi = st.indexByOp(in.OpKey)
					e := &st.Entries[oi]
					e.VideoID = v.ID
					e.Status = pubScheduled
					e.LastError = ""
					e.UpdatedAt = nowStr
					res.VideoID = v.ID
					res.Slot = slot
					res.Adopted = true
					return res, nil
				}
			}
		}
	}

	// 3. Slots already promised to uploads that are still in flight
	//    (including other concurrent executions) are taken first.
	occ := newSlotOccupancy(cal)
	for i := range st.Entries {
		e := &st.Entries[i]
		if e.Status != pubReserved || e.VideoID != "" {
			continue
		}
		if e.OpKey != in.OpKey {
			if u, ok := parsePubTime(e.UpdatedAt); !ok || in.Now.Sub(u) > cfg.StaleAfter {
				e.Status = pubFailed
				e.LastError = "stale reservation released"
				e.UpdatedAt = nowStr
				continue
			}
		}
		if t, ok := parsePubTime(e.Slot); ok && t.After(minStart) {
			occ.take(t)
		}
	}

	// 4. Existing scheduled videos: re-plan onto the grid.
	if in.SnapshotOK {
		plan := planExisting(occ, minStart, in.Snapshot)
		st.recordPlan(plan, nowStr)
		res.Moves = plan.Moves
		res.Kept = len(plan.Kept)
	} else {
		// YouTube could not be listed: stay safe and avoid every slot
		// (and every previous time) we already know about.
		for _, e := range st.Entries {
			if e.VideoID == "" {
				continue
			}
			for _, s := range []string{e.Slot, e.PrevPublishAt} {
				if t, ok := parsePubTime(s); ok && t.After(in.Now) {
					occ.take(t)
				}
			}
		}
	}

	// 5. This operation's own slot.
	oi = st.indexByOp(in.OpKey)
	if oi >= 0 {
		e := &st.Entries[oi]
		if t, ok := parsePubTime(e.Slot); ok && e.Status == pubReserved && t.After(minStart) {
			res.Slot = t // keep the slot this operation already holds (retry/resume)
			e.Attempts++
			e.UpdatedAt = nowStr
			return res, nil
		}
	}
	slot := occ.allocate(minStart)
	if slot.IsZero() {
		return res, errors.New("no free publishing slot could be found")
	}
	if oi < 0 {
		st.Seq++
		st.Entries = append(st.Entries, pubEntry{
			OpKey: in.OpKey, Title: in.Title, Source: "new", Status: pubReserved,
			Slot: fmtPubTime(slot), Seq: st.Seq, Attempts: 1, CreatedAt: nowStr, UpdatedAt: nowStr,
		})
	} else {
		e := &st.Entries[oi]
		e.Title = in.Title
		e.Status = pubReserved
		e.Slot = fmtPubTime(slot)
		e.Attempts++
		e.LastError = ""
		e.UpdatedAt = nowStr
	}
	res.Slot = slot
	return res, nil
}

func (st *pubState) commit(opKey, videoID, nowStr string) {
	for i := range st.Entries {
		e := &st.Entries[i]
		if e.OpKey == opKey {
			e.VideoID = videoID
			e.Status = pubScheduled
			e.LastError = ""
			e.UpdatedAt = nowStr
			return
		}
	}
}

func (st *pubState) release(opKey, msg, nowStr string) {
	for i := range st.Entries {
		e := &st.Entries[i]
		if e.OpKey == opKey && e.VideoID == "" {
			e.Status = pubFailed
			e.LastError = truncate(msg, 300)
			e.UpdatedAt = nowStr
			return
		}
	}
}

type moveOutcome struct {
	VideoID string
	Status  string // pubScheduled | pubFailed | pubSkipped
	Msg     string
}

func (st *pubState) applyOutcomes(outs []moveOutcome, nowStr string) {
	for _, o := range outs {
		for i := range st.Entries {
			e := &st.Entries[i]
			if e.VideoID != o.VideoID {
				continue
			}
			e.Status = o.Status
			e.LastError = truncate(o.Msg, 300)
			e.UpdatedAt = nowStr
			if o.Status == pubScheduled {
				e.PrevPublishAt = ""
			}
		}
	}
}

// ---------------------------------------------------------------------
// Queue service: ties the pure logic to static data + the YouTube API
// ---------------------------------------------------------------------

type pubQueue struct {
	store      engine.StaticDataStore
	workflowID string
	cfg        pubConfig
	token      string
	opID       string
	now        func() time.Time
}

func newPubQueue(rc *engine.RunContext, token string) (*pubQueue, error) {
	if rc.StaticData == nil {
		return nil, errors.New("publish queue needs the persistent workflow static-data store, which is not configured")
	}
	cfg, err := loadPubConfig(rc)
	if err != nil {
		return nil, err
	}
	return &pubQueue{
		store: rc.StaticData, workflowID: rc.Workflow.ID, cfg: cfg,
		token: token, opID: rc.CurrentOperationID, now: time.Now,
	}, nil
}

// mutate runs fn on the persisted queue state under the static-data row
// lock. Returning an error from fn rolls the whole change back.
func (q *pubQueue) mutate(ctx context.Context, fn func(st *pubState) error) error {
	return q.store.WithLock(ctx, q.workflowID, func(data map[string]any) (map[string]any, error) {
		if data == nil {
			data = map[string]any{}
		}
		st, err := loadPubState(data)
		if err != nil {
			return data, err
		}
		if err := fn(&st); err != nil {
			return data, err
		}
		if err := st.save(data); err != nil {
			return data, err
		}
		return data, nil
	})
}

type pubReservation struct {
	Slot        time.Time
	VideoID     string
	AlreadyDone bool
	Adopted     bool
	Moves       []slotMove
	Kept        int
	SnapshotErr string

	// filled by ApplyMoves
	Moved, AlreadyOK, Skipped, Failed, Deferred int
	Warnings                                    []string
}

// Begin reserves a publishing slot for the upload identified by opKey
// and plans the reconciliation of existing scheduled videos. It is
// idempotent per opKey.
func (q *pubQueue) Begin(ctx context.Context, opKey, title string) (pubReservation, error) {
	var out pubReservation

	// Fast path: this operation already uploaded in an earlier attempt.
	var done *reserveResult
	if err := q.mutate(ctx, func(st *pubState) error {
		if i := st.indexByOp(opKey); i >= 0 && st.Entries[i].VideoID != "" {
			r := reserveResult{VideoID: st.Entries[i].VideoID, AlreadyDone: true}
			r.Slot, _ = parsePubTime(st.Entries[i].Slot)
			done = &r
		}
		return nil
	}); err != nil {
		return out, err
	}
	if done != nil {
		out.Slot, out.VideoID, out.AlreadyDone = done.Slot, done.VideoID, true
		return out, nil
	}

	// Network read happens OUTSIDE the lock.
	snap, snapErr := q.fetchScheduled(ctx)
	if snapErr != nil {
		out.SnapshotErr = snapErr.Error()
		out.Warnings = append(out.Warnings, "could not list scheduled videos, reconciliation skipped: "+truncate(snapErr.Error(), 200))
		log.Printf("youtube publish queue: listing scheduled videos failed: %v", snapErr)
	}

	var res reserveResult
	err := q.mutate(ctx, func(st *pubState) error {
		var e error
		res, e = st.reserve(q.cfg, reserveInput{
			OpKey: opKey, Title: title, Now: q.now(), Snapshot: snap, SnapshotOK: snapErr == nil,
		})
		return e
	})
	if err != nil {
		return out, err
	}
	out.Slot, out.VideoID, out.AlreadyDone, out.Adopted = res.Slot, res.VideoID, res.AlreadyDone, res.Adopted
	out.Moves, out.Kept = res.Moves, res.Kept
	return out, nil
}

func pubBG() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 15*time.Second)
}

// Commit binds the uploaded video to its reservation. Uses its own
// context so a cancelled run still records a finished upload.
func (q *pubQueue) Commit(opKey, videoID string, r *pubReservation) {
	ctx, cancel := pubBG()
	defer cancel()
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		err = q.mutate(ctx, func(st *pubState) error {
			st.commit(opKey, videoID, fmtPubTime(q.now()))
			return nil
		})
		if err == nil {
			return
		}
		time.Sleep(time.Duration(attempt+1) * 300 * time.Millisecond)
	}
	// The video IS correctly scheduled on YouTube; the next reconcile
	// re-learns it from YouTube, and a retry adopts it by slot+title.
	r.Warnings = append(r.Warnings, "uploaded but could not persist queue state: "+truncate(err.Error(), 200))
	log.Printf("youtube publish queue: commit failed for video %s: %v", videoID, err)
}

// Release frees the reservation after a failed upload (best effort).
func (q *pubQueue) Release(opKey string, cause error) {
	ctx, cancel := pubBG()
	defer cancel()
	msg := "upload failed"
	if cause != nil {
		msg = cause.Error()
	}
	if err := q.mutate(ctx, func(st *pubState) error {
		st.release(opKey, msg, fmtPubTime(q.now()))
		return nil
	}); err != nil {
		log.Printf("youtube publish queue: release failed: %v", err)
	}
}

// ApplyMoves performs the planned videos.update calls (best effort: a
// failure is logged and recorded, never reported as success, and never
// fails the upload that already succeeded). Every move is verified
// against YouTube first, so a retry after a partial success is a no-op.
func (q *pubQueue) ApplyMoves(ctx context.Context, r *pubReservation) {
	if len(r.Moves) == 0 {
		return
	}
	var outs []moveOutcome
	for i, m := range r.Moves {
		if i >= q.cfg.MaxMoves {
			r.Deferred = len(r.Moves) - i
			r.Warnings = append(r.Warnings, fmt.Sprintf("%d reschedule(s) deferred to the next run (YT_PUBLISH_MAX_MOVES_PER_RUN=%d)", r.Deferred, q.cfg.MaxMoves))
			break
		}
		if ctx.Err() != nil {
			r.Deferred = len(r.Moves) - i
			break
		}
		changed, err := q.applyMove(ctx, m)
		var skip *pubSkip
		stop := false
		switch {
		case err == nil && changed:
			r.Moved++
			outs = append(outs, moveOutcome{VideoID: m.VideoID, Status: pubScheduled})
			log.Printf("youtube publish queue: video %s rescheduled %s -> %s", m.VideoID, fmtPubTime(m.From), fmtPubTime(m.To))
		case err == nil:
			r.AlreadyOK++
			outs = append(outs, moveOutcome{VideoID: m.VideoID, Status: pubScheduled})
		case errors.As(err, &skip):
			r.Skipped++
			outs = append(outs, moveOutcome{VideoID: m.VideoID, Status: pubSkipped, Msg: skip.reason})
			log.Printf("youtube publish queue: video %s not rescheduled: %s", m.VideoID, skip.reason)
		default:
			r.Failed++
			outs = append(outs, moveOutcome{VideoID: m.VideoID, Status: pubFailed, Msg: err.Error()})
			log.Printf("youtube publish queue: could NOT reschedule video %s to %s: %v", m.VideoID, fmtPubTime(m.To), err)
			if engine.IsPermanent(err) {
				// Missing scope / quota / revoked credential: the rest
				// would fail the same way, so stop instead of hammering.
				r.Deferred = len(r.Moves) - i - 1
				r.Warnings = append(r.Warnings, "rescheduling stopped: "+truncate(err.Error(), 200))
				stop = true
			}
		}
		if stop {
			break
		}
	}
	if len(outs) == 0 {
		return
	}
	bg, cancel := pubBG()
	defer cancel()
	if err := q.mutate(bg, func(st *pubState) error {
		st.applyOutcomes(outs, fmtPubTime(q.now()))
		return nil
	}); err != nil {
		r.Warnings = append(r.Warnings, "could not persist reschedule results: "+truncate(err.Error(), 200))
	}
}

type pubSkip struct{ reason string }

func (e *pubSkip) Error() string { return e.reason }

// applyMove verifies the video's current status and only then updates
// publishAt. Returns changed=false,nil when it was already at the target.
func (q *pubQueue) applyMove(ctx context.Context, m slotMove) (bool, error) {
	var cur struct {
		Items []struct {
			Status map[string]any `json:"status"`
		} `json:"items"`
	}
	getURL := "https://www.googleapis.com/youtube/v3/videos?part=status&id=" + url.QueryEscape(m.VideoID)
	if err := googleAPICall(ctx, "GET", getURL, q.token, nil, &cur, q.opID); err != nil {
		return false, err
	}
	if len(cur.Items) == 0 || cur.Items[0].Status == nil {
		return false, &pubSkip{"video not found (deleted or not accessible)"}
	}
	status := cur.Items[0].Status
	if p, _ := status["privacyStatus"].(string); p != "private" {
		return false, &pubSkip{"video is no longer private/scheduled (privacyStatus=" + p + ")"}
	}
	if u, _ := status["uploadStatus"].(string); u == "failed" || u == "rejected" || u == "deleted" {
		return false, &pubSkip{"video upload status is " + u}
	}
	curAt, ok := parsePubTime(fmt.Sprint(status["publishAt"]))
	if !ok {
		return false, &pubSkip{"video has no publishAt any more"}
	}
	if curAt.Equal(m.To) {
		return false, nil // already moved (earlier attempt succeeded)
	}
	if !curAt.Equal(m.From) {
		return false, &pubSkip{"publishAt changed since planning (" + fmtPubTime(curAt) + "); will re-plan next run"}
	}
	// videos.update replaces the whole "status" part, so send back every
	// writable field we just read and change only privacyStatus/publishAt.
	for _, k := range []string{"uploadStatus", "failureReason", "rejectionReason", "madeForKids"} {
		delete(status, k)
	}
	status["privacyStatus"] = "private"
	status["publishAt"] = fmtPubTime(m.To)
	body, err := json.Marshal(map[string]any{"id": m.VideoID, "status": status})
	if err != nil {
		return false, err
	}
	if err := googleAPICall(ctx, "PUT", "https://www.googleapis.com/youtube/v3/videos?part=status", q.token, body, nil, q.opID); err != nil {
		return false, err
	}
	return true, nil
}

// fetchScheduled lists the channel's videos that are currently
// "Scheduled" (private + a future publishAt). Public, plain-private,
// deleted, failed or rejected videos are never returned.
// Quota: channels.list 1 + playlistItems.list 1/50 + videos.list 1/50.
func (q *pubQueue) fetchScheduled(ctx context.Context) ([]schedVideo, error) {
	var ch struct {
		Items []struct {
			ContentDetails struct {
				RelatedPlaylists struct {
					Uploads string `json:"uploads"`
				} `json:"relatedPlaylists"`
			} `json:"contentDetails"`
		} `json:"items"`
	}
	if err := googleAPICall(ctx, "GET", "https://www.googleapis.com/youtube/v3/channels?part=contentDetails&mine=true", q.token, nil, &ch, q.opID); err != nil {
		return nil, fmt.Errorf("channels.list: %w", err)
	}
	if len(ch.Items) == 0 || ch.Items[0].ContentDetails.RelatedPlaylists.Uploads == "" {
		return nil, errors.New("channels.list returned no uploads playlist for the connected account")
	}
	uploads := ch.Items[0].ContentDetails.RelatedPlaylists.Uploads

	var ids []string
	pageToken := ""
	for len(ids) < q.cfg.ScanMax {
		u := "https://www.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults=50&playlistId=" + url.QueryEscape(uploads)
		if pageToken != "" {
			u += "&pageToken=" + url.QueryEscape(pageToken)
		}
		var pl struct {
			Items []struct {
				ContentDetails struct {
					VideoID string `json:"videoId"`
				} `json:"contentDetails"`
			} `json:"items"`
			NextPageToken string `json:"nextPageToken"`
		}
		if err := googleAPICall(ctx, "GET", u, q.token, nil, &pl, q.opID); err != nil {
			return nil, fmt.Errorf("playlistItems.list: %w", err)
		}
		for _, it := range pl.Items {
			if it.ContentDetails.VideoID != "" {
				ids = append(ids, it.ContentDetails.VideoID)
			}
		}
		pageToken = pl.NextPageToken
		if pageToken == "" {
			break
		}
	}
	if pageToken != "" {
		log.Printf("youtube publish queue: only the newest %d uploads were inspected (YT_PUBLISH_SCAN_MAX_VIDEOS)", len(ids))
	}

	now := q.now()
	var out []schedVideo
	for i := 0; i < len(ids); i += 50 {
		end := i + 50
		if end > len(ids) {
			end = len(ids)
		}
		var vl struct {
			Items []struct {
				ID      string `json:"id"`
				Snippet struct {
					Title string `json:"title"`
				} `json:"snippet"`
				Status struct {
					PrivacyStatus string `json:"privacyStatus"`
					PublishAt     string `json:"publishAt"`
					UploadStatus  string `json:"uploadStatus"`
				} `json:"status"`
			} `json:"items"`
		}
		u := "https://www.googleapis.com/youtube/v3/videos?part=status,snippet&id=" + url.QueryEscape(strings.Join(ids[i:end], ","))
		if err := googleAPICall(ctx, "GET", u, q.token, nil, &vl, q.opID); err != nil {
			return nil, fmt.Errorf("videos.list: %w", err)
		}
		for _, v := range vl.Items {
			if v.Status.PrivacyStatus != "private" {
				continue
			}
			if s := v.Status.UploadStatus; s == "failed" || s == "rejected" || s == "deleted" {
				continue
			}
			t, ok := parsePubTime(v.Status.PublishAt)
			if !ok || !t.After(now) {
				continue
			}
			out = append(out, schedVideo{ID: v.ID, Title: v.Snippet.Title, PublishAt: t})
		}
	}
	return out, nil
}

// decorate adds the queue outcome to the upload node's output item
// (additive: "id" and "videoId" are untouched).
func (q *pubQueue) decorate(item map[string]any, r *pubReservation) {
	if !r.Slot.IsZero() {
		item["publishAt"] = fmtPubTime(r.Slot)
		item["publishAtLocal"] = r.Slot.In(q.cfg.Loc).Format("2006-01-02 15:04 -07:00")
	}
	summary := map[string]any{
		"adopted":     r.Adopted,
		"keptInPlace": r.Kept,
		"rescheduled": r.Moved,
		"alreadyOk":   r.AlreadyOK,
		"skipped":     r.Skipped,
		"failed":      r.Failed,
		"deferred":    r.Deferred,
	}
	if r.SnapshotErr != "" {
		summary["listError"] = truncate(r.SnapshotErr, 200)
	}
	if len(r.Warnings) > 0 {
		w := make([]any, len(r.Warnings))
		for i, s := range r.Warnings {
			w[i] = s
		}
		summary["warnings"] = w
	}
	item["publishQueue"] = summary
}
