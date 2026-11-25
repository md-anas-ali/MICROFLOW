package nodes

// socialPublish — one node that publishes the video flowing through the
// workflow (binary "data", same as the youTube node) to other platforms.
//
//   platform = facebookReels | instagramReels | tiktok | dailymotion
//              -> real, official API upload
//   platform = rumble | snapchatSpotlight
//              -> NO public upload API exists for normal creators, so the node
//                 does NOT pretend to upload. It validates the file and returns
//                 a ready-to-paste "manual pack" (title / caption / tags / file).
//
// Secrets are read from node parameters, normally "{{ $env.NAME }}" (Global
// Environment), never hard-coded. Every Graph/TikTok/Dailymotion call sends the
// token in an Authorization header (not in the URL), so a network error message
// can never leak it.
//
// UNVERIFIED: these call live third-party endpoints and need real app
// approvals/tokens, which can only be exercised on the user's own accounts.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"microflow/internal/engine"
	"microflow/internal/expr"
	"microflow/internal/model"
)

// Base URLs are vars only so tests can point them at httptest servers.
var (
	graphBase       = "https://graph.facebook.com/v21.0"
	tiktokBase      = "https://open.tiktokapis.com"
	dailymotionBase = "https://api.dailymotion.com"
)

const socialMaxBody = 2 << 20 // cap on any API response we read

// FacebookPageResolver supplies Page tokens from the global "Connect Facebook"
// login (implemented by *vault.FacebookAccount). With it, a facebookReels /
// instagramReels node needs only pageId (comma-separated for several Pages):
// accessToken and igUserId are filled in automatically. An explicit
// accessToken on the node still wins, so old workflows keep working.
type FacebookPageResolver interface {
	Connected(ctx context.Context) bool
	PageAccess(ctx context.Context, pageID string) (token, igUserID string, err error)
}

type SocialPublishExecutor struct {
	Client   *http.Client         // nil -> a no-overall-timeout client (uploads are long); ctx bounds each call
	Facebook FacebookPageResolver // nil -> Facebook/Instagram need an explicit accessToken
}

// splitIDs splits "111, 222\n333" into ["111","222","333"].
func splitIDs(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ';' || r == ' ' || r == '\n' || r == '\t' })
}

// fbLoginMsg turns Facebook's "token invalid" (code 190) into a plain instruction.
func fbLoginMsg(err error) error {
	var a *apiErr
	if errors.As(err, &a) && strings.Contains(a.Body, `"code":190`) {
		return engine.Permanent(fmt.Errorf("Facebook connection expired or was revoked -- open Credentials and click Reconnect Facebook (%w)", err))
	}
	return err
}

func (e *SocialPublishExecutor) client() *http.Client {
	if e.Client != nil {
		return e.Client
	}
	return &http.Client{}
}

func (e *SocialPublishExecutor) Execute(ctx context.Context, rc *engine.RunContext, node *model.Node, input model.NodeOutput) (model.NodeOutput, error) {
	var out []model.Item
	for _, it := range flatten(input) {
		exprCtx := rc.ExprContext(it.JSON)
		p := func(key, def string) string {
			v, _ := expr.EvalValue(node.ParamString(key, def), exprCtx)
			if v == nil {
				return strings.TrimSpace(def)
			}
			// An unset/empty {{ $env.X }} falls back to the literal default, so
			// optional env variables can be left empty.
			if s := strings.TrimSpace(fmt.Sprint(v)); s != "" {
				return s
			}
			return strings.TrimSpace(def)
		}
		platform := p("platform", "")
		ref, ok := it.Binary["data"]
		if !ok || ref.FileName == "" {
			return nil, engine.Permanent(fmt.Errorf("socialPublish %q: needs binary video data on the item (field \"data\")", node.Name))
		}
		// skipIfUnconfigured=true: a platform whose token/ID is not set yet is skipped
		// (not failed), so the workflow can be wired for all platforms up front and
		// each one switches on as soon as its env variables are filled in.
		if boolStr(p("skipIfUnconfigured", "false")) == "true" {
			if missing := unconfigured(platform, p, e.Facebook != nil && e.Facebook.Connected(ctx)); missing != "" {
				out = append(out, model.Item{JSON: map[string]any{"platform": platform, "skipped": true, "reason": "not configured: " + missing}, Binary: it.Binary})
				continue
			}
		}
		var (
			results []map[string]any // one per Page for Facebook/Instagram, else one
			res     map[string]any
			err     error
		)
		switch platform {
		case "facebookReels":
			results, err = e.facebookPages(ctx, p, ref)
		case "instagramReels":
			results, err = e.instagramPages(ctx, p, ref)
		case "tiktok":
			res, err = e.tiktok(ctx, p, ref)
		case "dailymotion":
			res, err = e.dailymotion(ctx, p, ref)
		case "rumble", "snapchatSpotlight":
			res, err = manualPack(platform, p, ref)
		default:
			return nil, engine.Permanent(fmt.Errorf("socialPublish %q: unknown platform %q (use facebookReels|instagramReels|tiktok|dailymotion|rumble|snapchatSpotlight)", node.Name, platform))
		}
		if err != nil {
			return nil, fmt.Errorf("socialPublish %q (%s): %w", node.Name, platform, err)
		}
		if results == nil {
			results = []map[string]any{res}
		}
		for _, r := range results {
			r["platform"] = platform
			out = append(out, model.Item{JSON: r, Binary: it.Binary})
		}
	}
	return model.NodeOutput{out}, nil
}

// unconfigured returns the name of the first missing required setting ("" = all set).
// fbConnected = a global Facebook login exists, so a token/IG id isn't needed
// on the node as long as pageId is set.
func unconfigured(platform string, p func(string, string) string, fbConnected bool) string {
	switch platform {
	case "facebookReels":
		if p("pageId", "") == "" {
			return "pageId"
		}
		if p("accessToken", "") == "" && !fbConnected {
			return "accessToken (or Connect Facebook)"
		}
	case "instagramReels":
		if p("accessToken", "") != "" && p("igUserId", "") != "" {
			return ""
		}
		if p("pageId", "") != "" && fbConnected {
			return ""
		}
		return "igUserId + accessToken (or pageId + Connect Facebook)"
	case "tiktok", "dailymotion":
		if p("accessToken", "") == "" && p("refreshToken", "") == "" {
			return "accessToken or refreshToken"
		}
	}
	return ""
}

// ---------------------------------------------------------------- helpers

type apiErr struct {
	Status int
	Body   string
}

func (a *apiErr) Error() string { return fmt.Sprintf("HTTP %d: %s", a.Status, a.Body) }

// do runs one request and returns the (capped) body. Non-2xx -> *apiErr.
func (e *SocialPublishExecutor) do(ctx context.Context, method, rawURL string, hdr map[string]string, body io.Reader, contentLength int64) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, method, rawURL, body)
	if err != nil {
		return nil, 0, err
	}
	if contentLength >= 0 && body != nil {
		req.ContentLength = contentLength
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := e.client().Do(req)
	if err != nil {
		// url.Error embeds the URL; none of our URLs carry secrets.
		return nil, 0, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, socialMaxBody))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		s := string(b)
		if len(s) > 600 {
			s = s[:600] + "…"
		}
		return b, resp.StatusCode, &apiErr{Status: resp.StatusCode, Body: s}
	}
	return b, resp.StatusCode, nil
}

func (e *SocialPublishExecutor) doJSON(ctx context.Context, method, rawURL string, hdr map[string]string, payload any, into any) error {
	var rdr io.Reader
	var n int64 = -1
	h := map[string]string{}
	for k, v := range hdr {
		h[k] = v
	}
	switch v := payload.(type) {
	case nil:
	case url.Values:
		s := v.Encode()
		rdr, n = strings.NewReader(s), int64(len(s))
		h["Content-Type"] = "application/x-www-form-urlencoded"
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		rdr, n = bytes.NewReader(b), int64(len(b))
		h["Content-Type"] = "application/json; charset=UTF-8"
	}
	b, _, err := e.do(ctx, method, rawURL, h, rdr, n)
	if err != nil {
		return err
	}
	if into != nil && len(b) > 0 {
		if jerr := json.Unmarshal(b, into); jerr != nil {
			return fmt.Errorf("bad JSON from %s: %w", rawURL, jerr)
		}
	}
	return nil
}

func permanentIfAuth(err error) error {
	var a *apiErr
	if errors.As(err, &a) && (a.Status == 400 || a.Status == 401 || a.Status == 403) {
		return engine.Permanent(err)
	}
	return err
}

func need(vals map[string]string) error {
	for k, v := range vals {
		if v == "" {
			return engine.Permanent(fmt.Errorf("required setting %q is empty", k))
		}
	}
	return nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func fileSize(path string) (int64, error) {
	st, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

// ---------------------------------------------------------------- Facebook Reels (Page)

// facebookPages posts to every Page in pageId (comma-separated allowed). The
// Page token comes from the node's accessToken if set, else from the global
// Facebook login. Pages are posted one after another; the first failure stops
// the node (Pages already posted are not rolled back).
func (e *SocialPublishExecutor) facebookPages(ctx context.Context, p func(string, string) string, ref model.BinaryRef) ([]map[string]any, error) {
	ids := splitIDs(p("pageId", ""))
	if len(ids) == 0 {
		return nil, engine.Permanent(errors.New(`required setting "pageId" is empty`))
	}
	var all []map[string]any
	for _, id := range ids {
		token := p("accessToken", "")
		if token == "" && e.Facebook != nil {
			t, _, err := e.Facebook.PageAccess(ctx, id)
			if err != nil {
				return nil, engine.Permanent(err)
			}
			token = t
		}
		res, err := e.facebookReels(ctx, id, token, p, ref)
		if err != nil {
			return nil, fmt.Errorf("page %s: %w", id, fbLoginMsg(err))
		}
		res["pageId"] = id
		all = append(all, res)
	}
	return all, nil
}

func (e *SocialPublishExecutor) facebookReels(ctx context.Context, pageID, token string, p func(string, string) string, ref model.BinaryRef) (map[string]any, error) {
	if err := need(map[string]string{"pageId": pageID, "accessToken": token}); err != nil {
		return nil, err
	}
	size, err := fileSize(ref.FileName)
	if err != nil {
		return nil, err
	}
	auth := map[string]string{"Authorization": "Bearer " + token}

	var start struct {
		VideoID   string `json:"video_id"`
		UploadURL string `json:"upload_url"`
	}
	if err := e.doJSON(ctx, "POST", fmt.Sprintf("%s/%s/video_reels", graphBase, url.PathEscape(pageID)), auth,
		url.Values{"upload_phase": {"start"}}, &start); err != nil {
		return nil, permanentIfAuth(fmt.Errorf("start: %w", err))
	}
	if start.VideoID == "" || start.UploadURL == "" {
		return nil, errors.New("start: Facebook returned no video_id/upload_url")
	}

	f, err := os.Open(ref.FileName)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if _, _, err := e.do(ctx, "POST", start.UploadURL, map[string]string{
		"Authorization": "OAuth " + token,
		"offset":        "0",
		"file_size":     strconv.FormatInt(size, 10),
	}, f, size); err != nil {
		return nil, fmt.Errorf("upload bytes: %w", err)
	}

	state := strings.ToUpper(p("videoState", "PUBLISHED")) // PUBLISHED | DRAFT
	if state != "DRAFT" {
		state = "PUBLISHED"
	}
	finish := url.Values{
		"upload_phase": {"finish"},
		"video_id":     {start.VideoID},
		"video_state":  {state},
		"description":  {p("description", "")},
	}
	var fin map[string]any
	if err := e.doJSON(ctx, "POST", fmt.Sprintf("%s/%s/video_reels", graphBase, url.PathEscape(pageID)), auth, finish, &fin); err != nil {
		return nil, fmt.Errorf("finish: %w", err)
	}
	return map[string]any{"id": start.VideoID, "videoId": start.VideoID, "state": state, "response": fin,
		"url": "https://www.facebook.com/reel/" + start.VideoID}, nil
}

// ---------------------------------------------------------------- Instagram Reels

// instagramPages: explicit igUserId+accessToken behave exactly as before.
// Otherwise, for each Page in pageId, the Instagram business account linked to
// that Page and the Page token come from the global Facebook login.
func (e *SocialPublishExecutor) instagramPages(ctx context.Context, p func(string, string) string, ref model.BinaryRef) ([]map[string]any, error) {
	if igID, token := p("igUserId", ""), p("accessToken", ""); igID != "" || token != "" || p("pageId", "") == "" {
		res, err := e.instagramReels(ctx, igID, token, p, ref)
		if err != nil {
			return nil, fbLoginMsg(err)
		}
		return []map[string]any{res}, nil
	}
	if e.Facebook == nil {
		return nil, engine.Permanent(errors.New("Instagram needs igUserId + accessToken (Facebook login is not available on this server)"))
	}
	var all []map[string]any
	for _, id := range splitIDs(p("pageId", "")) {
		token, igID, err := e.Facebook.PageAccess(ctx, id)
		if err != nil {
			return nil, engine.Permanent(err)
		}
		if igID == "" {
			return nil, engine.Permanent(fmt.Errorf("page %s has no linked Instagram business/creator account (link it in Facebook Page settings, then reconnect Facebook)", id))
		}
		res, err := e.instagramReels(ctx, igID, token, p, ref)
		if err != nil {
			return nil, fmt.Errorf("page %s: %w", id, fbLoginMsg(err))
		}
		res["pageId"], res["igUserId"] = id, igID
		all = append(all, res)
	}
	return all, nil
}

func (e *SocialPublishExecutor) instagramReels(ctx context.Context, igID, token string, p func(string, string) string, ref model.BinaryRef) (map[string]any, error) {
	if err := need(map[string]string{"igUserId": igID, "accessToken": token}); err != nil {
		return nil, err
	}
	size, err := fileSize(ref.FileName)
	if err != nil {
		return nil, err
	}
	auth := map[string]string{"Authorization": "Bearer " + token}

	form := url.Values{
		"media_type":    {"REELS"},
		"upload_type":   {"resumable"},
		"caption":       {p("caption", "")},
		"share_to_feed": {boolStr(p("shareToFeed", "true"))},
	}
	var cont struct {
		ID  string `json:"id"`
		URI string `json:"uri"`
	}
	if err := e.doJSON(ctx, "POST", fmt.Sprintf("%s/%s/media", graphBase, url.PathEscape(igID)), auth, form, &cont); err != nil {
		return nil, permanentIfAuth(fmt.Errorf("create container: %w", err))
	}
	if cont.ID == "" || cont.URI == "" {
		return nil, errors.New("create container: Instagram returned no id/uri")
	}

	f, err := os.Open(ref.FileName)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if _, _, err := e.do(ctx, "POST", cont.URI, map[string]string{
		"Authorization": "OAuth " + token,
		"offset":        "0",
		"file_size":     strconv.FormatInt(size, 10),
	}, f, size); err != nil {
		return nil, fmt.Errorf("upload bytes: %w", err)
	}

	// Wait for Instagram's processing to finish (up to ~10 min).
	deadline := time.Now().Add(10 * time.Minute)
	for {
		var st struct {
			StatusCode string `json:"status_code"`
			Status     string `json:"status"`
		}
		if err := e.doJSON(ctx, "GET", fmt.Sprintf("%s/%s?fields=status_code,status", graphBase, url.PathEscape(cont.ID)), auth, nil, &st); err != nil {
			return nil, fmt.Errorf("status: %w", err)
		}
		if st.StatusCode == "FINISHED" {
			break
		}
		if st.StatusCode == "ERROR" || st.StatusCode == "EXPIRED" {
			return nil, engine.Permanent(fmt.Errorf("Instagram rejected the video (%s): %s", st.StatusCode, st.Status))
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("Instagram is still processing after 10 min (container %s) — NOT published", cont.ID)
		}
		if err := sleepCtx(ctx, 6*time.Second); err != nil {
			return nil, err
		}
	}

	var pub struct {
		ID string `json:"id"`
	}
	if err := e.doJSON(ctx, "POST", fmt.Sprintf("%s/%s/media_publish", graphBase, url.PathEscape(igID)), auth,
		url.Values{"creation_id": {cont.ID}}, &pub); err != nil {
		return nil, fmt.Errorf("publish: %w", err)
	}
	res := map[string]any{"id": pub.ID, "mediaId": pub.ID, "containerId": cont.ID}
	var pl struct {
		Permalink string `json:"permalink"`
	}
	if err := e.doJSON(ctx, "GET", fmt.Sprintf("%s/%s?fields=permalink", graphBase, url.PathEscape(pub.ID)), auth, nil, &pl); err == nil && pl.Permalink != "" {
		res["url"] = pl.Permalink
	}
	return res, nil
}

func boolStr(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "false", "0", "no", "off":
		return "false"
	}
	return "true"
}

// ---------------------------------------------------------------- TikTok (Content Posting API)

func (e *SocialPublishExecutor) tiktokToken(ctx context.Context, p func(string, string) string) (token, newRefresh string, err error) {
	if t := p("accessToken", ""); t != "" {
		return t, "", nil
	}
	ck, cs, rt := p("clientKey", ""), p("clientSecret", ""), p("refreshToken", "")
	if ck == "" || cs == "" || rt == "" {
		return "", "", engine.Permanent(errors.New("set accessToken, or all of clientKey, clientSecret, refreshToken"))
	}
	var r struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		Error        string `json:"error"`
		ErrorDesc    string `json:"error_description"`
	}
	form := url.Values{"client_key": {ck}, "client_secret": {cs}, "grant_type": {"refresh_token"}, "refresh_token": {rt}}
	if err := e.doJSON(ctx, "POST", tiktokBase+"/v2/oauth/token/", nil, form, &r); err != nil {
		return "", "", permanentIfAuth(fmt.Errorf("refresh token: %w", err))
	}
	if r.AccessToken == "" {
		return "", "", engine.Permanent(fmt.Errorf("refresh token failed: %s %s", r.Error, r.ErrorDesc))
	}
	if r.RefreshToken != "" && r.RefreshToken != rt {
		newRefresh = r.RefreshToken
	}
	return r.AccessToken, newRefresh, nil
}

func (e *SocialPublishExecutor) tiktok(ctx context.Context, p func(string, string) string, ref model.BinaryRef) (map[string]any, error) {
	token, newRefresh, err := e.tiktokToken(ctx, p)
	if err != nil {
		return nil, err
	}
	size, err := fileSize(ref.FileName)
	if err != nil {
		return nil, err
	}
	// TikTok chunk rules: 5–64 MB per chunk (last may be larger, up to 128 MB);
	// files under 5 MB must go as a single chunk.
	const chunk = int64(10 << 20)
	chunkSize, total := size, int64(1)
	if size > chunk {
		chunkSize, total = chunk, size/chunk
	}
	privacy := p("privacyLevel", "SELF_ONLY") // unaudited TikTok apps can only post SELF_ONLY
	postInfo := map[string]any{
		"title":           truncRunes(p("title", ""), 2200),
		"privacy_level":   privacy,
		"disable_duet":    boolStr(p("disableDuet", "false")) == "true",
		"disable_comment": boolStr(p("disableComment", "false")) == "true",
		"disable_stitch":  boolStr(p("disableStitch", "false")) == "true",
	}
	if boolStr(p("aiGenerated", "false")) == "true" {
		postInfo["is_aigc"] = true
	}
	initBody := map[string]any{
		"post_info": postInfo,
		"source_info": map[string]any{
			"source": "FILE_UPLOAD", "video_size": size, "chunk_size": chunkSize, "total_chunk_count": total,
		},
	}
	var init struct {
		Data struct {
			PublishID string `json:"publish_id"`
			UploadURL string `json:"upload_url"`
		} `json:"data"`
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	auth := map[string]string{"Authorization": "Bearer " + token}
	if err := e.doJSON(ctx, "POST", tiktokBase+"/v2/post/publish/video/init/", auth, initBody, &init); err != nil {
		return nil, permanentIfAuth(fmt.Errorf("init: %w", err))
	}
	if init.Data.UploadURL == "" {
		return nil, engine.Permanent(fmt.Errorf("init: TikTok refused: %s %s", init.Error.Code, init.Error.Message))
	}

	f, err := os.Open(ref.FileName)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var off int64
	for i := int64(0); i < total; i++ {
		n := chunkSize
		if i == total-1 {
			n = size - off // last chunk absorbs the remainder
		}
		sec := io.NewSectionReader(f, off, n)
		if _, _, err := e.do(ctx, "PUT", init.Data.UploadURL, map[string]string{
			"Content-Type":  "video/mp4",
			"Content-Range": fmt.Sprintf("bytes %d-%d/%d", off, off+n-1, size),
		}, sec, n); err != nil {
			return nil, fmt.Errorf("upload chunk %d/%d: %w", i+1, total, err)
		}
		off += n
	}

	res := map[string]any{"id": init.Data.PublishID, "publishId": init.Data.PublishID, "privacyLevel": privacy}
	if newRefresh != "" {
		res["newRefreshToken"] = newRefresh // TikTok rotated it: save this into your env or the next run fails
	}
	// Poll the publish status for up to ~3 min; a slow review is not an error.
	deadline := time.Now().Add(3 * time.Minute)
	for {
		var st struct {
			Data struct {
				Status     string `json:"status"`
				FailReason string `json:"fail_reason"`
			} `json:"data"`
		}
		if err := e.doJSON(ctx, "POST", tiktokBase+"/v2/post/publish/status/fetch/", auth,
			map[string]any{"publish_id": init.Data.PublishID}, &st); err != nil {
			res["statusNote"] = "status check failed: " + err.Error()
			break
		}
		res["status"] = st.Data.Status
		if st.Data.Status == "PUBLISH_COMPLETE" {
			break
		}
		if st.Data.Status == "FAILED" {
			return nil, engine.Permanent(fmt.Errorf("TikTok publish failed: %s", st.Data.FailReason))
		}
		if time.Now().After(deadline) {
			res["statusNote"] = "still processing at TikTok; check the TikTok app"
			break
		}
		if err := sleepCtx(ctx, 6*time.Second); err != nil {
			return nil, err
		}
	}
	return res, nil
}

func truncRunes(s string, max int) string {
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	r := []rune(s)
	return string(r[:max])
}

// ---------------------------------------------------------------- Dailymotion

func (e *SocialPublishExecutor) dailymotionToken(ctx context.Context, p func(string, string) string) (string, error) {
	if t := p("accessToken", ""); t != "" {
		return t, nil
	}
	id, secret, rt := p("apiKey", ""), p("apiSecret", ""), p("refreshToken", "")
	if id == "" || secret == "" || rt == "" {
		return "", engine.Permanent(errors.New("set accessToken, or all of apiKey, apiSecret, refreshToken"))
	}
	var r struct {
		AccessToken string `json:"access_token"`
	}
	form := url.Values{"grant_type": {"refresh_token"}, "client_id": {id}, "client_secret": {secret}, "refresh_token": {rt}}
	if err := e.doJSON(ctx, "POST", dailymotionBase+"/oauth/token", nil, form, &r); err != nil {
		return "", permanentIfAuth(fmt.Errorf("refresh token: %w", err))
	}
	if r.AccessToken == "" {
		return "", engine.Permanent(errors.New("Dailymotion returned no access_token"))
	}
	return r.AccessToken, nil
}

func (e *SocialPublishExecutor) dailymotion(ctx context.Context, p func(string, string) string, ref model.BinaryRef) (map[string]any, error) {
	token, err := e.dailymotionToken(ctx, p)
	if err != nil {
		return nil, err
	}
	if err := need(map[string]string{"title": p("title", "")}); err != nil {
		return nil, err
	}
	auth := map[string]string{"Authorization": "Bearer " + token}

	var up struct {
		UploadURL string `json:"upload_url"`
	}
	if err := e.doJSON(ctx, "GET", dailymotionBase+"/file/upload", auth, nil, &up); err != nil {
		return nil, permanentIfAuth(fmt.Errorf("get upload url: %w", err))
	}
	if up.UploadURL == "" {
		return nil, errors.New("Dailymotion returned no upload_url")
	}

	// Stream the file as multipart without holding it in RAM.
	f, err := os.Open(ref.FileName)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	go func() {
		part, err := mw.CreateFormFile("file", filepath.Base(ref.FileName))
		if err == nil {
			_, err = io.Copy(part, f)
		}
		if err == nil {
			err = mw.Close()
		}
		pw.CloseWithError(err)
	}()
	b, _, err := e.do(ctx, "POST", up.UploadURL, map[string]string{"Content-Type": mw.FormDataContentType()}, pr, -1)
	pr.Close()
	if err != nil {
		return nil, fmt.Errorf("upload bytes: %w", err)
	}
	var upRes struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal(b, &upRes); err != nil || upRes.URL == "" {
		return nil, fmt.Errorf("Dailymotion upload gave no file url: %s", truncRunes(string(b), 300))
	}

	form := url.Values{
		"url":                 {upRes.URL},
		"title":               {truncRunes(p("title", ""), 255)},
		"description":         {p("description", "")},
		"channel":             {p("channel", "lifestyle")},
		"published":           {boolStr(p("published", "true"))},
		"is_created_for_kids": {boolStr(p("madeForKids", "false"))}, // legally required flag; set truthfully
	}
	if tags := p("tags", ""); tags != "" {
		form.Set("tags", tags) // comma separated
	}
	var v struct {
		ID string `json:"id"`
	}
	if err := e.doJSON(ctx, "POST", dailymotionBase+"/me/videos", auth, form, &v); err != nil {
		return nil, fmt.Errorf("create video: %w", err)
	}
	return map[string]any{"id": v.ID, "videoId": v.ID, "url": "https://www.dailymotion.com/video/" + v.ID}, nil
}

// ---------------------------------------------------------------- Rumble / Snapchat Spotlight (manual pack)

func manualPack(platform string, p func(string, string) string, ref model.BinaryRef) (map[string]any, error) {
	size, err := fileSize(ref.FileName)
	if err != nil {
		return nil, err
	}
	title, caption, tags := p("title", ""), p("caption", ""), p("tags", "")
	res := map[string]any{
		"manualUploadRequired": true,
		"reason":               "no public upload API for this platform; nothing was uploaded",
		"file":                 ref.FileName,
		"sizeBytes":            size,
		"title":                title,
		"caption":              caption,
		"tags":                 tags,
	}
	var warn []string
	if platform == "snapchatSpotlight" {
		// Limits per Snap Public Profile docs as relayed by Ayrshare: MP4, 9:16, 5–60 s, <=1 GB, caption <=160.
		if size > 1<<30 {
			warn = append(warn, "file is over 1 GB")
		}
		if utf8.RuneCountInString(caption) > 160 {
			warn = append(warn, "caption is over 160 chars; Snapchat cuts it")
			res["captionTrimmed"] = truncRunes(caption, 160)
		}
		if !strings.HasSuffix(strings.ToLower(ref.FileName), ".mp4") {
			warn = append(warn, "Spotlight expects MP4")
		}
		res["spec"] = "MP4, 9:16 vertical, 5-60 seconds, max 1 GB, caption max 160 chars"
	}
	if len(warn) > 0 {
		res["warnings"] = warn
	}
	return res, nil
}
