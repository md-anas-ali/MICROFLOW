package nodes

// Market research for the analytics node: which public Shorts beat their OWN
// channel's normal views (competitors) or are popular right now (chart). Uses
// only the YouTube Data API calls the analytics node already makes (cheap
// 1-unit list calls, never search.list at 100 units), shares its call budget,
// and keeps nothing beyond the small yta.Research summary that rides inside
// the existing analytics report.

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"microflow/internal/engine"
	"microflow/internal/yta"
)

const ytaMaxCompetitors = 4

// ytaCompetitors reads MICROFLOW_YTA_COMPETITOR_CHANNELS: comma-separated
// channel ids (UC...) or @handles. Empty = use the category chart instead.
func ytaCompetitors(rc *engine.RunContext) []string {
	var out []string
	for _, p := range strings.Split(rc.Env("MICROFLOW_YTA_COMPETITOR_CHANNELS"), ",") {
		p = strings.TrimSpace(p)
		if p == "" || len(p) > 60 || strings.ContainsAny(p, " /&?=\"'") {
			continue
		}
		out = append(out, p)
		if len(out) == ytaMaxCompetitors {
			break
		}
	}
	return out
}

type ytaVideoList struct {
	Items []struct {
		ID      string `json:"id"`
		Snippet struct {
			Title        string `json:"title"`
			ChannelTitle string `json:"channelTitle"`
			PublishedAt  string `json:"publishedAt"`
		} `json:"snippet"`
		ContentDetails struct {
			Duration string `json:"duration"`
		} `json:"contentDetails"`
		Statistics struct {
			ViewCount string `json:"viewCount"`
		} `json:"statistics"`
	} `json:"items"`
}

func (c *ytaClient) marketVideos(ctx context.Context, u string, shortsOnly bool) ([]yta.MarketVideo, error) {
	var vl ytaVideoList
	if err := c.get(ctx, u, &vl); err != nil {
		return nil, err
	}
	var out []yta.MarketVideo
	for _, v := range vl.Items {
		pub, err := time.Parse(time.RFC3339, v.Snippet.PublishedAt)
		if err != nil {
			continue
		}
		dur := parseISODuration(v.ContentDetails.Duration)
		if shortsOnly && (dur <= 0 || dur > ytaMaxShortSec) {
			continue
		}
		views, _ := strconv.ParseFloat(strings.TrimSpace(v.Statistics.ViewCount), 64)
		out = append(out, yta.MarketVideo{Channel: v.Snippet.ChannelTitle, Title: v.Snippet.Title, Views: views, PublishedAt: pub, DurationSec: dur})
	}
	return out, nil
}

// fetchMarket collects competitor videos (preferred) or the category chart.
// It never fails the run: problems are returned as notes.
func (c *ytaClient) fetchMarket(ctx context.Context, rc *engine.RunContext, now time.Time) (yta.Research, []string) {
	var notes []string
	comps := ytaCompetitors(rc)
	if len(comps) > 0 {
		var all []yta.MarketVideo
		for _, ch := range comps {
			q := "id=" + url.QueryEscape(ch)
			if strings.HasPrefix(ch, "@") {
				q = "forHandle=" + url.QueryEscape(ch)
			}
			var cl struct {
				Items []struct {
					ContentDetails struct {
						RelatedPlaylists struct {
							Uploads string `json:"uploads"`
						} `json:"relatedPlaylists"`
					} `json:"contentDetails"`
				} `json:"items"`
			}
			if err := c.get(ctx, ytaDataAPI+"/channels?part=contentDetails&"+q, &cl); err != nil {
				if errors.Is(err, errYTABudget) || ctx.Err() != nil {
					notes = append(notes, "competitor research stopped: "+ytaScrub(err.Error()))
					break
				}
				notes = append(notes, "competitor channel lookup failed: "+ytaScrub(err.Error()))
				continue
			}
			if len(cl.Items) == 0 || cl.Items[0].ContentDetails.RelatedPlaylists.Uploads == "" {
				notes = append(notes, "competitor channel not found: "+ytaScrub(ch))
				continue
			}
			var pl struct {
				Items []struct {
					ContentDetails struct {
						VideoID string `json:"videoId"`
					} `json:"contentDetails"`
				} `json:"items"`
			}
			if err := c.get(ctx, ytaDataAPI+"/playlistItems?part=contentDetails&maxResults=30&playlistId="+url.QueryEscape(cl.Items[0].ContentDetails.RelatedPlaylists.Uploads), &pl); err != nil {
				if errors.Is(err, errYTABudget) || ctx.Err() != nil {
					break
				}
				notes = append(notes, "competitor uploads unavailable: "+ytaScrub(err.Error()))
				continue
			}
			var ids []string
			for _, it := range pl.Items {
				if it.ContentDetails.VideoID != "" {
					ids = append(ids, it.ContentDetails.VideoID)
				}
			}
			if len(ids) == 0 {
				continue
			}
			vs, err := c.marketVideos(ctx, ytaDataAPI+"/videos?part=snippet,statistics,contentDetails&id="+url.QueryEscape(strings.Join(ids, ",")), true)
			if err != nil {
				if errors.Is(err, errYTABudget) || ctx.Err() != nil {
					break
				}
				notes = append(notes, "competitor videos unavailable: "+ytaScrub(err.Error()))
				continue
			}
			all = append(all, vs...)
		}
		r := yta.AnalyzeCompetitors(all, 1.8, now)
		if r.Evidence != yta.LevelInsufficient || len(all) > 0 {
			return r, notes
		}
		notes = append(notes, "competitor research returned no usable Shorts; trying the category chart")
	}
	// chart fallback (1 call)
	region := strings.ToUpper(strings.TrimSpace(rc.Env("MICROFLOW_YTA_REGION")))
	if len(region) != 2 {
		region = "US"
	}
	u := ytaDataAPI + "/videos?part=snippet,statistics,contentDetails&chart=mostPopular&maxResults=40&regionCode=" + url.QueryEscape(region)
	if cat := strings.TrimSpace(rc.Env("YOUTUBE_CATEGORY_ID")); cat != "" {
		if _, err := strconv.Atoi(cat); err == nil {
			u += "&videoCategoryId=" + cat
		}
	}
	vs, err := c.marketVideos(ctx, u, false)
	if err != nil {
		notes = append(notes, fmt.Sprintf("category chart unavailable: %s", ytaScrub(err.Error())))
		return yta.Research{Source: "none", Evidence: yta.LevelInsufficient, Note: "no market data could be fetched"}, notes
	}
	return yta.AnalyzeTrending(vs, now), notes
}
