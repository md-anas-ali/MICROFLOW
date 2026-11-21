package parser

import (
	"os"
	"testing"
)

func TestUserWorkflowWithSocialParses(t *testing.T) {
	raw, err := os.ReadFile("../../examples/My_workflow_with_social.json")
	if err != nil {
		t.Skip("example not present")
	}
	r, err := Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range r.Checklist {
		if !c.Supported {
			t.Errorf("unsupported node: %+v", c)
		}
	}
	for _, name := range []string{"Social Captions", "Read Final Video (Social)", "Post to Facebook Reels", "Post to Instagram Reels", "Post to TikTok", "Post to Dailymotion", "Rumble Manual Pack", "Snapchat Spotlight Manual Pack"} {
		if r.Workflow.Nodes[name] == nil {
			t.Errorf("missing node %q", name)
		}
	}
}
