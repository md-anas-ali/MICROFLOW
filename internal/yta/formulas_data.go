package yta

// Hook/title formula catalogue. The names, match patterns and failure notes are
// adapted from the open-source youtube-agent-skill project (MIT licence,
// skills/yt-script/hooks.json) and are used here ONLY to label hook and title
// wording that is already on screen; they never claim why a video performed.
type hookFormula struct {
	Name      string
	FailsWhen string
	Patterns  []string
}

var hookFormulas = []hookFormula{
	{"The Statistic", "Dies if the number is vague or unverifiable. Say the source out loud.", []string{`\b\d+(\.\d+)?\s?%`, `\b\d+ (out of|in) \d+\b`, `\b\d{2,}(,\d{3})+\b`}},
	{"Someone Else's Result", "Do not invent the number. If you cannot name the channel, use a different formula.", []string{`\bthis (channel|guy|creator|account)\b`, `\bwent from .* to\b`, `\bin (a|one|two|three|\d+) (week|month|year)s?\b`}},
	{"The Mistake", "Only works if the mistake is common and the viewer can check it in one second.", []string{`\byou('re| are) (probably |still )?(doing|making)\b`, `\bthe (mistake|reason) (everyone|most people|nobody)\b`, `\bstop (doing )?\b`}},
	{"Contrarian Flip", "Needs evidence in the next 15 seconds or it reads as contrarian for its own sake.", []string{`\beveryone (says|thinks|tells you)\b`, `\bis (actually )?wrong\b`, `\bnobody (tells|talks about)\b`}},
	{"The Reveal", "You have to actually show it, early. A reveal at 8:00 is a retention cliff at 1:00.", []string{`\bI('m| am) going to show you\b`, `\bhere('s| is) (the|exactly)\b`, `\bon screen\b`}},
	{"The Superlative", "Superlatives are cheap. Earn it by naming what it beat.", []string{`\bthe (best|worst|fastest|easiest|only)\b`, `\bnumber one\b`}},
	{"The Clock", "The clock has to be true. Overrun it and the next video's hook is not believed.", []string{`\bin (the next )?\d+ (seconds|minutes|hours)\b`, `\bby the end of this video\b`, `\bunder \d+\b`}},
	{"I Tried It", "Needs a real cost - time, money, risk - or there is no stake.", []string{`\bI (tried|spent|tested|let|gave)\b`, `\bfor \d+ (days|weeks|months)\b`, `\bso you don'?t have to\b`}},
	{"The Question", "Must be the viewer's question, not yours. Pull it from your own comments.", []string{`^(why|how|what|when|should|can|do|does|is)\b`, `\?\s*$`}},
	{"Before and After", "Only works with a visual. Do not use it on a talking-head-only video.", []string{`\bbefore\b.*\bafter\b`, `\bthis was\b.*\bthis is\b`, `\bused to\b`}},
	{"The Teardown", "Name what you pulled and how many. A teardown of one example is an anecdote.", []string{`\b(broke|break|breaking) (it )?down\b`, `\bteardown\b`, `\banalys(ed|ed|is|e)\b`, `\bI pulled\b`}},
	{"The Stack", "Both tools must be nameable and the outcome must need both.", []string{`\b\w+ (plus|\+) \w+\b`, `\bcombine\b`, `\btogether\b`}},
	{"The Warning", "If the cost is small the hook is a lie. Reserve it.", []string{`\bdo not\b`, `\bdon'?t (upload|post|publish|start)\b`, `\bbefore you\b`, `\bstop\b`}},
	{"The List", "The count must be exact and every item must be different in kind.", []string{`^\d+ \w+`, `\b(here are|these are) \d+\b`}},
	{"The Receipt", "Requires a real screenshot. A described receipt is not a receipt.", []string{`\bthis is (the|my)\b`, `\bscreenshot\b`, `\bproof\b`, `\breceipts?\b`}},
	{"The Insider", "Do not claim insider access you do not have.", []string{`\b(nobody|almost nobody|most people) (knows?|opens?|uses?)\b`, `\bhidden\b`, `\bburied\b`}},
	{"The Impossible Claim", "The proof has to start within 20 seconds or it reads as clickbait.", []string{`\bwithout\b`, `\bnever\b`, `\bin one\b`, `\bzero\b`}},
	{"The Comparison", "Say the losing option out loud. A comparison with no loser is an advert.", []string{`\bvs\.?\b`, `\bversus\b`, `\bwhich (one )?(is|wins)\b`, `\bsame .* (three|two) ways\b`}},
	{"The Origin", "Works once per channel. It is a reveal about you, and you only have one.", []string{`\bevery (video|post) .* starts\b`, `\bit all (starts|started)\b`, `\bthe (real )?reason\b`}},
	{"The Deadline", "Only use with a real, checkable date. This is the fastest formula to lose trust with.", []string{`\b(this|next) (week|month|year)\b`, `\bis changing\b`, `\bno longer\b`, `\bas of\b`}},
	{"The Direct Address", "Narrow beats broad. 'If you make videos' addresses nobody.", []string{`^if you\b`, `\bfor (anyone|everyone|people) who\b`, `\byou specifically\b`}},
}
