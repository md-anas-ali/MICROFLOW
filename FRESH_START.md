# Fresh Start (প্রতি Service-এর আগে ও পরে clean reset)

প্রতিটি Service শুরু হওয়ার **আগে** এবং শেষ হওয়ার **পরে** MacroFlow নিজে যা মনে রাখে তা মুছে ফেলে
(`[FreshStart]` log line দেখুন):

| কী reset হয় | কেন |
|---|---|
| OpenRouter model-list cache | আগের Service-এর cache করা data যেন পরের Service না পায় |
| Idle network connection (HTTP keep-alive) | পুরনো connection ব্যবহার না হয়ে নতুন করে connect হয় |
| Go heap (full GC + OS-কে memory ফেরত) | আগের Service-এর garbage যেন RAM আটকে না রাখে |
| MemGuard throttle / back-off | আগের Service-এর memory peak-এর কারণে পরের Service যেন ধীর না হয় |

আগে থেকেই চলা cleanup (child process, temp/scratch files, HTTP body, 60s cooldown) আগের মতোই আছে।

Database-এর কিছু (Services, Workflows, Environment, Credentials, static data, schedules) **ছোঁয়া হয় না**।
কোনো execution live থাকলে reset skip হয়।

বন্ধ করতে: `MICROFLOW_FRESH_START=0`
দ্রুত করতে (cooldown কমাতে): `SERVICE_COOLDOWN_SECONDS=10` (default 60)
