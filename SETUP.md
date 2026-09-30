MacroFlow Setup

সহজভাবে এক ধাপ করে করুন।

START
 ↓
1. HOST SETUP
 ↓
2. GLOBAL ENVIRONMENT
 ↓
3. HOST DONE ✓
 ↓
4. CREATE SERVICE
 ↓
5. SERVICE DONE ✓
 ↓
6. SCHEDULE
 ↓
7. FINAL CHECK
 ↓
DONE ✓

1. HOST SETUP

PC

প্রথমবার:

1. Go 1.22+ install করুন।
2. "ffmpeg", "python", "edge-tts" PATH-এ রাখুন।
3. "2-Setup-Environment.bat" চালান।
4. "DATABASE_URL" দিন।
5. "MICROFLOW_MASTER_KEY" দিন।
   - নতুন database হলে নতুন key generate করতে পারেন।
   - পুরনো database হলে আগের key-ই দিতে হবে।
6. এরপর "1-Start-MacroFlow.bat" চালান।

Default PC URL:

"http://127.0.0.1:8080"

Subdomain ব্যবহার করলে পরে সেটি "APP_REFERER_URL" / OAuth redirect-এ ব্যবহার করুন।

NEXT →

CLOUD

1. Repo-র "Dockerfile" দিয়ে deploy করুন।
2. Port "8080" ব্যবহার করুন।
3. Host Environment-এ "DATABASE_URL" ও "MICROFLOW_MASTER_KEY" দিন।
4. Deploy হওয়া URL কপি করুন।

NEXT →

---

2. GLOBAL ENVIRONMENT

যান:

⚙️ → Global Environment → Add

শুধু এই ৮টি রাখুন:

Name| কী দেবেন
"PORT"| "8080"
"MICROFLOW_SESSION_HOURS"| Login session কত ঘণ্টা থাকবে
"MICROFLOW_MASTER_KEY"| Encryption key
"MICROFLOW_LOGIN_USER"| Login username
"MICROFLOW_LOGIN_PASSWORD"| Login password
"GOOGLE_OAUTH_REDIRECT_URL"| "<আপনার URL>/api/oauth/google/callback"
"DATABASE_URL"| PostgreSQL connection string
"APP_REFERER_URL"| আপনার MacroFlow URL

Google OAuth

Google Cloud Console → OAuth Client → Authorized redirect URI

দিয়ে দিন:

"<আপনার URL>/api/oauth/google/callback"

NEXT →

---

3. HOST DONE ✓

চেক করুন:

- [ ] MacroFlow খুলছে
- [ ] Login কাজ করছে
- [ ] "/healthz" → "ok"
- [ ] Global Environment-এ ৮টি variable আছে

NEXT →

---

4. CREATE SERVICE

Home → ➕ New Service

তারপর:

Service
 ↓
Environment
 ↓
Workflow
 ↓
Google Account
 ↓
Run Now

Environment

Service → Environment → Add

Workflow-এর প্রয়োজনীয় variable দিন।

যেমন:

- "GOOGLE_SHEETS_URL"
- "GOOGLE_SHEET_ID"
- "YOUTUBE_CATEGORY_ID"
- "OPENROUTER_API_KEY"
- "GEMINI_API_KEY"
- "YOUTUBE_DATA_API_KEY"
- অন্যান্য workflow-specific API key

যা workflow-এ লাগে না, দেবেন না।

Google OAuth Client

Google Connect-এর জন্য:

- "GOOGLE_OAUTH_CLIENT_ID"
- "GOOGLE_OAUTH_CLIENT_SECRET"

Service Environment-এ দিতে পারেন।

Workflow

Service → Workflow → Import

আপনার workflow JSON Import করুন।

«ZIP-এ "workflow.json" নেই, যদিও README-তে উল্লেখ আছে। তাই আপনার workflow JSON ব্যবহার করুন।»

Google Account

Service → Credentials

প্রয়োজন অনুযায়ী:

- Gmail
- YouTube
- Google Sheets

Connect Google → Account → Allow

প্রতিটি Service-এর connection আলাদা।

Test

Service → Overview → ▶ Run now

"Success" হলে পরের ধাপে যান।

NEXT →

---

5. SERVICE DONE ✓

- [ ] Service তৈরি
- [ ] Environment দেওয়া
- [ ] Workflow Import
- [ ] Google connected
- [ ] Run successful

আরও Service চাইলে আবার New Service করুন।

NEXT →

---

6. SCHEDULE

Workflow Schedule

Service → Workflow → Open Workflow

- "Active" ON
- Schedule Trigger ON
- "Disabled" OFF
- Save

সব Service একসাথে

Home → Run All Services → Schedule

1. "+ Add Schedule"
2. দিন/সময় বা Cron দিন
3. "Add Schedule"
4. "Enable"
5. "Next run" দেখুন

Timezone:

"Asia/Dhaka"

NEXT →

---

7. FINAL CHECK

HOST ✓
GLOBAL ENVIRONMENT ✓
SERVICE ✓
ENVIRONMENT ✓
WORKFLOW ✓
GOOGLE ACCOUNT ✓
SCHEDULE ✓

DONE ✓

MacroFlow Setup Complete
