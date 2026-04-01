# PayTrack Web

Flask + SQLite port of the PayTrack Flutter app — same business logic, runs in any browser.

## Quick start

```bash
pip install flask
python run.py
# → http://localhost:5000
```

No other dependencies. SQLite is built into Python.

---

## Database models (replaces Flutter SharedPreferences + files)

| Table | Purpose |
|---|---|
| `tier_definitions` | Named pay tiers: min OK, max OK, price per OK |
| `user_tiers` | Assigns a user to a specific tier |
| `custom_names` | Maps User ID → display name |
| `csv_entries` | Imported rows from CSV files |
| `balances` | Payment adjustments per user |

---

## CSV format

Each CSV file should be named by date, e.g. `2024-01-15.csv`.

Required columns (order doesn't matter, headers must match exactly):

```
User ID, Username, OK Count, Rate, Bkash, Rocket, Paid Status
```

- **User ID** — unique identifier for the worker
- **OK Count** — number of OKs completed
- **Rate** — fallback price per OK (used when no tier matches)
- **Bkash / Rocket** — payment account numbers (optional)
- **Paid Status** — e.g. `paid` / `unpaid`

---

## How tier pricing works (same as Flutter app)

1. If the user has a **user-specific tier assignment** → use that tier's price per OK.
2. Else → use the **global tier definitions** (all tiers act as the global pool).
3. If no tier range matches the OK count → fall back to the CSV `Rate` column.

---

## Pages

| URL | Description |
|---|---|
| `/` | Overview — all users sorted by pending balance |
| `/user/<id>` | User detail — entries, pending balance, record transactions |
| `/upload` | Upload one or more CSV files |
| `/settings` | Manage tiers, user assignments, custom names |
| `/settings/export` | Download config + balances as JSON |
| `/settings/import` | Restore config from exported JSON |
