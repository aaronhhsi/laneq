# Usage stats endpoint

Receives the daily totals that laneq and lanecast installs send, and serves the sums the README badges
show. It's one Supabase table and one Edge Function, in the Real Volleyball App's Supabase project
(`bpplyuckjuiuuyazcvzi`):

- `schema.sql`: the `usage_reports` table, with row level security on and no policies, so the public key
  can neither read nor write it.
- `supabase/functions/stats/index.ts`: `POST` one report (checked against the exact shape and ranges, one
  row per install per day, at most 20 installs per address per day), and `GET` the totals.

## Deploying

1. **Table:** Supabase dashboard → the project → **SQL Editor** → paste `schema.sql` → **Run**.
2. **Function**, from this folder:
   ```sh
   npx supabase login
   npx supabase functions deploy stats --project-ref bpplyuckjuiuuyazcvzi --no-verify-jwt
   ```
   Or in the dashboard: **Edge Functions → Deploy a new function → Via editor**, name it `stats`, paste
   `index.ts`, deploy, then turn off **Enforce JWT verification** in its settings (installs and badges call
   it without a key).
3. **Check it:**
   ```sh
   curl https://bpplyuckjuiuuyazcvzi.supabase.co/functions/v1/stats
   ```

`SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` are provided to the function by Supabase. An optional
`STATS_SALT` secret changes how addresses are hashed for the per-address limit.
