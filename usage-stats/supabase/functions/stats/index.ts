// Usage stats for laneq and lanecast.
//
//   POST /stats   {install_id, kind, version, totals}  one install's daily report
//   GET  /stats                                         the totals across every install (README badges)
//
// The database key never leaves Supabase: installs only talk to this function,
// which accepts exactly the report shape, drops anything out of range, keeps one
// row per install per day, and limits how many installs one address can report.
import { createClient } from "jsr:@supabase/supabase-js@2";

const db = createClient(Deno.env.get("SUPABASE_URL")!, Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!);

// The only totals each kind may send, and the most each can plausibly be.
const FIELDS: Record<string, Record<string, number>> = {
  laneq: { jobs: 1e9, succeeded: 1e9, attempts: 1e10, lanes: 1e6, schedules: 1e6, apps: 1e4, data_bytes: 1e13 },
  lanecast: { accounts: 1e4, posts_published: 1e8, posts_queued: 1e8, media_bytes: 1e14 },
};
const INSTALLS_PER_ADDRESS_PER_DAY = 20;
const CORS = { "Access-Control-Allow-Origin": "*" };

function reply(status: number, body?: unknown, headers: Record<string, string> = {}) {
  return new Response(body === undefined ? null : JSON.stringify(body), {
    status,
    headers: { ...CORS, "Content-Type": "application/json", ...headers },
  });
}

async function sha256(text: string) {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, "0")).join("");
}

async function ingest(req: Request) {
  let r: any;
  try {
    r = await req.json();
  } catch {
    return reply(400, { error: "not JSON" });
  }
  const fields = FIELDS[r?.kind];
  if (!fields || typeof r.install_id !== "string" || !/^[0-9a-f]{32}$/.test(r.install_id)) {
    return reply(400, { error: "unknown kind or bad install_id" });
  }
  if (typeof r.totals !== "object" || r.totals === null) return reply(400, { error: "no totals" });
  const totals: Record<string, number> = {};
  for (const [k, max] of Object.entries(fields)) {
    const v = r.totals[k];
    if (v === undefined) continue;
    if (typeof v !== "number" || !Number.isFinite(v) || v < 0 || v > max) {
      return reply(400, { error: `${k} out of range` });
    }
    totals[k] = Math.floor(v);
  }
  const day = new Date().toISOString().slice(0, 10);
  const address = (req.headers.get("x-forwarded-for") ?? "").split(",")[0].trim();
  const ipHash = await sha256(address + (Deno.env.get("STATS_SALT") ?? "laneq"));
  const { count } = await db.from("usage_reports").select("install_id", { count: "exact", head: true })
    .eq("ip_hash", ipHash).eq("day", day).neq("install_id", r.install_id);
  if ((count ?? 0) >= INSTALLS_PER_ADDRESS_PER_DAY) return reply(429, { error: "too many installs from one address today" });
  const { error } = await db.from("usage_reports").upsert({
    install_id: r.install_id, kind: r.kind, day, version: String(r.version ?? "").slice(0, 20),
    totals, ip_hash: ipHash, received_at: new Date().toISOString(),
  });
  return error ? reply(500, { error: error.message }) : reply(204);
}

const short = (n: number) =>
  n >= 1e9 ? `${(n / 1e9).toFixed(1)}B` : n >= 1e6 ? `${(n / 1e6).toFixed(1)}M` : n >= 1e4 ? `${(n / 1e3).toFixed(1)}k` : n.toLocaleString("en-US");
const bytes = (n: number) =>
  n >= 1e12 ? `${(n / 1e12).toFixed(1)} TB` : n >= 1e9 ? `${(n / 1e9).toFixed(1)} GB` : `${Math.round(n / 1e6)} MB`;

async function totals() {
  // Totals are running counts, so each install's latest report is its whole history.
  const { data, error } = await db.from("usage_reports").select("install_id,kind,day,totals")
    .order("day", { ascending: false }).limit(50000);
  if (error) return reply(500, { error: error.message });
  const active = new Date(Date.now() - 30 * 864e5).toISOString().slice(0, 10);
  const sum: Record<string, Record<string, number>> = {
    laneq: { installs: 0, active_installs: 0 },
    lanecast: { installs: 0, active_installs: 0 },
  };
  const seen = new Set<string>();
  for (const row of data ?? []) {
    const key = `${row.kind}/${row.install_id}`;
    if (seen.has(key)) continue;
    seen.add(key);
    const s = sum[row.kind];
    s.installs++;
    if (row.day >= active) s.active_installs++;
    for (const k of Object.keys(FIELDS[row.kind])) s[k] = (s[k] ?? 0) + (row.totals[k] ?? 0);
  }
  const L = sum.laneq, C = sum.lanecast;
  return reply(200, {
    laneq: L,
    lanecast: C,
    // Ready-made strings for shields.io badges.
    badges: {
      jobs: short(L.succeeded ?? 0),
      installs: short(L.installs), // every lanecast runs on a laneq, so these are the installs
      lanes: short(L.lanes ?? 0),
      laneq_data: bytes(L.data_bytes ?? 0),
      posts: short(C.posts_published ?? 0),
      accounts: short(C.accounts ?? 0),
      media: bytes(C.media_bytes ?? 0),
    },
    updated: new Date().toISOString(),
  }, { "Cache-Control": "public, max-age=900" });
}

Deno.serve((req) => {
  if (req.method === "OPTIONS") return reply(204, undefined, { "Access-Control-Allow-Methods": "GET, POST", "Access-Control-Allow-Headers": "content-type" });
  if (req.method === "GET") return totals();
  if (req.method === "POST") return ingest(req);
  return reply(405, { error: "GET or POST" });
});
