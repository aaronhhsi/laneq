-- Usage reports from laneq and lanecast installs: one row per install, kind and day
-- (a second report the same day replaces the first). Run once in the Supabase SQL editor.
create table if not exists usage_reports (
  install_id  text        not null check (install_id ~ '^[0-9a-f]{32}$'),
  kind        text        not null check (kind in ('laneq', 'lanecast')),
  day         date        not null default (now() at time zone 'utc')::date,
  version     text        not null default '' check (length(version) <= 20),
  totals      jsonb       not null,
  ip_hash     text        not null default '',
  received_at timestamptz not null default now(),
  primary key (install_id, kind, day)
);
create index if not exists usage_reports_day on usage_reports (day);
create index if not exists usage_reports_ip_day on usage_reports (ip_hash, day);

-- Row level security with no policies: the public (anon) key can't read or write it.
-- Only the stats function, which runs with the service role, touches the table.
alter table usage_reports enable row level security;
-- Newer Supabase projects don't grant API roles access to new tables on their own: the function's role needs this.
grant select, insert, update on usage_reports to service_role;
