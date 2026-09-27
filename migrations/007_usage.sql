-- Usage beacons and the install facts that ride along with them.
--
-- Dozzle sends a `usage` row about once a day per install. It carries `usage`, a map of
-- counters since the previous one (view.container, action.restart, host.add.ok ...), plus
-- `locales` and a bucketed `activeMinutes`. The `events` and `start` rows gain a few maps
-- of their own: hostsByType, alertRules, destinations and labels.
--
-- As with everything else here, nothing is copied out of the JSONB: these helpers read
-- it at query time, so a counter Dozzle adds later is on the Usage page with no migration.
--
-- Usage rows are not `events` rows, so none of the continuous aggregates or the refresh
-- procedure see them: they never count as activity, never enter client_daily/weekly, and
-- need nothing from drain_refresh_analytics(). The Usage page reads `beacon` directly,
-- filtered on name = 'usage' and time, which idx_beacon_time_name_client covers and the
-- `name` compression segment prunes (migrations/005). At one row per install per day
-- that is ~35k rows a day, so even a year stays well under the size of the daily
-- aggregate the other pages read.

-- One counter out of the usage map. Absent is zero: a counter Dozzle never incremented
-- is not sent at all.
CREATE OR REPLACE FUNCTION drain_usage(m jsonb, key text) RETURNS integer
  LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT CASE WHEN jsonb_typeof(m -> 'usage' -> key) = 'number'
               THEN (m -> 'usage' ->> key)::integer ELSE 0 END $$;

-- Sum of every numeric value in an object field, e.g. all alert rules regardless of
-- kind. Anything that is not an object, or a value that is not a number, counts as zero,
-- so a malformed payload can never make a panel error.
CREATE OR REPLACE FUNCTION drain_map_sum(m jsonb, field text) RETURNS integer
  LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT COALESCE(sum((e.value #>> '{}')::numeric), 0)::integer
     FROM jsonb_each(CASE WHEN jsonb_typeof(m -> field) = 'object' THEN m -> field
                          ELSE '{}'::jsonb END) AS e
    WHERE jsonb_typeof(e.value) = 'number' $$;

-- One key of an object field, zero when absent. hostsByType -> 'agent' and friends.
CREATE OR REPLACE FUNCTION drain_map_int(m jsonb, field text, key text) RETURNS integer
  LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT CASE WHEN jsonb_typeof(m -> field -> key) = 'number'
               THEN (m -> field ->> key)::integer ELSE 0 END $$;

-- Same function as 001/006 with every existing case repeated exactly, plus the new
-- adoption flags. cloudLinked is a plain boolean and would fall through to the ELSE on
-- its own; it is named anyway so the list of what this function knows is complete.
CREATE OR REPLACE FUNCTION drain_feature(m jsonb, feature text) RETURNS boolean
  LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT CASE feature
     WHEN 'auth'          THEN COALESCE(NULLIF(m ->> 'authProvider', ''), 'none') <> 'none'
     WHEN 'multiClient'   THEN COALESCE((m ->> 'clients')::int, 0) > 1
     WHEN 'agents'        THEN COALESCE((m ->> 'remoteAgents')::int, 0) > 0
     WHEN 'uiAgents'      THEN COALESCE((m ->> 'fileAgents')::int, 0) > 0
     WHEN 'remoteSockets' THEN COALESCE((m ->> 'remoteClients')::int, 0) > 0
     WHEN 'cloudLinked'   THEN COALESCE((m ->> 'cloudLinked')::boolean, false)
     WHEN 'alertRules'    THEN drain_map_sum(m, 'alertRules') > 0
     WHEN 'autoUpdate'    THEN COALESCE(NULLIF(m ->> 'autoUpdate', ''), 'off') <> 'off'
     WHEN 'privateAgents' THEN COALESCE((m ->> 'privateAgents')::int, 0) > 0
     WHEN 'labels'        THEN drain_map_sum(m, 'labels') > 0
     WHEN 'multiUser'     THEN COALESCE(m ->> 'users', '') NOT IN ('', '1')
     ELSE COALESCE((m ->> feature)::boolean, false)
   END $$;
