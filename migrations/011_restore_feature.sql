-- Puts drain_feature back to its 007 definition.
--
-- 001 defines the original two-case drain_feature, and 006 and 007 replace it. Editing
-- 001 re-applies it, and re-applying it resets the function to those two cases. 006 and
-- 007 are unchanged, so the runner skips them and nothing restores the later cases. On
-- 2026-10-07 the retained-column change edited 001, and production was left with the
-- 001 version: every key outside auth and multiClient fell through to `::boolean`.
-- alertRules (a map) and autoUpdate ("weekly") cannot be cast, so the Features page
-- failed with 22P02.
--
-- This is the same body as 007. Any later edit to 001 will reset the function again, so
-- copy the newest definition into a new migration after it, as this one does.
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
