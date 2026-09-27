-- Features that are counts rather than flags.
--
-- remoteAgents, remoteClients and fileAgents are how many agents / remote sockets an
-- install connects to, so "on" is "more than zero". They were only ever sent on the
-- `start` beacon, which is why they read as zero on the `events` rows the dashboard
-- uses; newer Dozzle sends them on every beacon.
--
-- Same function as 001, so every existing case is repeated exactly. CREATE OR REPLACE
-- keeps this idempotent, and nothing indexes drain_feature, so replacing it needs no
-- rebuild.
CREATE OR REPLACE FUNCTION drain_feature(m jsonb, feature text) RETURNS boolean
  LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT CASE feature
     WHEN 'auth'          THEN COALESCE(NULLIF(m ->> 'authProvider', ''), 'none') <> 'none'
     WHEN 'multiClient'   THEN COALESCE((m ->> 'clients')::int, 0) > 1
     WHEN 'agents'        THEN COALESCE((m ->> 'remoteAgents')::int, 0) > 0
     WHEN 'uiAgents'      THEN COALESCE((m ->> 'fileAgents')::int, 0) > 0
     WHEN 'remoteSockets' THEN COALESCE((m ->> 'remoteClients')::int, 0) > 0
     ELSE COALESCE((m ->> feature)::boolean, false)
   END $$;
