-- 003_schedule.sql
--
-- Runs drain_refresh_analytics() on TimescaleDB's own job scheduler - the same one that
-- already runs the compression, retention and continuous-aggregate policies from
-- 01_init.sql and 001.
--
-- This replaces the `scheduler` and `view-refresh` services: an ofelia daemon holding the
-- Docker socket, plus a second container idling on `tail -f /dev/null` to keep a psql
-- binary alive, both of them so one statement could be issued hourly from outside the
-- database. The socket mount is effectively root on the host, and ofelia's command label
-- embedded POSTGRES_PASSWORD where `docker service inspect` could read it back.
--
-- Idempotent: re-applying re-points the existing job rather than registering a second one.

-- A user-defined action must take exactly (job_id integer, config jsonb), so the scheduler
-- cannot call drain_refresh_analytics(boolean) directly. The wrapper also gives the job
-- somewhere to carry `full`, so a scheduled rebuild is
-- `SELECT alter_job(<id>, config => '{"full": true}')` rather than an edit here.
--
-- The inner CALL commits several times, because 002 runs each phase in its own
-- transaction. That is allowed because TimescaleDB invokes a user-defined action in a
-- non-atomic context and plpgsql propagates that through CALL. It is also why this body
-- must never grow an EXCEPTION block: that would make the block atomic and turn the first
-- COMMIT into a runtime error.
CREATE OR REPLACE PROCEDURE drain_refresh_job(job_id integer, config jsonb)
LANGUAGE plpgsql AS $proc$
BEGIN
  CALL drain_refresh_analytics(COALESCE((config ->> 'full')::boolean, false));
END;
$proc$;

-- add_job has no `if_not_exists` and nothing about the procedure identifies the job, so
-- calling it twice quietly registers a second copy running the same refresh on its own
-- schedule. Look the job up by procedure name instead.
DO $$
DECLARE
  v_job integer;
BEGIN
  SELECT job_id INTO v_job
  FROM timescaledb_information.jobs
  WHERE proc_schema = 'public' AND proc_name = 'drain_refresh_job';

  IF v_job IS NULL THEN
    -- fixed_schedule => false measures the hour from the end of the last run, so a
    -- refresh that overruns delays the next one instead of having it start immediately.
    PERFORM add_job('drain_refresh_job', INTERVAL '1 hour', fixed_schedule => false);
  ELSE
    PERFORM alter_job(v_job, schedule_interval => INTERVAL '1 hour');
  END IF;
END $$;
