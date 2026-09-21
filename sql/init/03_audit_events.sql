\connect dss150p;

BEGIN;

CREATE TABLE IF NOT EXISTS audit.pipeline_run_events (
  event_id BIGSERIAL PRIMARY KEY,
  recorded_at_utc TIMESTAMPTZ NOT NULL,
  event_type TEXT NOT NULL CHECK (event_type IN ('load', 'partition_load')),
  pipeline_run_id TEXT NOT NULL,
  partition_key TEXT,
  status TEXT NOT NULL CHECK (status IN ('loaded', 'failed')),
  rows_in INTEGER,
  rows_inserted INTEGER,
  rows_updated INTEGER,
  rows_unchanged INTEGER,
  message TEXT
);

CREATE INDEX IF NOT EXISTS idx_run_events_run ON audit.pipeline_run_events (pipeline_run_id, recorded_at_utc);

CREATE OR REPLACE FUNCTION audit.reject_event_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'audit.pipeline_run_events is append-only';
END;
$$;

DROP TRIGGER IF EXISTS trg_events_no_update_delete ON audit.pipeline_run_events;
CREATE TRIGGER trg_events_no_update_delete
  BEFORE UPDATE OR DELETE ON audit.pipeline_run_events
  FOR EACH ROW EXECUTE FUNCTION audit.reject_event_change();

DROP TRIGGER IF EXISTS trg_events_no_truncate ON audit.pipeline_run_events;
CREATE TRIGGER trg_events_no_truncate
  BEFORE TRUNCATE ON audit.pipeline_run_events
  FOR EACH STATEMENT EXECUTE FUNCTION audit.reject_event_change();

COMMIT;
