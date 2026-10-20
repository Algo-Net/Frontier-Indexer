CREATE TABLE IF NOT EXISTS events_extension_frozen (
  event_id    VARCHAR(100)  NOT NULL,
  occurred_at TIMESTAMPTZ   NOT NULL,
  id          VARCHAR(66)   NOT NULL,
  PRIMARY KEY (event_id, occurred_at, id)
);

SELECT public.create_hypertable('events_extension_frozen', 'occurred_at');
