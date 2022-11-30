CREATE TABLE IF NOT EXISTS batch_info (
  start_date timestamp NOT NULL,
  end_date timestamp NOT NULL CHECK (end_date > start_date),
  batch_id text
);