CREATE TABLE IF NOT EXISTS batch_phase (
  start_date timestamp NOT NULL,
  end_date timestamp NOT NULL CHECK (end_date > start_date),
  batch_phase text NOT NULL
);