-- Created automatically by server.js on start (CREATE IF NOT EXISTS). Kept here for reference.
CREATE TABLE IF NOT EXISTS players (
  id          text PRIMARY KEY,
  nick        text NOT NULL,
  secret_hash text NOT NULL,
  created     timestamptz DEFAULT now(),
  last_seen   timestamptz DEFAULT now()
);
CREATE TABLE IF NOT EXISTS scores (
  player_id text REFERENCES players(id) ON DELETE CASCADE,
  board     text NOT NULL,            -- 'stunt' (higher is better) | 'trial' (lower is better)
  region    text NOT NULL,
  value     double precision NOT NULL,
  updated   timestamptz DEFAULT now(),
  PRIMARY KEY (player_id, board, region)
);
CREATE INDEX IF NOT EXISTS scores_board_idx ON scores (board, region, value);
