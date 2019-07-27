CREATE TABLE metadata (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  container TEXT,
  _id TEXT,
  data JSON
);

CREATE INDEX _id_metadata_idx ON metadata (container, _id);
