CREATE TABLE metadata (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  _id TEXT,
  data JSON
);

CREATE INDEX _id_metadata_idx ON metadata (_id);
