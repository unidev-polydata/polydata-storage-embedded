CREATE TABLE data (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  container TEXT,
  _id TEXT,
  data JSON
);

CREATE INDEX _id_idx ON data (container, _id);
CREATE INDEX _tag_idx ON data (container, tags);