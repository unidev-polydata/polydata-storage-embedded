CREATE TABLE tags (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  _id TEXT,
  count INTEGER,
  data JSON
);

CREATE INDEX categories_id_idx ON tags (_id);