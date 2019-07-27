CREATE TABLE poly_list (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  container TEXT,
  _id TEXT,
  data JSON
);

CREATE INDEX poly_list_id_idx ON tags (container, _id);