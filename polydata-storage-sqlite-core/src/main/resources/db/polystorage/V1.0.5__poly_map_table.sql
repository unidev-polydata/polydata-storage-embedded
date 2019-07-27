CREATE TABLE poly_map (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  container TEXT,
  _id TEXT,
  data JSON
);

CREATE INDEX poly_map_id_idx ON tags (container, _id);