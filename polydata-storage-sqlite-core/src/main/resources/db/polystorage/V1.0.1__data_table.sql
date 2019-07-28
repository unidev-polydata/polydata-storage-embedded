CREATE TABLE data (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  container TEXT,
  _type TEXT,
  _id TEXT,
  data JSON,
  create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX _id_idx ON data (container, _type, _id);
CREATE INDEX update_date_idx ON data (update_date);