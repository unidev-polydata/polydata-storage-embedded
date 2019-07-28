CREATE TABLE poly_index (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  container TEXT,
  _id VARCHAR(255),
  data JSON,
  create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP);

CREATE INDEX tag_index_id_idx ON poly_index (container, _id);
CREATE INDEX tag_index_update_date_idx ON poly_index(update_date);
