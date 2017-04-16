CREATE TABLE tag_index (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
_id VARCHAR(255),
data JSON,
create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP);

CREATE INDEX tag_index_id_idx ON tag_index (_id);
CREATE INDEX tag_index_update_date_idx ON tag_index(update_date);
