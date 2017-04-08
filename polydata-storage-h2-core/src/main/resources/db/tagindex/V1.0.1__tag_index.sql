CREATE TABLE tag_index (
id INTEGER PRIMARY KEY auto_increment,
_id VARCHAR(255),
tag TEXT,
data TEXT,
create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP);

CREATE INDEX tag_index_id_idx ON tag_index (_id);
CREATE INDEX tag_index_tag_idx ON tag_index (tag);
CREATE INDEX tag_index_update_date_idx ON tag_index(update_date);
