CREATE TABLE tags (
  id INTEGER PRIMARY KEY auto_increment,
  _id VARCHAR(255),
  count INTEGER,
  data TEXT
);

CREATE INDEX tag_id_idx ON tags (_id);
