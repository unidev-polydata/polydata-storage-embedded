CREATE TABLE data (
  id INTEGER PRIMARY KEY auto_increment,
  _id VARCHAR(255),
  tags TEXT,
  data TEXT
);

CREATE INDEX _id_idx ON data (_id);