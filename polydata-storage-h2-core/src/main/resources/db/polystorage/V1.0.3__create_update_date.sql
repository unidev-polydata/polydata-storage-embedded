ALTER TABLE data ADD  create_date TIMESTAMP ;
UPDATE data SET create_date = CURRENT_TIMESTAMP();

ALTER TABLE data ADD  update_date TIMESTAMP ;
UPDATE data SET update_date = CURRENT_TIMESTAMP();

CREATE INDEX update_date_idx ON data (update_date);