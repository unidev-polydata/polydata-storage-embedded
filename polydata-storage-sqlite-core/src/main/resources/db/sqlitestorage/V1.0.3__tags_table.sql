ALTER TABLE data ADD  create_date datetime ;
UPDATE data SET create_date = datetime();

ALTER TABLE data ADD  update_date datetime ;
UPDATE data SET update_date = datetime();