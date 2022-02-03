DROP TABLE IF EXISTS country_info;

CREATE TABLE country_info (
  "Country Code" INTEGER,
  "Name" VARCHAR(100),
  "ISO 3166" VARCHAR(10)
);
INSERT INTO country_info values ('1', 'Bolivia, Plurinational State of', 'BO');
INSERT INTO country_info values ('2', 'Ireland', 'IE');
INSERT INTO country_info values ('3', 'Saint Martin (French part)', 'MF');
