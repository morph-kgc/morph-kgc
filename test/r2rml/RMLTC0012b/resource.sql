DROP TABLE IF EXISTS persons;

CREATE TABLE persons (
  "fname" VARCHAR(200),
  "lname" VARCHAR(200),
  "amount" INTEGER
);
INSERT INTO persons values ('Bob','Smith','30');
INSERT INTO persons values ('Sue','Jones','20');
INSERT INTO persons values ('Bob','Smith','30');

DROP TABLE IF EXISTS lives;

CREATE TABLE lives (
  "fname" VARCHAR(200),
  "lname" VARCHAR(200),
  "city" VARCHAR(200)
);
INSERT INTO lives values ('Bob','Smith','London');
INSERT INTO lives values ('Sue','Jones','Madrid');
INSERT INTO lives values ('Bob','Smith','London');
