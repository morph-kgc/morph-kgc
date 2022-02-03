DROP TABLE IF EXISTS student_sport;
DROP TABLE IF EXISTS student;
DROP TABLE IF EXISTS sport;

CREATE TABLE sport (
  ID INTEGER,
  Description VARCHAR(200)
);
INSERT INTO sport values ('110','Tennis');
INSERT INTO sport values ('111','Football');
INSERT INTO sport values ('112','Formula1');


CREATE TABLE student (
  ID INTEGER,
  FirstName VARCHAR(200),
  LastName VARCHAR(200)
);
INSERT INTO student values ('10','Venus','Williams');
INSERT INTO student values ('11','Fernando','Alonso');
INSERT INTO student values ('12','David','Villa');


CREATE TABLE student_sport (
  ID_Student INTEGER,
  ID_Sport INTEGER
);
INSERT INTO student_sport values ('10', '110');
INSERT INTO student_sport values ('11','111');
INSERT INTO student_sport values ('11','112');
INSERT INTO student_sport values ('12','111');
