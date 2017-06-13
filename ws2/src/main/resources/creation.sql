create database spk;

use spk;

CREATE TABLE stat_licences (
  code INT NOT NULL,
  libelle VARCHAR(128) NOT NULL,
  nb INT NOT NULL,
  CONSTRAINT pk_code PRIMARY KEY (code)
);