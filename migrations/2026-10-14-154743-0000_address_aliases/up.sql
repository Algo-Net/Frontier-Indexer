CREATE TABLE IF NOT EXISTS address_aliases (
  address             VARCHAR(66)   NOT NULL,
  alias               VARCHAR(66)   NOT NULL,
  checkpoint_updated  BIGINT        NOT NULL,
  PRIMARY KEY (address, alias)
);
