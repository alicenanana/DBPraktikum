-- =========================================================
-- Media-Store – CREATE TABLES (final)
-- =========================================================

-- safety first: alles in definierter Reihenfolge droppen
DROP TABLE IF EXISTS item_person        CASCADE;
DROP TABLE IF EXISTS item_kategorie     CASCADE;
DROP TABLE IF EXISTS item_publisher     CASCADE;
DROP TABLE IF EXISTS item_label         CASCADE;
DROP TABLE IF EXISTS item_studio        CASCADE;
DROP TABLE IF EXISTS item_listmania     CASCADE;
DROP TABLE IF EXISTS similar_product    CASCADE;
DROP TABLE IF EXISTS angebot            CASCADE;
DROP TABLE IF EXISTS rezension          CASCADE;

DROP TABLE IF EXISTS book_spec          CASCADE;
DROP TABLE IF EXISTS music_spec         CASCADE;
DROP TABLE IF EXISTS dvd_spec           CASCADE;

DROP TABLE IF EXISTS item               CASCADE;
DROP TABLE IF EXISTS kategorie          CASCADE;
DROP TABLE IF EXISTS person             CASCADE;
DROP TABLE IF EXISTS publisher          CASCADE;
DROP TABLE IF EXISTS label              CASCADE;
DROP TABLE IF EXISTS studio             CASCADE;
DROP TABLE IF EXISTS listmania          CASCADE;
DROP TABLE IF EXISTS kunde              CASCADE;
DROP TABLE IF EXISTS shop               CASCADE;

-- =========================================================
-- Stammtabellen
-- =========================================================
CREATE TABLE shop (
  shop_id   SERIAL PRIMARY KEY,
  name      TEXT NOT NULL UNIQUE
);

CREATE TABLE kunde (
  kunden_id SERIAL PRIMARY KEY,
  username  TEXT NOT NULL UNIQUE,
  email     TEXT
);

CREATE TABLE person (
  person_id SERIAL PRIMARY KEY,
  name      TEXT NOT NULL
);

CREATE TABLE publisher (
  publisher_id SERIAL PRIMARY KEY,
  name         TEXT NOT NULL UNIQUE
);

CREATE TABLE label (
  label_id SERIAL PRIMARY KEY,
  name     TEXT NOT NULL UNIQUE
);

CREATE TABLE studio (
  studio_id SERIAL PRIMARY KEY,
  name      TEXT NOT NULL UNIQUE
);

CREATE TABLE listmania (
  list_id SERIAL PRIMARY KEY,
  name    TEXT NOT NULL
);

-- Kategorien-Baum (Self-Reference)
CREATE TABLE kategorie (
  kategorie_id SERIAL PRIMARY KEY,
  parent_id    INT REFERENCES kategorie(kategorie_id) ON DELETE CASCADE,
  name         TEXT NOT NULL
);

-- =========================================================
-- Kernobjekt: Item/Produkt
-- =========================================================
CREATE TABLE item (
  asin            VARCHAR(40) PRIMARY KEY,
  shop_id         INT REFERENCES shop(shop_id) ON DELETE SET NULL,
  pgroup          VARCHAR(40) NOT NULL CHECK (pgroup IN ('Book','DVD','Music')),
  title           TEXT NOT NULL,
  salesrank       INT,
  picture         TEXT,
  detailpage      TEXT,
  ean             VARCHAR(60),
  price           NUMERIC(10,2),
  currency        VARCHAR(10) NOT NULL,
  item_status     VARCHAR(20),

  -- WICHTIG für Auswertungen/Trigger:
  rating          REAL,
  rating_counter  INT DEFAULT 0
);

-- Produktspezifische 1:1-Details (optional, nach Bedarf befüllen)
CREATE TABLE book_spec (
  asin  VARCHAR(40) PRIMARY KEY REFERENCES item(asin) ON DELETE CASCADE,
  isbn  VARCHAR(40),
  pages INT
);

CREATE TABLE music_spec (
  asin   VARCHAR(40) PRIMARY KEY REFERENCES item(asin) ON DELETE CASCADE,
  media  TEXT,
  year   INT
);

CREATE TABLE dvd_spec (
  asin        VARCHAR(40) PRIMARY KEY REFERENCES item(asin) ON DELETE CASCADE,
  region_code INT,
  runtime_min INT
);

-- Zuordnungen
CREATE TABLE item_kategorie (
  asin         VARCHAR(40) REFERENCES item(asin) ON DELETE CASCADE,
  kategorie_id INT         REFERENCES kategorie(kategorie_id) ON DELETE CASCADE,
  PRIMARY KEY (asin, kategorie_id)
);

CREATE TABLE item_publisher (
  asin         VARCHAR(40) REFERENCES item(asin) ON DELETE CASCADE,
  publisher_id INT         REFERENCES publisher(publisher_id) ON DELETE CASCADE,
  PRIMARY KEY (asin, publisher_id)
);

CREATE TABLE item_label (
  asin      VARCHAR(40) REFERENCES item(asin) ON DELETE CASCADE,
  label_id  INT         REFERENCES label(label_id) ON DELETE CASCADE,
  PRIMARY KEY (asin, label_id)
);

CREATE TABLE item_studio (
  asin       VARCHAR(40) REFERENCES item(asin) ON DELETE CASCADE,
  studio_id  INT         REFERENCES studio(studio_id) ON DELETE CASCADE,
  PRIMARY KEY (asin, studio_id)
);

CREATE TABLE item_listmania (
  asin     VARCHAR(40) REFERENCES item(asin) ON DELETE CASCADE,
  list_id  INT         REFERENCES listmania(list_id) ON DELETE CASCADE,
  PRIMARY KEY (asin, list_id)
);

-- Ähnliche Produkte (symmetrisch optional – hier einfache gerichtete Kante)
CREATE TABLE similar_product (
  asin        VARCHAR(40) REFERENCES item(asin) ON DELETE CASCADE,
  similar_asin VARCHAR(40) REFERENCES item(asin) ON DELETE CASCADE,
  PRIMARY KEY (asin, similar_asin)
);

-- Personen-Beteiligung am Item (mit Rolle)
CREATE TABLE item_person (
  asin        VARCHAR(40) REFERENCES item(asin) ON DELETE CASCADE,
  person_id   INT         REFERENCES person(person_id) ON DELETE CASCADE,
  person_role TEXT NOT NULL,                -- z.B. 'AUTHOR', 'ARTIST', 'ACTOR', ...
  PRIMARY KEY (asin, person_id, person_role)
);

-- Angebote pro Shop und Item (zusammengesetzter Schlüssel)
CREATE TABLE angebot (
  shop_id   INT         REFERENCES shop(shop_id) ON DELETE CASCADE,
  asin      VARCHAR(40) REFERENCES item(asin)    ON DELETE CASCADE,
  price     NUMERIC(10,2),
  currency  VARCHAR(10),
  condition TEXT,                              -- z.B. 'new', 'used'
  available BOOLEAN DEFAULT TRUE,
  PRIMARY KEY (shop_id, asin)
);

-- Rezensionen der Kunden
CREATE TABLE rezension (
  rezension_id    SERIAL PRIMARY KEY,
  kunden_id       INT         REFERENCES kunde(kunden_id) ON DELETE CASCADE,
  asin            VARCHAR(40) REFERENCES item(asin) ON DELETE CASCADE,
  bewertung       INT CHECK (bewertung BETWEEN 1 AND 5),
  titel           TEXT,                         -- WICHTIG: wird von Import/2b genutzt
  text            TEXT,
  rezensionsdatum TIMESTAMP
);

-- sinnvolle Indizes
CREATE INDEX idx_item_title        ON item USING gin (to_tsvector('simple', coalesce(title,'')));
CREATE INDEX idx_item_pgroup       ON item (pgroup);
CREATE INDEX idx_item_price        ON item (price);
CREATE INDEX idx_rezension_asin    ON rezension (asin);
CREATE INDEX idx_rezension_kunde   ON rezension (kunden_id);
CREATE INDEX idx_kategorie_parent  ON kategorie (parent_id);
CREATE INDEX idx_angebot_price     ON angebot (price);
