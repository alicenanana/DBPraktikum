-- Datenbankschema für "meine_media_store_db"
-- Achtung: Führe das nur aus, wenn du wirklich alle Tabellen neu erstellen willst!
-- Bestehende Tabellen müssen ggf. vorher gedroppt werden!



CREATE TABLE produkt (
    produktnr VARCHAR PRIMARY KEY,
    titel TEXT NOT NULL,
    typ VARCHAR(10) CHECK (typ IN ('CD', 'DVD', 'BUCH')),
    verkaufsrang INTEGER,
    rating FLOAT, -- ???
    bild TEXT,
    ean TEXT, --???
    detailseite TEXT,
    erscheinungsdatum DATE -- ??? siehe erscheinungsdatum in den Subklassen
);

CREATE TABLE buch ( --subklasse von Produkt
    produktnr VARCHAR PRIMARY KEY REFERENCES produkt(produktnr),
    isbn VARCHAR UNIQUE NOT NULL,
    seitenzahl INT,
    erscheinungsdatum DATE,
    verlag TEXT,
    -- autor_in TEXT, --optional, produkt_autor siehe unten für 1:n Beziehung
    auflage TEXT,
    bindung TEXT
);

CREATE TABLE cd ( --subklasse von Produkt
    produktnr VARCHAR PRIMARY KEY REFERENCES produkt(produktnr),
    label TEXT, --??
    genre TEXT, --??
    erscheinungsdatum DATE --??
);

CREATE TABLE dvd (
    produktnr VARCHAR PRIMARY KEY REFERENCES produkt(produktnr),
    format TEXT, --??
    dauer INT,
    region_code TEXT CHECK (length(region_code) <= 5), --??
    fsk TEXT,
    sprache TEXT --??
);

CREATE TABLE kuenstler (
    kuenstler_id SERIAL PRIMARY KEY,
    name TEXT --meistens unique? not null??
);

CREATE TABLE autor ( --nötig?
    autor_id SERIAL PRIMARY KEY,
    name TEXT UNIQUE --?
);

CREATE TABLE dvd_person ( -- stattdessen auch PRIMARY KEY (dvd_id, name, rolle),
    id SERIAL PRIMARY KEY,
    produktnr VARCHAR NOT NULL REFERENCES dvd(produktnr),
    name TEXT NOT NULL,
    rolle TEXT NOT NULL CHECK (rolle IN ('actor', 'director', 'creator'))
);

CREATE TABLE lied (
    lied_id SERIAL PRIMARY KEY,
    titel TEXT ,
    produktnr VARCHAR NOT NULL REFERENCES cd(produktnr),
    dauer INT,
    kuenstler_id INTEGER REFERENCES kuenstler(kuenstler_id)
);

CREATE TABLE produkt_autor (
    produktnr VARCHAR REFERENCES buch(produktnr) ON DELETE CASCADE,
    autor_id INTEGER REFERENCES autor(autor_id) ON DELETE CASCADE,
    PRIMARY KEY (produktnr, autor_id)
);

CREATE TABLE kunde (
    kunden_id VARCHAR PRIMARY KEY,
    name VARCHAR(100),
    adresse VARCHAR(255),
    email VARCHAR(100) UNIQUE,
    geburtsdatum DATE,
    kontonr VARCHAR(50) UNIQUE,
    registrierungsdatum DATE
);

CREATE TABLE rezension (
    rezension_id SERIAL PRIMARY KEY, 
    kunden_id VARCHAR REFERENCES kunde(kunden_id) ON DELETE CASCADE, --user
    produktnr VARCHAR REFERENCES produkt(produktnr) ON DELETE CASCADE, --produkt
    punkte INT CHECK (punkte BETWEEN 1 AND 5), --rating
    titel TEXT, --summary
    kommentar TEXT, --content
    hilfreich INT, --helpful
    datum DATE --reviewdate
);

CREATE TABLE filiale (
    filial_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    strasse TEXT NOT NULL,
    plz VARCHAR(10) NOT NULL,
    ort TEXT NOT NULL
);

CREATE TABLE kategorie (
    kategorie_id INT PRIMARY KEY, -- oder VARCHAR?
    name TEXT NOT NULL,
    eltern_id INT REFERENCES kategorie(kategorie_id)
);

CREATE TABLE produkt_kategorie (
    produktnr VARCHAR REFERENCES produkt(produktnr) ON DELETE CASCADE, --stimmt das denn?
    kategorie_id INT REFERENCES kategorie(kategorie_id) ON DELETE CASCADE,
    PRIMARY KEY (produktnr, kategorie_id)
);

CREATE TABLE verfuegbarkeit (
    produktnr VARCHAR REFERENCES produkt(produktnr) ON DELETE CASCADE,
    filial_id INT REFERENCES filiale(filial_id) ON DELETE CASCADE, --serial oder int?
    preis NUMERIC CHECK (preis >= 0),
    zustand TEXT,
    PRIMARY KEY (produktnr, filial_id)
);

CREATE TABLE kauf (
    kauf_id INT PRIMARY KEY,
    kunden_id VARCHAR REFERENCES kunde(kunden_id),
    zeit TIMESTAMP NOT NULL
);

CREATE TABLE kauf_position (
    kauf_id INT REFERENCES kauf(kauf_id) ON DELETE CASCADE,
    produktnr VARCHAR REFERENCES produkt(produktnr) ON DELETE CASCADE,
    menge INT NOT NULL CHECK (menge > 0),
    einzelpreis NUMERIC(10, 2) NOT NULL, -- sicher?
    PRIMARY KEY (kauf_id, produktnr)
);



-- Indexe für Performance auf JOINs
CREATE INDEX idx_rezension_produkt ON rezension(produktnr);
CREATE INDEX idx_verfuegbarkeit_produkt ON verfuegbarkeit(produktnr);
CREATE INDEX idx_kauf_kunde ON kauf(kunden_id);

-- optional
CREATE INDEX idx_lied_cd ON lied(produktnr);
CREATE INDEX idx_dvd_person_dvd ON dvd_person(produktnr);

CREATE TABLE import_errors (
    entity TEXT,
    entity_id TEXT,
    attribut TEXT,
    fehler TEXT
);