-- Produkte (allgemein)
CREATE TABLE Produkt (
    produkt_id SERIAL PRIMARY KEY,
    titel TEXT NOT NULL,
    verkaufsrang INTEGER,
    rating NUMERIC(2,1),
    typ TEXT NOT NULL CHECK (typ IN ('Buch', 'DVD', 'CD'))
);

-- Bücher
CREATE TABLE Buch (
    produkt_id INTEGER PRIMARY KEY REFERENCES Produkt(produkt_id),
    autor TEXT NOT NULL,
    seitenzahl INTEGER,
    erscheinungsdatum DATE,
    isbn TEXT UNIQUE,
    verlag TEXT
);

-- DVDs
CREATE TABLE DVD (
    produkt_id INTEGER PRIMARY KEY REFERENCES Produkt(produkt_id),
    format TEXT,
    laufzeit INTEGER,
    region_code TEXT
);

-- Personen zu DVDs
CREATE TABLE DVD_Person (
    dvd_id INTEGER REFERENCES DVD(produkt_id),
    name TEXT,
    rolle TEXT CHECK (rolle IN ('Actor', 'Director', 'Creator')),
    PRIMARY KEY (dvd_id, name, rolle)
);

-- CDs
CREATE TABLE CD (
    produkt_id INTEGER PRIMARY KEY REFERENCES Produkt(produkt_id),
    label TEXT,
    erscheinungsdatum DATE
);

-- Titel auf CDs
CREATE TABLE CD_Titel (
    cd_id INTEGER REFERENCES CD(produkt_id),
    track_nr INTEGER,
    name TEXT,
    PRIMARY KEY (cd_id, track_nr)
);

-- Künstler auf CDs
CREATE TABLE CD_Kuenstler (
    cd_id INTEGER REFERENCES CD(produkt_id),
    kuenstler TEXT,
    PRIMARY KEY (cd_id, kuenstler)
);

-- Kategorien (hierarchisch)
CREATE TABLE Kategorie (
    kategorie_id SERIAL PRIMARY KEY,
    name TEXT,
    parent_id INTEGER REFERENCES Kategorie(kategorie_id)
);

-- Produkt-Kategorie-Zuordnung
CREATE TABLE Produkt_Kategorie (
    produkt_id INTEGER REFERENCES Produkt(produkt_id),
    kategorie_id INTEGER REFERENCES Kategorie(kategorie_id),
    PRIMARY KEY (produkt_id, kategorie_id)
);

-- Ähnliche Produkte
CREATE TABLE Aehnlich (
    produkt_id1 INTEGER REFERENCES Produkt(produkt_id),
    produkt_id2 INTEGER REFERENCES Produkt(produkt_id),
    PRIMARY KEY (produkt_id1, produkt_id2),
    CHECK (produkt_id1 <> produkt_id2)
);

-- Fillialen
CREATE TABLE Filliale (
    filliale_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    anschrift TEXT
);

-- Angebote
CREATE TABLE Angebot (
    produkt_id INTEGER REFERENCES Produkt(produkt_id),
    filliale_id INTEGER REFERENCES Filliale(filliale_id),
    preis NUMERIC(6,2),
    zustand TEXT,
    verfuegbar BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (produkt_id, filliale_id)
);

-- Kunden
CREATE TABLE Kunde (
    kunde_id SERIAL PRIMARY KEY,
    name TEXT,
    kontonummer TEXT,
    lieferadresse TEXT
);

-- Käufe
CREATE TABLE Kauf (
    kauf_id SERIAL PRIMARY KEY,
    kunde_id INTEGER REFERENCES Kunde(kunde_id),
    zeitpunkt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Kaufdetails
CREATE TABLE Kauf_Produkt (
    kauf_id INTEGER REFERENCES Kauf(kauf_id),
    produkt_id INTEGER REFERENCES Produkt(produkt_id),
    preis_zum_zeitpunkt NUMERIC(6,2),
    PRIMARY KEY (kauf_id, produkt_id)
);

-- Rezensionen
CREATE TABLE Rezension (
    rezension_id SERIAL PRIMARY KEY,
    kunde_id INTEGER REFERENCES Kunde(kunde_id),
    produkt_id INTEGER REFERENCES Produkt(produkt_id),
    punkte INTEGER CHECK (punkte BETWEEN 1 AND 5),
    text TEXT
);

-- INDEXE zur Performance
CREATE INDEX idx_produkt_typ ON Produkt(typ);
CREATE INDEX idx_rating ON Produkt(rating);
CREATE INDEX idx_kategorie_name ON Kategorie(name);
CREATE INDEX idx_angebot_preis ON Angebot(preis);
CREATE INDEX idx_rezension_punkte ON Rezension(punkte);
