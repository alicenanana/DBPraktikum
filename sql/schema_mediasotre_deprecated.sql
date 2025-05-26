
-- =======================
-- Media Store Datenbankschema
-- =======================

CREATE TABLE Produkt (
    produktnr SERIAL PRIMARY KEY,
    titel VARCHAR(255) NOT NULL,
    verkaufsrang INTEGER,
    rating DECIMAL, -- Berechnet aus Rezensionen
    typ VARCHAR(20),
    bild TEXT
);

CREATE TABLE Buch (
    produktnr INTEGER PRIMARY KEY REFERENCES Produkt(produktnr),
    isbn VARCHAR(20) UNIQUE NOT NULL,
    autor_in VARCHAR(100) NOT NULL,
    seitenzahl INTEGER,
    erscheinungsdatum DATE,
    verlag VARCHAR(100)
);

CREATE TABLE CD (
    produktnr INTEGER PRIMARY KEY REFERENCES Produkt(produktnr),
    label VARCHAR(100),
    erscheinungsdatum DATE
);

CREATE TABLE DVD (
    produktnr INTEGER PRIMARY KEY REFERENCES Produkt(produktnr),
    format VARCHAR(50),
    laufzeit INTEGER,
    region_code CHAR(5) CHECK (char_length(region_code) = 5)
);

CREATE TABLE Lied (
    lied_id SERIAL PRIMARY KEY,
    produktnr INTEGER REFERENCES CD(produktnr),
    name VARCHAR(100) NOT NULL
);

CREATE TABLE Kunde (
    kunden_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    adresse TEXT NOT NULL,
    kontonr VARCHAR(30) NOT NULL UNIQUE
);

CREATE TABLE Rezension (
    rezension_id SERIAL PRIMARY KEY,
    kunden_id INTEGER NOT NULL REFERENCES Kunde(kunden_id),
    produktnr INTEGER NOT NULL REFERENCES Produkt(produktnr),
    punkte INTEGER NOT NULL CHECK (punkte BETWEEN 1 AND 5),
    kommentar TEXT
);

CREATE TABLE Filiale (
    filial_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    anschrift TEXT NOT NULL
);

CREATE TABLE Kategorie (
    kategorie_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    eltern_id INTEGER REFERENCES Kategorie(kategorie_id)
);

CREATE TABLE Kauf (
    kauf_id SERIAL PRIMARY KEY,
    kunden_id INTEGER NOT NULL REFERENCES Kunde(kunden_id),
    zeit TIMESTAMP NOT NULL
);

CREATE TABLE DVD_Person (
    dvd_id INTEGER REFERENCES DVD(produktnr),
    name VARCHAR(100) NOT NULL,
    rolle VARCHAR(50) NOT NULL CHECK (rolle IN ('Actor', 'Director', 'Creator')),
    PRIMARY KEY (dvd_id, name, rolle)
);

CREATE TABLE Kuenstler_in (
    kuenstler_in_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);

-- Zwischentabellen (n:m)
CREATE TABLE aehnlich (
    produktnr1 INTEGER REFERENCES Produkt(produktnr),
    produktnr2 INTEGER REFERENCES Produkt(produktnr),
    PRIMARY KEY (produktnr1, produktnr2)
);

CREATE TABLE Produkt_Kategorie (
    produktnr INTEGER REFERENCES Produkt(produktnr),
    kategorie_id INTEGER REFERENCES Kategorie(kategorie_id),
    PRIMARY KEY (produktnr, kategorie_id)
);

CREATE TABLE CD_Kuenstler_in (
    produktnr INTEGER REFERENCES CD(produktnr),
    kuenstler_in_id INTEGER REFERENCES Kuenstler_in(kuenstler_in_id),
    PRIMARY KEY (produktnr, kuenstler_in_id)
);
