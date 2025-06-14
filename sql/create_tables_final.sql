-- Shop
CREATE TABLE shop (
    shop_id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE,
    street VARCHAR(50),
    zip VARCHAR(20)
);

-- Item (gemeinsam für Bücher, DVDs, Music)
CREATE TABLE item (
    shop_id INT REFERENCES shop(shop_id) ON DELETE SET NULL,
    asin VARCHAR(40) PRIMARY KEY,
    pgroup VARCHAR(40) NOT NULL CHECK (pgroup IN ('Book', 'DVD', 'Music')),
    title TEXT NOT NULL,
    salesrank INT,
    picture TEXT,
    detailpage TEXT,
    ean VARCHAR(60),
    price DECIMAL(5,2),
    item_status VARCHAR(20),
    currency VARCHAR(10) NOT NULL
);

-- Buch
CREATE TABLE bookspec (
    asin VARCHAR(40) PRIMARY KEY REFERENCES item(asin) ON DELETE CASCADE,
    binding VARCHAR(80),
    edition VARCHAR(80),
    isbn VARCHAR(40) NOT NULL,
    weight VARCHAR(30),
    height VARCHAR(30),
    length VARCHAR(30),
    pages INT,
    publication_date DATE
);

-- Musiks
CREATE TABLE musicspec (
    asin VARCHAR(40) PRIMARY KEY REFERENCES item(asin) ON DELETE CASCADE,
    binding VARCHAR(50),
    format VARCHAR(150),
    num_discs INT,
    releasedate DATE,
    upc VARCHAR(40)
);

-- DVD
CREATE TABLE dvdspec (
    asin VARCHAR(40) PRIMARY KEY REFERENCES item(asin) ON DELETE CASCADE,
    aspectratio VARCHAR(80),
    format VARCHAR(100),
    regioncode INT,
    releasedate DATE,
    runningtime INT,
    theatr_release INT,
    upc VARCHAR(60)
);

-- Tracks (für Music-CDs)
CREATE TABLE track (
    track_id SERIAL PRIMARY KEY,
    name TEXT
);

CREATE TABLE item_track (
    asin VARCHAR(40) REFERENCES item(asin) ON DELETE CASCADE,
    track_id INT REFERENCES track(track_id) ON DELETE CASCADE,
    PRIMARY KEY (asin, track_id)
);

-- Similar Products
CREATE TABLE similar_product (
    sim_id SERIAL PRIMARY KEY,
    asin VARCHAR(40) REFERENCES item(asin) ON DELETE CASCADE,
    sim_asin VARCHAR(40)
);

-- Audiotext (Mehrsprachige Angaben)
CREATE TABLE audiotext (
    audiotext_id SERIAL PRIMARY KEY,
    lang_type VARCHAR(50),
    language VARCHAR(50),
    audioformat VARCHAR(100)
);

CREATE TABLE item_audiotext (
    asin VARCHAR(40) REFERENCES item(asin) ON DELETE CASCADE,
    audiotext_id INT REFERENCES audiotext(audiotext_id) ON DELETE CASCADE,
    PRIMARY KEY (asin, audiotext_id)
);

-- Publisher
CREATE TABLE publisher (
    publisher_id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE item_publisher (
    asin VARCHAR(20) REFERENCES item(asin) ON DELETE CASCADE,
    publisher_id INT REFERENCES publisher(publisher_id) ON DELETE CASCADE,
    PRIMARY KEY (asin, publisher_id)
);

-- Label
CREATE TABLE label (
    label_id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE item_label (
    asin VARCHAR(20) REFERENCES item(asin) ON DELETE CASCADE,
    label_id INT REFERENCES label(label_id) ON DELETE CASCADE,
    PRIMARY KEY (asin, label_id)
);

-- Studio
CREATE TABLE studio (
    studio_id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE item_studio (
    asin VARCHAR(20) REFERENCES item(asin) ON DELETE CASCADE,
    studio_id INT REFERENCES studio(studio_id) ON DELETE CASCADE,
    PRIMARY KEY (asin, studio_id)
);

-- Listmania
CREATE TABLE listmania (
    list_id SERIAL PRIMARY KEY,
    name TEXT
);

CREATE TABLE item_listmania (
    asin VARCHAR(20) REFERENCES item(asin) ON DELETE CASCADE,
    list_id INT REFERENCES listmania(list_id) ON DELETE CASCADE,
    PRIMARY KEY (asin, list_id)
);

-- Person rolle nur bei item_person
CREATE TABLE person (
    person_id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE item_person (
    asin VARCHAR(40) REFERENCES item(asin) ON DELETE CASCADE,
    person_id INT REFERENCES person(person_id) ON DELETE CASCADE,
    person_role VARCHAR(20),
    PRIMARY KEY (asin, person_id)
);

-- Kategorie
CREATE TABLE kategorie (
    kategorie_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    eltern_id INT REFERENCES kategorie(kategorie_id) ON DELETE CASCADE
);

CREATE TABLE item_kategorie (
    asin VARCHAR(40) REFERENCES item(asin) ON DELETE CASCADE,
    kategorie_id INT REFERENCES kategorie(kategorie_id) ON DELETE SET NULL,
    PRIMARY KEY (asin, kategorie_id)
);

-- Illegal Data
CREATE TABLE illegal_data (
    illegal_id SERIAL PRIMARY KEY,
    asin VARCHAR(40),
    pgroup VARCHAR(40),
    title TEXT,
    error_message TEXT,
    import_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Kunden und Bestellungen
CREATE TABLE kunde (
    kunden_id SERIAL PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    mail TEXT UNIQUE,
    adresse TEXT,
    konto_nr TEXT
);

CREATE TABLE bestellung (
    bestellung_id SERIAL PRIMARY KEY,
    kunden_id INT REFERENCES kunde(kunden_id) ON DELETE CASCADE,
    kaufdatum TIMESTAMP
);

CREATE TABLE bestellposition (
    bestellung_id INT REFERENCES bestellung(bestellung_id) ON DELETE CASCADE,
    asin VARCHAR(40) REFERENCES item(asin) ON DELETE CASCADE,
    PRIMARY KEY (bestellung_id, asin)
);

CREATE TABLE rezension (
    rezension_id SERIAL PRIMARY KEY,
    kunden_id INT REFERENCES kunde(kunden_id) ON DELETE CASCADE,
    asin VARCHAR(40) REFERENCES item(asin) ON DELETE CASCADE,
    bewertung INT CHECK (bewertung BETWEEN 1 AND 5),
    text TEXT,
    rezensionsdatum TIMESTAMP
);

-- Angebot je Shop
CREATE TABLE angebot (
    shop_id INT REFERENCES shop(shop_id) ON DELETE CASCADE,
    asin VARCHAR(40) REFERENCES item(asin) ON DELETE CASCADE,
    preis DECIMAL(5,2),
    verfuegbar BOOLEAN,
    zustand TEXT,
    PRIMARY KEY (shop_id, asin)
);
