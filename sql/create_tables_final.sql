-- Shop
CREATE TABLE shop (
    shop_id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE,
    street VARCHAR(50),zip VARCHAR(20)
);

-- IteProduktem (gemeinsam für Bücher, DVDs, Music)
CREATE TABLE item (
    shop_id INT REFERENCES shop(shop_id),
    asin VARCHAR(40) Primary Key, 
    pgroup VARCHAR(40) NOT NULL CHECK (pgroup IN ('Book' ,'DVD', 'Music','Unknown')),
    title TEXT NOT NULL,
    salesrank INT,
    picture TEXT,
    detailpage TEXT,
    ean VARCHAR(60),
    price DECIMAL(5,2),
    item_status VARCHAR(20),
    currency VARCHAR(10)
);

-- Buch
CREATE TABLE bookspec (
    asin VARCHAR(40) PRIMARY KEY REFERENCES item(asin),
    binding VARCHAR(80),
    edition VARCHAR(80),
    isbn VARCHAR(40),
    weight VARCHAR(30),
    height VARCHAR(30),
    length VARCHAR(30),
    pages INT,
    publication_date DATE
);

-- Musiks
CREATE TABLE musicspec (
    asin VARCHAR(40) PRIMARY KEY REFERENCES item(asin),
    binding VARCHAR(50)
    ,format VARCHAR(150),
    num_discs INT,
    releasedate DATE,
    upc VARCHAR(40)
);

-- DVD
CREATE TABLE dvdspec (
    asin VARCHAR(40) PRIMARY KEY REFERENCES item(asin),
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
    asin VARCHAR(40) REFERENCES item(asin),
    track_id INT REFERENCES track(track_id),
    PRIMARY KEY (asin, track_id)
);

-- Similar Products
CREATE TABLE similar_product (
    sim_id SERIAL PRIMARY KEY, 
    asin VARCHAR(40) REFERENCES item(asin),
    sim_asin VARCHAR(40),sim_title TEXT
);


-- Audiotext (Mehrsprachige Angaben)
CREATE TABLE audiotext (
    audiotext_id SERIAL PRIMARY KEY,
    lang_type VARCHAR(50),
    language VARCHAR(50),
    audioformat VARCHAR(100)
);

Create Table item_audiotext (
    asin VARCHAR(40) REFERENCES item(asin),
    audiotext_id INT REFERENCES audiotext(audiotext_id),
    PRIMARY KEY (asin, audiotext_id)
);

-- Publisher
CREATE TABLE publisher (
    publisher_id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE item_publisher (
    asin VARCHAR(20) REFERENCES item(asin),
    publisher_id INT REFERENCES publisher(publisher_id),
    PRIMARY KEY (asin, publisher_id)
    );

-- Labelf
CREATE TABLE label (
    label_id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE item_label (
    asin VARCHAR(20) REFERENCES item(asin),
    label_id INT REFERENCES label(label_id),
    PRIMARY KEY (asin, label_id)
);

-- Studio
CREATE TABLE studio (
    studio_id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE item_studio (
    asin VARCHAR(20) REFERENCES item(asin),
    studio_id INT REFERENCES studio(studio_id),
    PRIMARY KEY (asin, studio_id)
);

-- Listmania
CREATE TABLE listmania (
    list_id SERIAL PRIMARY KEY,
    name TEXT
);

CREATE TABLE item_listmania (
    asin VARCHAR(20) REFERENCES item(asin),
    list_id INT REFERENCES listmania(list_id),
    PRIMARY KEY (asin, list_id)
);

-- Person rolle nur bei item_person
Create table person (
    person_id Serial Primary Key, 
    name Varchar (100)
);

Create table item_person (
    asin VARCHAR(40) REFERENCES item(asin),
    person_id INT REFERENCES person (person_id), 
    person_role Varchar(20),
    PRIMARY KEY (asin, person_id)
);

CREATE TABLE kategorie (
    kategorie_id VARCHAR(40) PRIMARY KEY,
    name TEXT NOT NULL,
    eltern_id INT REFERENCES kategorie(kategorie_id)
);

CREATE TABLE item_kategorie (
    asin VARCHAR(40) REFERENCES item(asin),
    kategorie_id INT REFERENCES kategorie(kategorie_id),
    PRIMARY KEY (asin, kategorie_id)
);


CREATE TABLE illegal_data (
    illegal_id SERIAL PRIMARY KEY,
    asin VARCHAR(40),
    pgroup VARCHAR(40),
    title TEXT,
    error_message TEXT,
    import_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
