# Aufgabe 2a
Hinweis: Diese Datei verwendet Markdown-Formatierung zur besseren Lesbarkeit.
Alle SQL-Statements sind nummeriert und mit Ergebnissen versehen, sofern die Ergebnistabellen überschaubar waren. 
Bei umfangreicheren Ergebnissen haben wir stattdessen die Anzahl der Datensätze angegeben. 
Vollständige Ergebnislisten können bei Bedarf nachgereicht werden. Wir sind davon ausgegangen, dass eine vollständige Darstellung in einer Textdatei den Rahmen sprengen würde.

## 1. Wieviele Produkte jeden Typs (Buch, Musik-CD, DVD) sind in der Datenbank erfasst?

```sql
SELECT pgroup AS typ, COUNT(*) AS anzahl
FROM item
GROUP BY pgroup;
```
| Typ   | Anzahl |
|-------|--------|
| DVD   | 436    |
| Book  | 695    |
| Music | 1841   |


#### oder in drei Spalten:
```sql
SELECT
  COUNT(CASE WHEN pgroup = 'Book' THEN 1 END) AS book_count,
  COUNT(CASE WHEN pgroup = 'Music' THEN 1 END) AS music_count,
  COUNT(CASE WHEN pgroup = 'DVD' THEN 1 END) AS dvd_count
FROM item;
```

| book\_count | music\_count | dvd\_count |
| ----------- | ------------ | ---------- |
| 695         | 1841         | 436        |



## 2. Die 5 besten Produkte jedes Typs sortiert nach durchschnittlichem Rating
Tie-Break nach rating_counter bei gleicher Bewertung

```sql
SELECT pgroup, asin, rating
FROM (
  SELECT asin, pgroup, rating, rating_counter,
         ROW_NUMBER() OVER (
           PARTITION BY pgroup
           ORDER BY rating DESC, rating_counter DESC
         ) AS rn
  FROM item
  WHERE pgroup IN ('Book', 'Music', 'DVD')
    AND rating IS NOT NULL
) t
WHERE rn <= 5
ORDER BY pgroup, rating DESC, rating_counter DESC;
```


| pgroup | asin       | rating |
|--------|------------|--------|
| Book   | 3789105090 | 5.0    |
| Book   | 3551354928 | 5.0    |
| Book   | 3401053698 | 5.0    |
| Book   | 3401027050 | 5.0    |
| Book   | 3832712291 | 5.0    |
| DVD    | B000BW9BZW | 5.0    |
| DVD    | B00062ICW0 | 5.0    |
| DVD    | B00006FR4D | 5.0    |
| DVD    | B0008Y4J6  | 5.0    |
| DVD    | B00005R5VJ | 5.0    |
| Music  | B000062V2L | 5.0    |
| Music  | B0016OYNW  | 5.0    |
| Music  | B0000423X  | 5.0    |
| Music  | B00005MFMN | 5.0    |
| Music  | B000007TKK | 5.0    |



---

## 3. Für welche Produkte gibt es im Moment kein Angebot?

 ```sql
SELECT angebot.asin, title
FROM item
Join angebot
ON item.asin = angebot.asin
WHERE angebot.verfuegbar IS False;
 ```

#### Für 2310

---

## 4. Für welche Produkte ist das teuerste Angebot mehr als doppelt so teuer wie das preiswerteste?

```sql
SELECT asin, MIN(preis) AS min_preis, MAX(preis) AS max_preis
FROM angebot
GROUP BY asin
HAVING MAX(preis) > 2 * MIN(preis);
```

#### für Zwei:
| asin        | min_preis | max_preis |
|-------------|-----------|-----------|
| B0007ZOY72  | 0.10      | 7.19      |
| B00005AT2N  | 7.12      | 17.12     |


---

## 5. Produkte mit mindestens einer Bewertung 1 und einer Bewertung 5

```sql
SELECT asin
FROM rezension
WHERE bewertung = 1
INTERSECT
SELECT asin
FROM rezension
WHERE bewertung = 5;
```

#### 130 Produkte

---

## 6. Für wieviele Produkte gibt es gar keine Rezension?

```sql
SELECT COUNT(*)
FROM item
WHERE rating_counter = 0;
```

#### Für 1112

---

## 7. Rezensenten mit mindestens 10 Rezensionen

```sql
SELECT k.username
FROM kunde k
JOIN rezension r ON k.kunden_id = r.kunden_id
GROUP BY k.username
HAVING COUNT(*) >= 10;
```

#### Es gibt 33 solcher Rezensenten

---

## 8. Alphabetisch sortierte Liste der Buchautoren, die auch an DVDs oder Musik-CDs beteiligt sind

```sql
SELECT DISTINCT p.name
FROM person p
JOIN item_person ip_book ON p.person_id = ip_book.person_id
JOIN item i_book ON ip_book.asin = i_book.asin
WHERE i_book.pgroup = 'Book'
  AND EXISTS (
    SELECT 1
    FROM item_person ip_other
    JOIN item i_other ON ip_other.asin = i_other.asin
    WHERE ip_other.person_id = p.person_id
      AND i_other.pgroup IN ('DVD', 'Music')
  )
ORDER BY p.name;
```

- Ac
- Al
- Brun
- Dav
- Heino
- Jürgen
- Nas
- Nicole
- Peter
- Robin
- Sandra
- Va

---

## 9. Durchschnittliche Anzahl von Liedern einer Musik-CD

```sql
SELECT AVG(cnt) AS schnitt
FROM (
  SELECT it.asin, COUNT(*) AS cnt
  FROM item i
  JOIN item_track it ON i.asin = it.asin
  WHERE i.pgroup = 'Music'
  GROUP BY it.asin
) t;
```

#### Im Schnitt 22.1 Lieder

---

## 10. Produkte mit ähnlichen Produkten in einer anderen Hauptkategorie (rekursiv, PostgreSQL Syntax)

```sql
WITH RECURSIVE hauptkat AS (
  SELECT kategorie_id, name, kategorie_id AS hauptkat_id
  FROM kategorie
  WHERE eltern_id IS NULL
  UNION ALL
  SELECT k.kategorie_id, k.name, h.hauptkat_id
  FROM kategorie k
  JOIN hauptkat h ON k.eltern_id = h.kategorie_id
)
SELECT DISTINCT ik1.asin
FROM item_kategorie ik1
JOIN hauptkat h1 ON ik1.kategorie_id = h1.kategorie_id
JOIN similar_product sp ON ik1.asin = sp.asin
JOIN item_kategorie ik2 ON sp.sim_asin = ik2.asin
JOIN hauptkat h2 ON ik2.kategorie_id = h2.kategorie_id
WHERE h1.hauptkat_id <> h2.hauptkat_id;
```

#### 654 Produkte

---

## 11. Produkte, die in allen Shops angeboten werden

```sql
SELECT asin
FROM angebot
WHERE verfuegbar IS TRUE
GROUP BY asin
HAVING COUNT(DISTINCT shop_id) = (SELECT COUNT(*) FROM shop);
```

#### Es gibt 103 dieser Produkte

---

## 12. In wieviel Prozent der Fälle aus 11 gibt es im Shop 'Leipzig' das preiswerteste Angebot?

```sql
WITH in_alle_shops AS (
  SELECT asin
  FROM angebot
  WHERE verfuegbar IS TRUE
  GROUP BY asin
  HAVING COUNT(DISTINCT shop_id) = (SELECT COUNT(*) FROM shop)
)
SELECT 100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM in_alle_shops), 0) AS prozent
FROM (
  SELECT a.asin
  FROM angebot a
  JOIN shop s ON a.shop_id = s.shop_id
  WHERE s.name = 'Leipzig'
    AND a.preis = (
      SELECT MIN(preis) FROM angebot WHERE asin = a.asin
    )
    AND a.asin IN (SELECT asin FROM in_alle_shops)
) x;
```

#### 49.52%

