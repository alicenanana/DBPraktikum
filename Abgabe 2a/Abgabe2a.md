## 1. Wieviele Produkte jeden Typs (Buch, Musik-CD, DVD) sind in der Datenbank erfasst?

```sql
SELECT pgroup AS typ, COUNT(*) AS anzahl
FROM item
GROUP BY pgroup;
```
| Typ   | Anzahl |
|-------|--------|
| DVD   | 436    |
| Book  | 697    |
| Music | 1841   |
---

## 2. Die 5 besten Produkte jedes Typs sortiert nach durchschnittlichem Rating


```sql
SELECT pgroup, asin, rating
FROM (
  SELECT i.pgroup, i.asin, avg_r.rating,
         ROW_NUMBER() OVER (PARTITION BY i.pgroup ORDER BY avg_r.rating DESC) AS rn
  FROM (
    SELECT asin, AVG(bewertung) AS rating
    FROM rezension
    GROUP BY asin
  ) avg_r
  JOIN item i ON i.asin = avg_r.asin
  WHERE i.pgroup IN ('Book', 'Music', 'DVD')
) t
WHERE rn <= 5
ORDER BY pgroup, rating DESC;
```

| pgroup | asin       | rating | rn |
|--------|------------|--------|----|
| Book   | 3789105090 | 5.0    | 1  |
| Book   | 3551354928 | 5.0    | 2  |
| Book   | 3401053698 | 5.0    | 3  |
| Book   | 3401027050 | 5.0    | 4  |
| Book   | 3832712291 | 5.0    | 5  |
| DVD    | B000BW9BZW | 5.0    | 1  |
| DVD    | B00062ICW0 | 5.0    | 2  |
| DVD    | B00006FR4D | 5.0    | 3  |
| DVD    | B0008Y4J6  | 5.0    | 4  |
| DVD    | B00005R5VJ | 5.0    | 5  |
| Music  | B000062V2L | 5.0    | 1  |
| Music  | B0016OYNW  | 5.0    | 2  |
| Music  | B0000423X  | 5.0    | 3  |
| Music  | B00005MFMN | 5.0    | 4  |
| Music  | B000007TKK | 5.0    | 5  |



---

## 3. Für welche Produkte gibt es im Moment kein Angebot?

```sql
SELECT asin, title
FROM item
WHERE asin NOT IN (SELECT asin FROM angebot WHERE verfuegbar IS TRUE);
```

 #### Für 2148

---

## 4. Für welche Produkte ist das teuerste Angebot mehr als doppelt so teuer wie das preiswerteste?
(keins)

```sql
SELECT asin
FROM angebot
GROUP BY asin
HAVING MAX(preis) > 2 * MIN(preis);
```

---

## 5. Produkte mit mindestens einer Bewertung 1 und einer Bewertung 5
(130)

```sql
SELECT asin
FROM rezension
WHERE bewertung = 1
INTERSECT
SELECT asin
FROM rezension
WHERE bewertung = 5;
```

---

## 6. Für wieviele Produkte gibt es gar keine Rezension?
Für 1112

```sql
SELECT COUNT(*)
FROM item i
LEFT JOIN rezension r ON i.asin = r.asin
WHERE r.asin IS NULL;
```

---

## 7. Rezensenten mit mindestens 10 Rezensionen
Es gibt 33 solcher Rezensenten

```sql
SELECT k.username
FROM kunde k
JOIN rezension r ON k.kunden_id = r.kunden_id
GROUP BY k.username
HAVING COUNT(*) >= 10;
```

---

## 8. Alphabetisch sortierte Liste der Buchautoren, die auch an DVDs oder Musik-CDs beteiligt sind

*Hinweis: Annahme: Autoren sind in person, und die Verknüpfung zur Rolle erfolgt über item_person.person_role='Author', Produktart über item.pgroup.*

Da kommt nichts raus

```sql
SELECT DISTINCT p.name
FROM person p
JOIN item_person ip ON p.person_id = ip.person_id
JOIN item i1 ON ip.asin = i1.asin
WHERE i1.pgroup = 'Book' AND ip.person_role = 'Author'
AND p.person_id IN (
  SELECT ip2.person_id
  FROM item_person ip2
  JOIN item i2 ON ip2.asin = i2.asin
  WHERE i2.pgroup IN ('Music', 'DVD')
)
ORDER BY p.name;
```

---

## 9. Durchschnittliche Anzahl von Liedern einer Musik-CD

22.0757918552036199

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

---

## 10. Produkte mit ähnlichen Produkten in einer anderen Hauptkategorie (rekursiv, PostgreSQL Syntax)

geht noch nicht

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
SELECT DISTINCT i1.asin
FROM item_kategorie ik1
JOIN hauptkat h1 ON ik1.kategorie_id = h1.kategorie_id
JOIN similar_product sp ON ik1.asin = sp.asin
JOIN item_kategorie ik2 ON sp.sim_asin = ik2.asin
JOIN hauptkat h2 ON ik2.kategorie_id = h2.kategorie_id
WHERE h1.hauptkat_id <> h2.hauptkat_id;
```

---

## 11. Produkte, die in allen Shops angeboten werden

Es gibt 103 dieser Produkte

```sql
SELECT asin
FROM angebot
WHERE verfuegbar IS TRUE
GROUP BY asin
HAVING COUNT(DISTINCT shop_id) = (SELECT COUNT(*) FROM shop);
```

---

## 12. In wieviel Prozent der Fälle aus 11 gibt es im Shop 'Leipzig' das preiswerteste Angebot?

49.5145631067961165%

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

