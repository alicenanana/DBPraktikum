
--1
SELECT pgroup AS typ, COUNT(*) AS anzahl
FROM item
GROUP BY pgroup;

--1 mit 3 Spalten
SELECT
  COUNT(CASE WHEN pgroup = 'Book' THEN 1 END) AS book_count,
  COUNT(CASE WHEN pgroup = 'Music' THEN 1 END) AS music_count,
  COUNT(CASE WHEN pgroup = 'DVD' THEN 1 END) AS dvd_count
FROM item;


--2
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

--3
SELECT asin, title
FROM item
WHERE asin NOT IN (
  SELECT asin
  FROM angebot
  WHERE verfuegbar IS TRUE
    AND asin IS NOT NULL
);

Select verfuegbar, Count(*) as vc from Angebot
Where verfuegbar IS false
GROUP BY verfuegbar;

SELECT angebot.asin, title
FROM item
Join angebot
ON item.asin = angebot.asin
WHERE angebot.verfuegbar IS False;



--4
SELECT asin, MIN(preis) AS min_preis, MAX(preis) AS max_preis
FROM angebot
GROUP BY asin
HAVING MAX(preis) > 2 * MIN(preis);

--5
SELECT asin
FROM rezension
WHERE bewertung = 1
INTERSECT
SELECT asin
FROM rezension
WHERE bewertung = 5;

--6
SELECT COUNT(*)
FROM item
WHERE rating_counter IS NULL;

--7
SELECT k.username
FROM kunde k
JOIN rezension r ON k.kunden_id = r.kunden_id
GROUP BY k.username
HAVING COUNT(*) >= 10;

--8
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


--9
SELECT AVG(cnt) AS schnitt
FROM (
  SELECT it.asin, COUNT(*) AS cnt
  FROM item i
  JOIN item_track it ON i.asin = it.asin
  WHERE i.pgroup = 'Music'
  GROUP BY it.asin
) t;

--10
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

--11
SELECT asin
FROM angebot
WHERE verfuegbar IS TRUE
GROUP BY asin
HAVING COUNT(DISTINCT shop_id) = (SELECT COUNT(*) FROM shop);

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