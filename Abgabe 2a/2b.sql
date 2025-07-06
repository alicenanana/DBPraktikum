Create Extension plpgsql;

-- Neue Bewertung einfügen
INSERT INTO rezension (kunden_id,  asin, bewertung, rezensionsdatum, titel, text)
VALUES (11, '3551551677', 1, CURRENT_DATE, 'Naja', 'Meh, das Buch war nicht so toll.') ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION update_rating()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE item
    SET
        rating = sub.avg_rating,
        rating_counter = sub.count_rating
    FROM (
        SELECT asin, AVG(bewertung)::FLOAT AS avg_rating, COUNT(*) AS count_rating
        FROM rezension
        WHERE asin = NEW.asin
        GROUP BY asin
    ) AS sub
    WHERE item.asin = sub.asin;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger für UPDATE einer Bewertung (nur wenn sich die Bewertung ändert)
CREATE TRIGGER trg_update_rating
AFTER INSERT ON rezension
FOR EACH ROW
EXECUTE FUNCTION update_rating();

-- Trigger für DELETE einer Rezension
CREATE TRIGGER trg_update_rating_after_delete
AFTER DELETE ON rezension
FOR EACH ROW
EXECUTE FUNCTION update_rating();

-- Abfrage der Rezensionen und des Items
Select * from Rezension Where asin = '3551551677';
Select * from item where asin = '3551551677';
