CREATE EXTENSION IF NOT EXISTS plpgsql;

-- Neue Bewertung einfügen
INSERT INTO rezension (kunden_id, asin, bewertung, rezensionsdatum, titel, text)
VALUES (11, '3551551677', 1, CURRENT_DATE, 'Naja', 'Meh, das Buch war nicht so toll.')
ON CONFLICT DO NOTHING;

-- Funktion zur automatischen Aktualisierung des Ratings
CREATE OR REPLACE FUNCTION update_rating()
RETURNS TRIGGER AS $$
DECLARE
    target_asin TEXT;
BEGIN
    -- asin aus NEW (für INSERT, UPDATE) oder OLD (für DELETE)
    target_asin := COALESCE(NEW.asin, OLD.asin);

    -- Durchschnitt berechnen und in item schreiben
    UPDATE item
    SET
        rating = sub.avg_rating,
        rating_counter = sub.count_rating
    FROM (
        SELECT asin, AVG(bewertung)::FLOAT AS avg_rating, COUNT(*) AS count_rating
        FROM rezension
        WHERE asin = target_asin
        GROUP BY asin
    ) AS sub
    WHERE item.asin = sub.asin;

    -- Falls keine Rezensionen mehr vorhanden sind
    IF NOT EXISTS (SELECT 1 FROM rezension WHERE asin = target_asin) THEN
        UPDATE item
        SET rating = NULL,
            rating_counter = 0
        WHERE asin = target_asin;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Trigger für neue Rezensionen (INSERT)
CREATE TRIGGER trg_update_rating_after_insert
AFTER INSERT ON rezension
FOR EACH ROW
EXECUTE FUNCTION update_rating();

-- Trigger für Änderung der Bewertung (UPDATE)
CREATE TRIGGER trg_update_rating_after_update
AFTER UPDATE ON rezension
FOR EACH ROW
EXECUTE FUNCTION update_rating();

-- Trigger für Löschen von Rezensionen (DELETE)
CREATE TRIGGER trg_update_rating_after_delete
AFTER DELETE ON rezension
FOR EACH ROW
EXECUTE FUNCTION update_rating();

-- Abfrage der Rezensionen und des Items
Select * from Rezension Where asin = '3551551677';
Select * from item where asin = '3551551677';
