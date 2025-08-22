-- triggers_rating.sql
CREATE EXTENSION IF NOT EXISTS plpgsql;

-- Funktion: hält item.rating und item.rating_counter aktuell
CREATE OR REPLACE FUNCTION update_rating()
RETURNS TRIGGER AS $$
DECLARE
  target_asin TEXT;
BEGIN
  target_asin := COALESCE(NEW.asin, OLD.asin);

  -- Durchschnitt & Anzahl neu setzen
  UPDATE item i
  SET rating = sub.avg_rating,
      rating_counter = sub.count_rating
  FROM (
    SELECT asin,
           AVG(bewertung)::FLOAT AS avg_rating,
           COUNT(*)               AS count_rating
    FROM rezension
    WHERE asin = target_asin
    GROUP BY asin
  ) sub
  WHERE i.asin = sub.asin;

  -- Falls es gar keine Rezensionen (mehr) gibt → zurücksetzen
  IF NOT EXISTS (SELECT 1 FROM rezension WHERE asin = target_asin) THEN
    UPDATE item SET rating = NULL, rating_counter = 0
    WHERE asin = target_asin;
  END IF;

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Idempotent: alte Trigger wegräumen
DROP TRIGGER IF EXISTS trg_update_rating_after_insert ON rezension;
DROP TRIGGER IF EXISTS trg_update_rating_after_update ON rezension;
DROP TRIGGER IF EXISTS trg_update_rating_after_delete ON rezension;

-- Neue Trigger anlegen
CREATE TRIGGER trg_update_rating_after_insert
AFTER INSERT ON rezension
FOR EACH ROW EXECUTE FUNCTION update_rating();

CREATE TRIGGER trg_update_rating_after_update
AFTER UPDATE ON rezension
FOR EACH ROW EXECUTE FUNCTION update_rating();

CREATE TRIGGER trg_update_rating_after_delete
AFTER DELETE ON rezension
FOR EACH ROW EXECUTE FUNCTION update_rating();
