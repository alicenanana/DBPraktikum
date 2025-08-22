import java.io.FileReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.opencsv.CSVReader;

/**
 * ImportReviews.java
 * 
 * This class imports reviews from a CSV file into a PostgreSQL database.
 * It checks if the customer exists and creates a new customer if not.
 * 
 * Usage: Ensure the CSV file is located in the "media-store/data" directory and
 * run this class.
 */
public class ImportReviews {

    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:5432/postgres";
        String user = "postgres";
        String password = "postgres";
        String filePath = "data/reviews.csv";

        try (Connection conn = DriverManager.getConnection(url, user, password);
                CSVReader reader = new CSVReader(new FileReader(filePath))) {

            String[] line;
            int lineNumber = 0;

            while ((line = reader.readNext()) != null) {
                if (lineNumber++ == 0)
                    continue; // Skip header

                if (line.length < 6) {
                    System.err.println("Übersprungene Zeile (zu wenig Spalten): " + String.join(" | ", line));
                    continue;
                }

                try {
                    String asin = line[0].trim();

                    int bewertung = Integer.parseInt(line[1].trim());
                    int kunden_id = Integer.parseInt(line[2].trim());
                    Date rezensionsdatum = Date.valueOf(line[3].trim()); // Format: yyyy-mm-dd
                    String nutzername = line[4].trim();
                    String titel = line[5].trim();
                    String text = (line.length > 6) ? line[6].trim() : "";

                    if (nutzername.isEmpty() || kunden_id < 0) {
                        System.err.println("Ungültige Nutzerdaten in Zeile: " + lineNumber);
                        continue;
                    }

                    // Prüfen, ob Kunde mit ID existiert
                    boolean kundeExistiert = false;
                    try (PreparedStatement check = conn.prepareStatement("SELECT 1 FROM kunde WHERE kunden_id = ?")) {
                        check.setInt(1, kunden_id);
                        ResultSet rs = check.executeQuery();
                        if (rs.next()) {
                            kundeExistiert = true;
                        }
                    }

                    // Falls nicht vorhanden, neuen Kunden mit nur Username einfügen (mail, adresse,
                    // konto_nr = NULL)
                    if (!kundeExistiert) {
                        try (PreparedStatement insertKunde = conn.prepareStatement(
                                "INSERT INTO kunde (kunden_id, username) VALUES (?, ?) ON CONFLICT DO NOTHING")) {
                            insertKunde.setInt(1, kunden_id);
                            insertKunde.setString(2, nutzername);
                            insertKunde.executeUpdate();
                            System.out.println("→ Neuer Kunde eingefügt: ID " + kunden_id + ", Name: " + nutzername);
                        } catch (SQLException e) {
                            System.err.println("✗ Fehler beim Einfügen des Kunden: " + e.getMessage());
                            continue;
                        }
                    }
                    try (PreparedStatement checkAsin = conn.prepareStatement("SELECT asin FROM item WHERE asin = ?")) {
                        checkAsin.setString(1, asin);
                        ResultSet rs = checkAsin.executeQuery();
                        if (!rs.next()) {
                            System.err.println("Übersprungene Zeile (ASIN nicht gefunden): " + asin);
                            continue;
                        }
                    }

                    if (bewertung < 1 || bewertung > 5) {
                        System.err.println("Ungültige Bewertung in Zeile " + lineNumber + ": " + bewertung);
                        continue;
                    }

                    // Rezension einfügen
                    try (PreparedStatement stmt = conn.prepareStatement(
                            "INSERT INTO rezension (asin, bewertung, text, rezensionsdatum, titel) VALUES (?, ?, ?, ?, ?  ) ON CONFLICT DO NOTHING")) {
                        stmt.setString(1, asin);
                        stmt.setInt(2, bewertung);
                        stmt.setString(3, text);
                        stmt.setDate(4, rezensionsdatum);
                        stmt.setString(5, titel);
                        stmt.executeUpdate();
                        System.out.println("Rezension importiert für ASIN " + asin + ", Kunde " + kunden_id);
                        alterRating(conn, asin, bewertung);
                    }

                } catch (Exception e) {
                    System.err.println("Fehler in Zeile " + lineNumber + ": " + e.getMessage());
                }
            }

        } catch (Exception e) {
            System.err.println("Allgemeiner Fehler beim Import: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Aktualisiert das Rating eines Produkts basierend auf der neuen Bewertung.
     * @param conn   Die Datenbankverbindung.
     * @param asin   Die ASIN des Produkts.
     * @param rating Die neue Bewertung.
     */
    public static void alterRating(Connection conn, String asin, float rating) {
        float currentRating = getRatin(conn, asin);
        int ratingCounter = getRatingCounter(conn, asin);
        if (currentRating == 0) {
            currentRating = rating;
            ratingCounter = 1;
            System.out.println("Neues Rating: " + rating + ", Aktuelles Rating: " + currentRating + ", Rating Counter: " + ratingCounter + " " + asin);
        } else {
            
            currentRating = (currentRating * ratingCounter + rating) / (ratingCounter+1);
            ratingCounter++;
            System.out.println("Neuse Ratin: "+ rating +" Aktuelles Rating: " + currentRating + ", Rating Counter: " + ratingCounter + " " +  asin);
        } 
        String sql = "UPDATE item SET Rating = ? WHERE asin = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setFloat(1, currentRating);
            pstmt.setString(2, asin);
            pstmt.executeUpdate();
            System.out.println("Rating aktualisiert für ASIN: " + asin);
        } catch (SQLException e) {
            System.err.println("Fehler beim Aktualisieren des Ratings: " + e.getMessage());
        }
        try (PreparedStatement pstmt = conn.prepareStatement("UPDATE item SET Rating_Counter = ? WHERE asin = ?")) {
            pstmt.setInt(1, ratingCounter);
            pstmt.setString(2, asin);
            pstmt.executeUpdate();
            System.out.println("Rating Counter aktualisiert für ASIN: " + asin);
        } catch (SQLException e) {
            System.err.println("Fehler beim Aktualisieren des Rating Counters: " + e.getMessage());
        }
        
    }

    /**
     * Ruft das aktuelle Rating eines Produkts ab.
     * @param conn Die Datenbankverbindung.
     * @param asin Die ASIN des Produkts.
     * @return Das aktuelle Rating oder 0, wenn nicht gefunden.
     */
    public static float getRatin(Connection conn, String asin) {
        String sql = "SELECT Rating FROM item WHERE asin = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, asin);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getFloat("Rating");
            }
        } catch (Exception e) {
            System.err.println("Fehler beim Abrufen des Ratings: " + e.getMessage());
        }
        return 0;
    }

    /**
     * Ruft den Rating Counter eines Produkts ab.
     * @param conn Die Datenbankverbindung.
     * @param asin Die ASIN des Produkts.
     * @return Der Rating Counter oder 0, wenn nicht gefunden.
     */
    public static int getRatingCounter(Connection conn, String asin) {
        String sql = "SELECT Rating_Counter FROM item WHERE asin = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, asin);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("Rating_Counter");
            }
        } catch (Exception e) {
            System.err.println("Fehler beim Abrufen des RatingCounters: " + e.getMessage());
        }
        return 0;
    }
}
