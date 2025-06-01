import com.opencsv.CSVReader;
import java.io.FileReader;
import java.sql.*;
import java.sql.Date;

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
        String url = "jdbc:postgresql://localhost:5432/meine_media_store_db";
        String user = "postgres";
        String password = "postgres";
        String filePath = "media-store/data/reviews.csv";

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
                                "INSERT INTO kunde (kunden_id, username) VALUES (?, ?)")) {
                            insertKunde.setInt(1, kunden_id);
                            insertKunde.setString(2, nutzername);
                            insertKunde.executeUpdate();
                            System.out.println("→ Neuer Kunde eingefügt: ID " + kunden_id + ", Name: " + nutzername);
                        } catch (SQLException e) {
                            System.err.println("✗ Fehler beim Einfügen des Kunden: " + e.getMessage());
                            continue;
                        }
                    }

                    // Rezension einfügen
                    try (PreparedStatement stmt = conn.prepareStatement(
                            "INSERT INTO rezension (kunden_id, asin, bewertung, text, rezensionsdatum) VALUES (?, ?, ?, ?, ?)")) {
                        stmt.setInt(1, kunden_id);
                        stmt.setString(2, asin);
                        stmt.setInt(3, bewertung);
                        stmt.setString(4, text);
                        stmt.setDate(5, rezensionsdatum);
                        stmt.executeUpdate();
                        System.out.println("✓ Rezension importiert für ASIN " + asin + ", Kunde " + kunden_id);
                    }

                } catch (Exception e) {
                    System.err.println("✗ Fehler in Zeile " + lineNumber + ": " + e.getMessage());
                }
            }

        } catch (Exception e) {
            System.err.println("Allgemeiner Fehler beim Import: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
