
import com.opencsv.CSVReader;

import java.io.FileReader;
import java.sql.*;

public class ImportReviews {
    public static void main(String[] args) {
        String jdbcUrl = "jdbc:postgresql://localhost:5432/postgres";
        String dbUser = "postgres";
        String dbPass = "postgres";
        String csvPath = "data/reviews.csv";

        try (Connection conn = DriverManager.getConnection(jdbcUrl, dbUser, dbPass);
                CSVReader reader = new CSVReader(new FileReader(csvPath))) {

            conn.setAutoCommit(false);
            String[] line;
            int count = 0;

            // Header überspringen
            reader.readNext();

            while ((line = reader.readNext()) != null) {
                String produktId = line[0];
                String username = line[1];
                int punkte = Integer.parseInt(line[2]);
                String text = line[3];

                if (punkte < 1 || punkte > 5) {
                    System.out.println("Überspringe ungültige Bewertung: " + punkte);
                    continue;
                }

                // Kunde suchen oder anlegen
                int kundenId = getOrCreateKundeId(conn, username);

                // Rezension einfügen
                PreparedStatement stmt = conn.prepareStatement(
                        "INSERT INTO rezension (produktnr, kunden_id, punkte, kommentar) VALUES (?, ?, ?, ?)");
                stmt.setInt(1, Integer.parseInt(produktId));
                stmt.setInt(2, kundenId);
                stmt.setInt(3, punkte);
                stmt.setString(4, text);
                stmt.executeUpdate();

                count++;
            }

            conn.commit();
            System.out.println(count + " Rezensionen importiert.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static int getOrCreateKundeId(Connection conn, String username) throws SQLException {
        // Prüfen, ob Kunde bereits existiert
        PreparedStatement select = conn.prepareStatement(
                "SELECT kunden_id FROM kunde WHERE name = ?");
        select.setString(1, username);
        ResultSet rs = select.executeQuery();
        if (rs.next()) {
            return rs.getInt(1);
        }

        // Kunde anlegen, wenn nicht vorhanden
        PreparedStatement insert = conn.prepareStatement(
                "INSERT INTO kunde (name, adresse, kontonr) VALUES (?, '', ?) RETURNING kunden_id");
        insert.setString(1, username);
        insert.setString(2, "KNR" + System.currentTimeMillis()); // dummy kontonr
        ResultSet newRs = insert.executeQuery();
        newRs.next();
        return newRs.getInt(1);
    }
}
