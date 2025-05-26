import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import com.opencsv.CSVReader;

public class ImportReviews {
    public static void main(String[] args) {
        String jdbcUrl = "jdbc:postgresql://localhost:5432/meine_media_store_db";
        String username = "postgres";
        String password = "postgres";
        String csvFile = "data/reviews.csv";

        try (
                Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
                CSVReader reader = new CSVReader(new FileReader(csvFile));) {
            String[] nextLine;
            boolean firstLine = true;

            String sql = "INSERT INTO rezension (kunden_id, produktnr, punkte, kommentar) VALUES (?, ?, ?, ?)";
            PreparedStatement stmt = conn.prepareStatement(sql);

            while ((nextLine = reader.readNext()) != null) {
                if (firstLine) {
                    firstLine = false;
                    continue; // skip header
                }

                try {
                    String kundenId = nextLine[1]; // user (z.B. "A3IYJZGW3A4UVO")
                    String produktNr = nextLine[2]; // product (z.B. "B00005RSCH")
                    int punkte = Integer.parseInt(nextLine[3]); // rating
                    String kommentar = nextLine[4]; // content

                    stmt.setString(1, kundenId);
                    stmt.setString(2, produktNr);
                    stmt.setInt(3, punkte);
                    stmt.setString(4, kommentar);

                    stmt.executeUpdate();
                } catch (Exception e) {
                    System.err.println("❌ Fehler beim Einfügen: " + String.join(", ", nextLine));
                    e.printStackTrace();
                }
            }

            System.out.println("✅ Import abgeschlossen.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
