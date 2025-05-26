/*
 * deprecated - stattdessen über inserts erledigt
 * import com.opencsv.CSVReader;
 * import java.io.FileReader;
 * import java.sql.*;
 * import java.util.HashSet;
 * import java.util.Set;
 * 
 * public class ImportCustomers {
 * 
 * public static void main(String[] args) {
 * String jdbcUrl = "jdbc:postgresql://localhost:5432/meine_media_store_db";
 * String username = "postgres";
 * String password = "postgres";
 * String csvFile = "data/reviews.csv";
 * 
 * Set<String> seenUsers = new HashSet<>();
 * 
 * try (
 * Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
 * CSVReader reader = new CSVReader(new FileReader(csvFile));
 * ) {
 * conn.setAutoCommit(false);
 * String[] nextLine;
 * boolean firstLine = true;
 * 
 * String insertSql =
 * "INSERT INTO kunde (kunden_id, name, adresse, kontonr) VALUES (?, ?, ?, ?)";
 * PreparedStatement insertStmt = conn.prepareStatement(insertSql);
 * 
 * while ((nextLine = reader.readNext()) != null) {
 * if (firstLine) {
 * firstLine = false;
 * continue; // Skip header
 * }
 * 
 * String kundenId = nextLine[1].trim(); // 'user' column
 * if (!seenUsers.contains(kundenId)) {
 * seenUsers.add(kundenId);
 * insertStmt.setString(1, kundenId);
 * insertStmt.setString(2, "Dummy Name");
 * insertStmt.setString(3, "Dummy Adresse");
 * insertStmt.setString(4, "konto_" + Math.abs(kundenId.hashCode())); //
 * dynamisch und unique
 * insertStmt.addBatch();
 * }
 * }
 * 
 * insertStmt.executeBatch();
 * conn.commit();
 * System.out.println("✅ Kundenimport abgeschlossen: " + seenUsers.size() +
 * " Benutzer eingefügt.");
 * 
 * } catch (Exception e) {
 * e.printStackTrace();
 * }
 * }
 * }
 */