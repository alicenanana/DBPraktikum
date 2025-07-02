import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
/**
* Aufgabe 2b
*/

public class UserInputs {
    public static void main(String[] args) throws SQLException {
        String url = "jdbc:postgresql://localhost:5432/postgres";
        String user = "postgres";
        String password = "postgres";

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            String asin = "B00005N5PF";
            int bewertung = 5;
            int kunden_id = 1;
            Date rezensionsdatum = Date.valueOf(LocalDate.now());
            String nutzername = "MaxMustermann";
            String titel = "Tolle Rezension";
            String text = "Das Produkt ist wirklich hervorragend!";
            newRewiew(conn, asin, bewertung, kunden_id, rezensionsdatum, nutzername, titel, text);

        }
    }

    public static void newRewiew(Connection conn, String asin, int bewertung, int kunden_id, Date rezensionsdatum, String nutzername, String titel, String text) throws SQLException {
        String sql = "INSERT INTO rezension (asin, bewertung, kunden_id, rezensionsdatum, nutzername, titel, text) VALUES (?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, asin);
            pstmt.setInt(2, bewertung);
            pstmt.setInt(3, kunden_id);
            pstmt.setDate(4, rezensionsdatum);
            pstmt.setString(5, nutzername);
            pstmt.setString(6, titel);
            if (text != null && !text.isEmpty()) {
                pstmt.setString(7, text);
            } else {
                pstmt.setNull(7, Types.VARCHAR);
            }
            pstmt.executeUpdate();
            
            alterRating(conn, asin, bewertung);
            System.out.println("Neue Rezension hinzugefügt: " + titel);
        }
    }

    public static void alterRating(Connection conn, String asin, float rating) {
        float currentRating = getRatin(conn, asin);
        int ratingCounter = getRatingCounter(conn, asin);
        if (currentRating == 0) {
            currentRating = rating;
            ratingCounter = 1;
        } else {
            currentRating = (currentRating * ratingCounter + rating) / (ratingCounter + 1);
            ratingCounter++;
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
        
    }

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

    public static int getRatingCounter(Connection conn, String asin) {
        String sql = "SELECT RatingCounter FROM item WHERE asin = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, asin);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("RatingCounter");
            }
        } catch (Exception e) {
            System.err.println("Fehler beim Abrufen des RatingCounters: " + e.getMessage());
        }
        return 0;
    }
}
