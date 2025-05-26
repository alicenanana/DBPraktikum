import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.*;

import java.io.File;
import java.sql.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class XmlImporter {
    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:5432/meine_media_store_db";
        String user = "postgres";
        String password = "postgres";

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            System.out.println("Verbindung zur Datenbank hergestellt.");

            // Importiere alle XML-Dateien
            importXmlFile(conn, "data/leipzig_transformed.xml", "Leipzig");
            importXmlFile(conn, "data/dresden.xml", "Dresden");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void importXmlFile(Connection conn, String filePath, String stadtName) {
        try {
            File xmlFile = new File(filePath);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(xmlFile);
            doc.getDocumentElement().normalize();

            Set<String> bekannteTypen = new HashSet<>(Arrays.asList(
                    "AUDIO-CD", "MUSIKKASSETTE", "VINYL LP", "CD", "MUSIC", "MUSICAL", "DVD + CD", "DVD", "BUCH",
                    "BOOK"));
            Set<String> unbekannteTypen = new HashSet<>();

            // Beispiel: Importiere Produkte
            NodeList produktList = doc.getElementsByTagName("item");
            System.out.println("-- Datei gelesen: " + filePath);
            System.out.println("> Starte Verarbeitung ...");
            System.out.println("> Anzahl Produkte gefunden: " + produktList.getLength());

            for (int i = 0; i < produktList.getLength(); i++) {
                Node node = produktList.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    Element element = (Element) node;
                    String produktnr = element.getAttribute("asin");
                    String titel = getTextContent(element, "title").trim();
                    String verkaufsrang = element.getAttribute("salesrank");
                    String rating = element.getAttribute("rating");

                    String originalTyp = element.getAttribute("pgroup").trim();
                    String typ = originalTyp != null ? originalTyp.trim().toUpperCase() : null;

                    switch (typ) {
                        case "AUDIO-CD":
                        case "MUSIKKASSETTE":
                        case "VINYL LP":
                        case "MUSIC":
                        case "MUSICAL":
                        case "CD":
                            typ = "CD";
                            break;
                        case "DVD + CD":
                        case "DVD":
                            typ = "DVD";
                            break;
                        case "BUCH":
                        case "BOOK":
                            typ = "BUCH";
                            break;
                        default:
                            if (!originalTyp.isEmpty() && !bekannteTypen.contains(typ)
                                    && !unbekannteTypen.contains(typ)) {
                                System.out.println(
                                        "!!! Unbekannter pgroup-Typ: '" + typ + "' (Produkt " + produktnr + ")");
                                unbekannteTypen.add(typ);
                            }
                            typ = null; // ungültig
                    }

                    System.out.println("> Verarbeite Produkt: " + produktnr + " | Titel: " + titel);

                    try {
                        if (typ != null && !titel.isBlank()) {
                            PreparedStatement stmt = conn.prepareStatement(
                                    "INSERT INTO produkt (produktnr, titel, typ, verkaufsrang, rating) VALUES (?, ?, ?, ?, ?) "
                                            +
                                            "ON CONFLICT (produktnr) DO NOTHING");
                            stmt.setString(1, produktnr);
                            stmt.setString(2, titel);
                            stmt.setString(3, typ);
                            stmt.setInt(4, verkaufsrang.isEmpty() ? 0 : Integer.parseInt(verkaufsrang));
                            stmt.setFloat(5, rating.isEmpty() ? 0f : Float.parseFloat(rating));
                            stmt.executeUpdate();

                            System.out.println("-- Eingefügt: " + produktnr);
                        } else {
                            System.out.println("--- Überspringe Produkt " + produktnr + " (fehlender Typ oder Titel)");
                        }
                    } catch (SQLException e) {
                        System.err.println("X Fehler beim Einfügen von Produkt: " + produktnr);
                        e.printStackTrace();
                    }

                }
            }

            if (!unbekannteTypen.isEmpty()) {
                System.out.println("=== Unbekannte pgroup-Typen ===");
                for (String typ : unbekannteTypen) {
                    System.out.println("- " + typ);
                }
            }

        } catch (Exception e) {
            System.err.println("X Fehler beim Import von Datei: " + filePath);
            e.printStackTrace();
        }
    }

    private static String getTextContent(Element parent, String tag) {
        NodeList list = parent.getElementsByTagName(tag);
        return (list.getLength() > 0) ? list.item(0).getTextContent() : "";
    }

}
