import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class ImportCategories {

    // Zähler zur Vergabe eindeutiger Kategorie-IDs
    private static int categoryCounter = 1;

    // Map zur Zwischenspeicherung bereits eingefügter Kategorien anhand von Name + Parent-ID
    private static final Map<String, Integer> categoryMap = new HashMap<>();

    public static void main(String[] args) {
        // Verbindungsparameter für die Datenbank
        String url = "jdbc:postgresql://localhost:5432/postgres";
        String user = "postgres";
        String password = "postgres";

        try (Connection conn = DriverManager.getConnection(url, user, password)) {

            // Lade die XML-Datei mit den Kategorien
            File xmlFile = new File("data", "categories.xml");
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(xmlFile);
            doc.getDocumentElement().normalize();

            // Ausgabe zur Visualisierung des Starts
            for (int i = 0; i < 3; i++) System.out.println("");
            System.out.println("Starte Import der Kategorien...");
            for (int i = 0; i < 3; i++) System.out.println("");

            // Iteriere über alle direkten Kindknoten des Wurzelelements
            NodeList categories = doc.getDocumentElement().getChildNodes();
            for (int i = 0; i < categories.getLength(); i++) {
                Node node = categories.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE && node.getNodeName().equals("category")) {
                    // Verarbeite jede Top-Level-Kategorie rekursiv
                    processCategory((Element) node, null, conn);
                }
            }

            // Abschlussmeldung
            System.out.println("Kategorieimport abgeschlossen.");
            for (int i = 0; i < 3; i++) System.out.println("");

        } catch (Exception e) {
            // Fehlerbehandlung
            System.err.println("Ein Fehler ist aufgetreten: " + e.getMessage());
        }
    }

    // Diese Methode verarbeitet eine einzelne Kategorie, rekursiv für alle Unterkategorien
    private static void processCategory(Element element, Integer parentId, Connection conn) throws Exception {
        // Hole den Namen der Kategorie aus dem Attribut "name"
        String categoryName = element.getAttribute("name");
        if (categoryName == null || categoryName.trim().isEmpty()) {
            System.err.println("Kategorie ohne Namen gefunden. Überspringe.");
            return;
        }

        // Füge die Kategorie in die Datenbank ein (oder hole ID, falls schon vorhanden)
        int categoryId = insertCategory(conn, categoryName, parentId);

        // Verarbeite Kindknoten: können Unterkategorien oder Items sein
        NodeList children = element.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (child.getNodeType() == Node.ELEMENT_NODE) {
                if (child.getNodeName().equals("category")) {
                    // Rekursiver Aufruf für Unterkategorie
                    processCategory((Element) child, categoryId, conn);
                } else if (child.getNodeName().equals("item")) {
                    // Eintrag in item_kategorie herstellen
                    String asin = ((Element) child).getAttribute("asin");
                    insertItemCategory(conn, asin, categoryId);
                }
            }
        }
    }

    // Fügt eine neue Kategorie in die Tabelle "kategorie" ein
    private static int insertCategory(Connection conn, String name, Integer parentId) throws Exception {
        // Generiere Schlüssel für Map, um doppelte Einfügungen zu vermeiden
        String key = name + ":" + (parentId != null ? parentId : "root");
        if (categoryMap.containsKey(key)) {
            return categoryMap.get(key);
        }

        // Neue ID erzeugen
        int newId = categoryCounter++;

        // SQL-Statement zum Einfügen vorbereiten
        PreparedStatement ps = conn.prepareStatement("INSERT INTO kategorie (kategorie_id, name, eltern_id) VALUES (?, ?, ?)");
        ps.setInt(1, newId);
        ps.setString(2, name);
        if (parentId != null) {
            ps.setInt(3, parentId);
        } else {
            ps.setNull(3, Types.INTEGER);
        }
        ps.executeUpdate();

        // Map aktualisieren
        categoryMap.put(key, newId);
        return newId;
    }

    // Verknüpft eine ASIN mit einer Kategorie in der Tabelle "item_kategorie"
    private static void insertItemCategory(Connection conn, String asin, int categoryId) throws Exception {
        if (asin == null || asin.trim().isEmpty()) return;

        // SQL-Insert vorbereiten mit ON CONFLICT DO NOTHING, um Duplikate zu vermeiden
        PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO item_kategorie (asin, kategorie_id) VALUES (?, ?) ON CONFLICT DO NOTHING"
        );
        ps.setString(1, asin);
        ps.setInt(2, categoryId);
        ps.executeUpdate();
    }
}
