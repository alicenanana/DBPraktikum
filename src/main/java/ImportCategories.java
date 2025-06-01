import java.io.File;
import java.sql.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.*;

public class ImportCategories {

    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:5432/postgres";
        String user = "postgres";
        String password = "postgres";

        try (Connection conn = DriverManager.getConnection(url, user, password)) {

            File xmlFile = new File("media-store/data/categories.xml");
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(xmlFile);
            doc.getDocumentElement().normalize();

            System.out.println("\n\n\nStarte Import der Kategorien...\n\n\n");

            NodeList categories = doc.getDocumentElement().getChildNodes();
            for (int i = 0; i < categories.getLength(); i++) {
                Node node = categories.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE && node.getNodeName().equals("category")) {
                    processCategory((Element) node, null, conn);
                }
            }

            System.out.println("\n\n\nKategorieimport abgeschlossen.\n\n\n");

        } catch (Exception e) {
            System.err.println("Fehler beim Import: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void processCategory(Element element, Integer parentId, Connection conn) throws Exception {
        String categoryName = getCategoryName(element);
        if (categoryName.isEmpty()) {
            System.err.println("Kategorie ohne Namen übersprungen.");
            return;
        }

        int categoryId = getOrInsertCategory(conn, categoryName, parentId);

        NodeList children = element.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (child.getNodeType() == Node.ELEMENT_NODE) {
                Element el = (Element) child;
                if (el.getTagName().equals("category")) {
                    processCategory(el, categoryId, conn);
                } else if (el.getTagName().equals("item")) {
                    String asin = el.getTextContent().trim();
                    insertItemCategory(conn, asin, categoryId);
                }
            }
        }
    }

    private static String getCategoryName(Element element) {
        Node first = element.getFirstChild();
        if (first != null && first.getNodeType() == Node.TEXT_NODE) {
            return first.getNodeValue().trim();
        }
        return "";
    }

    private static int getOrInsertCategory(Connection conn, String name, Integer parentId) throws Exception {
        String query = "SELECT kategorie_id FROM kategorie WHERE name = ? AND "
                + (parentId == null ? "eltern_id IS NULL" : "eltern_id = ?");
        try (PreparedStatement ps = conn.prepareStatement(query)) {
            ps.setString(1, name);
            if (parentId != null) {
                ps.setInt(2, parentId);
            }
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt("kategorie_id");
            }
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO kategorie (name, eltern_id) VALUES (?, ?) RETURNING kategorie_id")) {
            ps.setString(1, name);
            if (parentId != null) {
                ps.setInt(2, parentId);
            } else {
                ps.setNull(2, Types.INTEGER);
            }
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                int id = rs.getInt(1);
                System.out.println("Kategorie eingefügt: " + name + " (ID: " + id + ")");
                return id;
            }
        }

        throw new Exception("Kategorie konnte nicht eingefügt werden: " + name);
    }

    private static void insertItemCategory(Connection conn, String asin, int categoryId) throws Exception {
        if (asin.isEmpty())
            return;

        try (PreparedStatement check = conn.prepareStatement("SELECT 1 FROM item WHERE asin = ?")) {
            check.setString(1, asin);
            ResultSet rs = check.executeQuery();
            if (!rs.next()) {
                System.err.println("ASIN nicht gefunden in item-Tabelle: " + asin);
                return;
            }
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO item_kategorie (asin, kategorie_id) VALUES (?, ?) ON CONFLICT DO NOTHING")) {
            ps.setString(1, asin);
            ps.setInt(2, categoryId);
            ps.executeUpdate();
            System.out.println("ASIN " + asin + " Kategorie zugeordnet (Kategorie-ID: " + categoryId + ")");
        }
    }
}
