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
    private static int categoryCounter = 1;
    private static final Map<String, Integer> categoryMap = new HashMap<>();

    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:5432/postgres";
        String user = "postgres";
        String password = "postgres";

        try (Connection conn = DriverManager.getConnection(url, user, password)) {

            File xmlFile = new File("data", "categories.xml");
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(xmlFile);
            doc.getDocumentElement().normalize();

            for (int i = 0; i < 3; i++) System.out.println("");
            System.out.println("Starte Import der Kategorien...");
            for (int i = 0; i < 3; i++) System.out.println("");

            NodeList categories = doc.getDocumentElement().getChildNodes();
            for (int i = 0; i < categories.getLength(); i++) {
                Node node = categories.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE && node.getNodeName().equals("category")) {
                    processCategory((Element) node, null, conn);
                }
            }

            System.out.println("Kategorieimport abgeschlossen.");
            for (int i = 0; i < 3; i++) System.out.println("");

        } catch (Exception e) {
            System.err.println("Ein Fehler ist aufgetreten: " + e.getMessage());
        }
    }
}