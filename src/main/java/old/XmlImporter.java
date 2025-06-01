package old;

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

            NodeList produktList = doc.getElementsByTagName("item");
            System.out.println("-- Datei gelesen: " + filePath);
            System.out.println("> Starte Verarbeitung ...");
            System.out.println("> Anzahl Produkte gefunden: " + produktList.getLength());

            for (int i = 0; i < produktList.getLength(); i++) {
                Node node = produktList.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    Element element = (Element) node;
                    String produktnr = element.getAttribute("asin").trim();
                    String titel = getTextContent(element, "title").trim();
                    String verkaufsrang = element.getAttribute("salesrank").trim();
                    String rating = element.getAttribute("rating").trim();

                    if (produktnr.isEmpty()) {
                        logError(conn, "Produkt", "UNKNOWN", "produktnr", "Produktnummer fehlt");
                        continue;
                    }
                    if (titel.isEmpty()) {
                        logError(conn, "Produkt", produktnr, "titel", "Titel fehlt");
                        continue;
                    }

                    String bild = null;
                    Node detailsNode = element.getElementsByTagName("details").item(0);
                    if (detailsNode != null && detailsNode.getNodeType() == Node.ELEMENT_NODE) {
                        bild = ((Element) detailsNode).getAttribute("img").trim();
                    }

                    String ean = getTextContent(element, "ean").trim();
                    String detailseite = getTextContent(element, "details").trim();
                    String originalTyp = element.getAttribute("pgroup").trim();
                    String typ = originalTyp.toUpperCase();

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
                                logError(conn, "Produkt", produktnr, "pgroup", "Unbekannter Typ: " + typ);
                            }
                            typ = null;
                    }

                    if (typ == null)
                        continue;

                    System.out.println("> Verarbeite Produkt: " + produktnr + " | Titel: " + titel);

                    try {
                        PreparedStatement stmt = conn.prepareStatement(
                                "INSERT INTO produkt (produktnr, titel, typ, verkaufsrang, rating, bild, ean, detailseite, erscheinungsdatum) "
                                        +
                                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (produktnr) DO NOTHING");
                        stmt.setString(1, produktnr);
                        stmt.setString(2, titel);
                        stmt.setString(3, typ);
                        stmt.setInt(4, verkaufsrang.isEmpty() ? 0 : Integer.parseInt(verkaufsrang));
                        stmt.setFloat(5, rating.isEmpty() ? 0f : Float.parseFloat(rating));
                        stmt.setString(6, bild.isEmpty() ? null : bild);
                        stmt.setString(7, ean.isEmpty() ? null : ean);
                        stmt.setString(8, detailseite.isEmpty() ? null : detailseite);

                        String released = getTextContent(element, "releasedate").trim();
                        java.sql.Date erscheinungsdatum = null;
                        if (!released.isEmpty()) {
                            try {
                                erscheinungsdatum = java.sql.Date.valueOf(released);
                            } catch (IllegalArgumentException e) {
                                logError(conn, "Produkt", produktnr, "erscheinungsdatum",
                                        "Ungültiges Datum: " + released);
                            }
                        }

                        if (erscheinungsdatum != null) {
                            stmt.setDate(9, erscheinungsdatum);
                        } else {
                            stmt.setNull(9, Types.DATE);
                        }

                        stmt.executeUpdate();
                        System.out.println("→ Produkt eingetragen: " + produktnr);

                        // === CD ===
                        if ("CD".equals(typ)) {
                            String genre = getTextContent(element, "genre").trim();
                            String label = getTextContent(element, "label").trim();

                            PreparedStatement cdStmt = conn.prepareStatement(
                                    "INSERT INTO cd (produktnr, label, genre, erscheinungsdatum) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING");
                            cdStmt.setString(1, produktnr);
                            cdStmt.setString(2, label.isEmpty() ? null : label);
                            cdStmt.setString(3, genre.isEmpty() ? null : genre);
                            if (erscheinungsdatum != null) {
                                cdStmt.setDate(4, erscheinungsdatum);
                            } else {
                                cdStmt.setNull(4, Types.DATE);
                            }
                            cdStmt.executeUpdate();
                            System.out.println("   → CD eingetragen: " + produktnr);
                        }

                        // === DVD ===
                        if ("DVD".equals(typ)) {
                            try {
                                Element dvdSpec = (Element) element.getElementsByTagName("dvdspec").item(0);
                                String format = getTextContent(dvdSpec, "format");
                                String laufzeitStr = getTextContent(dvdSpec, "laufzeit");
                                String regionCode = getTextContent(dvdSpec, "regioncode");
                                String fsk = getTextContent(dvdSpec, "fsk");
                                String sprache = getTextContent(dvdSpec, "sprache");

                                Integer laufzeit = null;
                                try {
                                    laufzeit = laufzeitStr.isEmpty() ? null : Integer.parseInt(laufzeitStr);
                                } catch (NumberFormatException e) {
                                    logError(conn, "DVD", produktnr, "dauer", "Ungültige Zahl: " + laufzeitStr);
                                }

                                PreparedStatement dvdStmt = conn.prepareStatement(
                                        "INSERT INTO dvd (produktnr, format, dauer, region_code, fsk, sprache) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING");
                                dvdStmt.setString(1, produktnr);
                                dvdStmt.setString(2, format.isEmpty() ? null : format);
                                if (laufzeit != null) {
                                    dvdStmt.setInt(3, laufzeit);
                                } else {
                                    dvdStmt.setNull(3, Types.INTEGER);
                                }
                                dvdStmt.setString(4, regionCode.isEmpty() ? null : regionCode);
                                dvdStmt.setString(5, fsk.isEmpty() ? null : fsk);
                                dvdStmt.setString(6, sprache.isEmpty() ? null : sprache);
                                dvdStmt.executeUpdate();
                                System.out.println("   → DVD eingetragen: " + produktnr);
                            } catch (Exception e) {
                                logError(conn, "DVD", produktnr, "ALLGEMEIN",
                                        "Fehler beim Einfügen: " + e.getMessage());
                            }
                        }

                        // === BUCH ===
                        if ("BUCH".equals(typ)) {
                            try {
                                String isbn = getTextContent(element, "isbn").trim();
                                String seitenStr = getTextContent(element, "seitenzahl").trim();
                                String verlag = getTextContent(element, "verlag").trim();
                                String auflage = getTextContent(element, "auflage").trim();
                                String bindung = getTextContent(element, "bindung").trim();

                                Integer seitenzahl = null;
                                try {
                                    seitenzahl = seitenStr.isEmpty() ? null : Integer.parseInt(seitenStr);
                                } catch (NumberFormatException e) {
                                    logError(conn, "BUCH", produktnr, "seitenzahl", "Ungültige Zahl: " + seitenStr);
                                }

                                PreparedStatement buchStmt = conn.prepareStatement(
                                        "INSERT INTO buch (produktnr, isbn, seitenzahl, erscheinungsdatum, verlag, auflage, bindung) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING");
                                buchStmt.setString(1, produktnr);
                                buchStmt.setString(2, isbn.isEmpty() ? null : isbn);
                                if (seitenzahl != null) {
                                    buchStmt.setInt(3, seitenzahl);
                                } else {
                                    buchStmt.setNull(3, Types.INTEGER);
                                }
                                if (erscheinungsdatum != null) {
                                    buchStmt.setDate(4, erscheinungsdatum);
                                } else {
                                    buchStmt.setNull(4, Types.DATE);
                                }
                                buchStmt.setString(5, verlag.isEmpty() ? null : verlag);
                                buchStmt.setString(6, auflage.isEmpty() ? null : auflage);
                                buchStmt.setString(7, bindung.isEmpty() ? null : bindung);
                                buchStmt.executeUpdate();
                                System.out.println("   → BUCH eingetragen: " + produktnr);
                            } catch (Exception e) {
                                logError(conn, "BUCH", produktnr, "ALLGEMEIN",
                                        "Fehler beim Einfügen: " + e.getMessage());
                            }
                        }

                    } catch (SQLException e) {
                        logError(conn, "Produkt", produktnr, "ALLGEMEIN", "SQL-Fehler: " + e.getMessage());
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
        return (list.getLength() > 0) ? list.item(0).getTextContent().trim() : "";
    }

    private static void logError(Connection conn, String entity, String entityId, String attribut, String fehler) {
        try (PreparedStatement stmt = conn.prepareStatement(
                "INSERT INTO import_errors (entity, entity_id, attribut, fehler) VALUES (?, ?, ?, ?)")) {
            stmt.setString(1, entity);
            stmt.setString(2, entityId);
            stmt.setString(3, attribut);
            stmt.setString(4, fehler);
            stmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("!! Fehler beim Schreiben in import_errors für " + entity + " (" + entityId + ")");
            e.printStackTrace();
        }
    }
}
