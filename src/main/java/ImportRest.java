import java.io.File;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class ImportRest {

    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:5432/postgres";
        String user = "postgres";
        String password = "Robin2504!";

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            // Tabellen resetten
            Statement stmt = conn.createStatement();
            // --- Drop all tables and recreate schema ---
            stmt.executeUpdate(
                "DROP TABLE IF EXISTS item_kategorie,item_person,item_label,item_publisher," +
                "item_studio,item_listmania,similar_product,item_track,track,item_audiotext," +
                "audiotext,bookspec,musicspec,dvdspec,kategorie,publisher,label,studio,listmania," +
                "person,illegal_data,item,kunde,bestellung,bestellposition,rezension,angebot,shop CASCADE;"
            );

            // Shop
            stmt.executeUpdate(
                "CREATE TABLE shop (" +
                "    shop_id SERIAL PRIMARY KEY," +
                "    name VARCHAR(50) UNIQUE," +
                "    street VARCHAR(50)," +
                "    zip VARCHAR(20));"
            );

            // Item (gemeinsam für Bücher, DVDs, Music)
            stmt.executeUpdate(
                "CREATE TABLE item (" +
                "    shop_id INT REFERENCES shop(shop_id) ON DELETE SET NULL," +
                "    asin VARCHAR(40) PRIMARY KEY," +
                "    pgroup VARCHAR(40) NOT NULL CHECK (pgroup IN ('Book', 'DVD', 'Music'))," +
                "    title TEXT NOT NULL," +
                "    salesrank INT," +
                "    picture TEXT," +
                "    detailpage TEXT," +
                "    ean VARCHAR(60)," +
                "    price DECIMAL(5,2) NOT NULL," +
                "    item_status VARCHAR(20)," +
                "    currency VARCHAR(10) NOT NULL);"
            );

            // Buch
            stmt.executeUpdate(
                "CREATE TABLE bookspec (" +
                "    asin VARCHAR(40) PRIMARY KEY REFERENCES item(asin) ON DELETE CASCADE," +
                "    binding VARCHAR(80)," +
                "    edition VARCHAR(80)," +
                "    isbn VARCHAR(40) NOT NULL," +
                "    weight VARCHAR(30)," +
                "    height VARCHAR(30)," +
                "    length VARCHAR(30)," +
                "    pages INT," +
                "    publication_date DATE);"
            );

            // Musiks
            stmt.executeUpdate(
                "CREATE TABLE musicspec (" +
                "    asin VARCHAR(40) PRIMARY KEY REFERENCES item(asin) ON DELETE CASCADE," +
                "    binding VARCHAR(50)," +
                "    format VARCHAR(150)," +
                "    num_discs INT," +
                "    releasedate DATE," +
                "    upc VARCHAR(40));"
            );

            // DVD
            stmt.executeUpdate(
                "CREATE TABLE dvdspec (" +
                "    asin VARCHAR(40) PRIMARY KEY REFERENCES item(asin) ON DELETE CASCADE," +
                "    aspectratio VARCHAR(80)," +
                "    format VARCHAR(100)," +
                "    regioncode INT," +
                "    releasedate DATE," +
                "    runningtime INT," +
                "    theatr_release INT," +
                "    upc VARCHAR(60));"
            );

            // Tracks (für Music-CDs)
            stmt.executeUpdate(
                "CREATE TABLE track (" +
                "    track_id SERIAL PRIMARY KEY," +
                "    name TEXT);"
            );
            stmt.executeUpdate(
                "CREATE TABLE item_track (" +
                "    asin VARCHAR(40) REFERENCES item(asin) ON DELETE CASCADE," +
                "    track_id INT REFERENCES track(track_id) ON DELETE CASCADE," +
                "    PRIMARY KEY (asin, track_id));"
            );

            // Similar Products
            stmt.executeUpdate(
                "CREATE TABLE similar_product (" +
                "    sim_id SERIAL PRIMARY KEY," +
                "    asin VARCHAR(40) REFERENCES item(asin) ON DELETE CASCADE," +
                "    sim_asin VARCHAR(40));"
            );

            // Audiotext (Mehrsprachige Angaben)
            stmt.executeUpdate(
                "CREATE TABLE audiotext (" +
                "    audiotext_id SERIAL PRIMARY KEY," +
                "    lang_type VARCHAR(50)," +
                "    language VARCHAR(50)," +
                "    audioformat VARCHAR(100));"
            );
            stmt.executeUpdate(
                "CREATE TABLE item_audiotext (" +
                "    asin VARCHAR(40) REFERENCES item(asin) ON DELETE CASCADE," +
                "    audiotext_id INT REFERENCES audiotext(audiotext_id) ON DELETE CASCADE," +
                "    PRIMARY KEY (asin, audiotext_id));"
            );

            // Publisher
            stmt.executeUpdate(
                "CREATE TABLE publisher (" +
                "    publisher_id SERIAL PRIMARY KEY," +
                "    name VARCHAR(100));"
            );
            stmt.executeUpdate(
                "CREATE TABLE item_publisher (" +
                "    asin VARCHAR(20) REFERENCES item(asin) ON DELETE CASCADE," +
                "    publisher_id INT REFERENCES publisher(publisher_id) ON DELETE CASCADE," +
                "    PRIMARY KEY (asin, publisher_id));"
            );

            // Label
            stmt.executeUpdate(
                "CREATE TABLE label (" +
                "    label_id SERIAL PRIMARY KEY," +
                "    name VARCHAR(100));"
            );
            stmt.executeUpdate(
                "CREATE TABLE item_label (" +
                "    asin VARCHAR(20) REFERENCES item(asin) ON DELETE CASCADE," +
                "    label_id INT REFERENCES label(label_id) ON DELETE CASCADE," +
                "    PRIMARY KEY (asin, label_id));"
            );

            // Studio
            stmt.executeUpdate(
                "CREATE TABLE studio (" +
                "    studio_id SERIAL PRIMARY KEY," +
                "    name VARCHAR(100));"
            );
            stmt.executeUpdate(
                "CREATE TABLE item_studio (" +
                "    asin VARCHAR(20) REFERENCES item(asin) ON DELETE CASCADE," +
                "    studio_id INT REFERENCES studio(studio_id) ON DELETE CASCADE," +
                "    PRIMARY KEY (asin, studio_id));"
            );

            // Listmania
            stmt.executeUpdate(
                "CREATE TABLE listmania (" +
                "    list_id SERIAL PRIMARY KEY," +
                "    name TEXT);"
            );
            stmt.executeUpdate(
                "CREATE TABLE item_listmania (" +
                "    asin VARCHAR(20) REFERENCES item(asin) ON DELETE CASCADE," +
                "    list_id INT REFERENCES listmania(list_id) ON DELETE CASCADE," +
                "    PRIMARY KEY (asin, list_id));"
            );

            // Person rolle nur bei item_person
            stmt.executeUpdate(
                "CREATE TABLE person (" +
                "    person_id SERIAL PRIMARY KEY," +
                "    name VARCHAR(100));"
            );
            stmt.executeUpdate(
                "CREATE TABLE item_person (" +
                "    asin VARCHAR(40) REFERENCES item(asin) ON DELETE CASCADE," +
                "    person_id INT REFERENCES person(person_id) ON DELETE CASCADE," +
                "    person_role VARCHAR(20)," +
                "    PRIMARY KEY (asin, person_id));"
            );

            // Kategorie
            stmt.executeUpdate(
                "CREATE TABLE kategorie (" +
                "    kategorie_id SERIAL PRIMARY KEY," +
                "    name TEXT NOT NULL," +
                "    eltern_id INT REFERENCES kategorie(kategorie_id) ON DELETE CASCADE);"
            );
            stmt.executeUpdate(
                "CREATE TABLE item_kategorie (" +
                "    asin VARCHAR(40) REFERENCES item(asin) ON DELETE CASCADE," +
                "    kategorie_id INT REFERENCES kategorie(kategorie_id) ON DELETE SET NULL," +
                "    PRIMARY KEY (asin, kategorie_id));"
            );

            // Illegal Data
            stmt.executeUpdate(
                "CREATE TABLE illegal_data (" +
                "    illegal_id SERIAL PRIMARY KEY," +
                "    asin VARCHAR(40)," +
                "    pgroup VARCHAR(40)," +
                "    title TEXT," +
                "    error_message TEXT," +
                "    import_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"
            );

            // Kunden und Bestellungen
            stmt.executeUpdate(
                "CREATE TABLE kunde (" +
                "    kunden_id SERIAL PRIMARY KEY," +
                "    username TEXT UNIQUE NOT NULL," +
                "    mail TEXT UNIQUE," +
                "    adresse TEXT," +
                "    konto_nr TEXT);"
            );
            stmt.executeUpdate(
                "CREATE TABLE bestellung (" +
                "    bestellung_id SERIAL PRIMARY KEY," +
                "    kunden_id INT REFERENCES kunde(kunden_id) ON DELETE CASCADE," +
                "    kaufdatum TIMESTAMP);"
            );
            stmt.executeUpdate(
                "CREATE TABLE bestellposition (" +
                "    bestellung_id INT REFERENCES bestellung(bestellung_id) ON DELETE CASCADE," +
                "    asin VARCHAR(40) REFERENCES item(asin) ON DELETE CASCADE," +
                "    PRIMARY KEY (bestellung_id, asin));"
            );
            stmt.executeUpdate(
                "CREATE TABLE rezension (" +
                "    rezension_id SERIAL PRIMARY KEY," +
                "    kunden_id INT REFERENCES kunde(kunden_id) ON DELETE CASCADE," +
                "    asin VARCHAR(40) REFERENCES item(asin) ON DELETE CASCADE," +
                "    bewertung INT CHECK (bewertung BETWEEN 1 AND 5)," +
                "    text TEXT," +
                "    rezensionsdatum TIMESTAMP);"
            );

            // Angebot je Shop
            stmt.executeUpdate(
                "CREATE TABLE angebot (" +
                "    shop_id INT REFERENCES shop(shop_id) ON DELETE CASCADE," +
                "    asin VARCHAR(40) REFERENCES item(asin) ON DELETE CASCADE," +
                "    preis DECIMAL(5,2)," +
                "    verfuegbar BOOLEAN," +
                "    zustand TEXT," +
                "    PRIMARY KEY (shop_id, asin));"
            );
            // extrahiere aus Files
            File leipzigFile = new File("data", "leipzig_transformed.xml");
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document leipzigDoc = builder.parse(leipzigFile);
            leipzigDoc.getDocumentElement().normalize();

            File dresdenFile = new File("data", "dresden.xml");
            Document dresdenDoc = builder.parse(dresdenFile);
            dresdenDoc.getDocumentElement().normalize();

            List<Document> Shops = new ArrayList<>();
            Shops.add(leipzigDoc);
            Shops.add(dresdenDoc);
            int shop_id = 0;

            for (Document activeShop : Shops) {
                Element shopElement = (Element) activeShop.getElementsByTagName("shop").item(0);
                String shopName = shopElement.getAttribute("name");
                String shopStreet = shopElement.getAttribute("street");
                String shopZip = shopElement.getAttribute("zip");
                shop_id++;
                PreparedStatement psShop = conn.prepareStatement("INSERT INTO shop (shop_id, name, street, zip) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING");
                psShop.setInt(1, shop_id);
                psShop.setString(2, shopName);
                psShop.setString(3, shopStreet);
                psShop.setString(4, shopZip);
                psShop.executeUpdate();

                for (int i = 0; i < 3; i++) {System.out.println("");}
                System.out.println("Starte Import für " + shopName +"...");
                for (int i = 0; i < 3; i++) {System.out.println("");}

                // Filtere nur Haupt-<item>-Elemente (direkte Kinder des Wurzelelements)
                NodeList allItems = activeShop.getElementsByTagName("item");
                List<Element> productList = new ArrayList<>();
                for (int i = 0; i < allItems.getLength(); i++) {
                    Element item = (Element) allItems.item(i);
                    if (item.getParentNode().equals(activeShop.getDocumentElement())) {
                        productList.add(item);
                    }
                }
                for (Element product : productList) {
                    String asin = product.getAttribute("asin");
                    if (asin.isEmpty() || asin.length() > 20) {
                        logIllegal(product, "ASIN ungültig oder leer", conn);
                        continue;
                    }
                   


                    String pgroup = product.getAttribute("pgroup").trim();
                    if (pgroup.matches("(?i)AUDIO-CD|CD|MUSIKKASSETTE|VINYL  LP|Musical|Music")) {
                        pgroup = "Music";
                    } else if (pgroup.matches("(?i)DVD + CD|DVD")) {
                        pgroup = "DVD";
                    } else if (pgroup.matches("(?i)Buch|Book")) {
                        pgroup = "Book";
                    } else if (pgroup.isEmpty()) {
                        boolean hasTracks = product.getElementsByTagName("tracks").getLength() > 0;
                        boolean hasBookSpec = product.getElementsByTagName("bookspec").getLength() > 0;
                        boolean hasDvdSpec = product.getElementsByTagName("dvdspec").getLength() > 0;
                        boolean hasMusicSpec = product.getElementsByTagName("musicspec").getLength() > 0;
                        boolean hasAuthors = !getNamesForRole(product, "authors", "author").isEmpty();
                        boolean hasActors = !getNamesForRole(product, "actors", "actor").isEmpty();

                        if (hasBookSpec || hasAuthors)
                            pgroup = "Book";
                        else if (hasDvdSpec || hasActors)
                            pgroup = "DVD";
                        else if (hasMusicSpec || hasTracks)
                            pgroup = "Music";
                        else {
                            logIllegal(product, "pgroup ungültig oder leer", conn);
                            continue;
                        }

                    }
                    
                    System.out.println("Verarbeite nun PGROUP: " + pgroup + " fuer ASIN: " + asin);

                    switch (pgroup) {
                        case "Book": {
                            Element bookspec = (Element) product.getElementsByTagName("bookspec").item(0);
                            Element isbnele = (Element) bookspec.getElementsByTagName("isbn").item(0);
                            String isbn = isbnele.getAttribute("val");
                            if (isbn.isEmpty() || 10 > isbn.length() || isbn.length() > 13) {
                                logIllegal(product, "Isbn leer oder ungültig", conn);
                                continue;
                            }
                            break;
                        }
                        case "DVD": {
                            Element dvdspec = (Element) product.getElementsByTagName("dvdspec").item(0);
                            Element upcElement = (Element) dvdspec.getElementsByTagName("upc").item(0);
                            String regionStr = getText(dvdspec, "regioncode");
                            int region;
                            if (upcElement != null && upcElement.hasAttribute("val")) {
                                String upc = upcElement.getAttribute("val");
                                if (upc.length() != 8 && upc.length() != 12) {
                                    logIllegal(product, "upc ungültig", conn);
                                    continue;
                                }
                            } else if (!regionStr.isEmpty()) {
                                region = Integer.parseInt(regionStr);
                                if (region < 0 || region > 8) {
                                    logIllegal(product, "Region ungültig", conn);
                                    continue;
                                }
                            }
                            break;
                        }
                        case "Music": {
                            Element musicspec = (Element) product.getElementsByTagName("musicspec").item(0);
                            String upc = getText(musicspec, "upc");
                            if (upc != null && !upc.isEmpty() && (upc.length() != 8 && upc.length() != 12)) {
                                logIllegal(product, "upc ungültig", conn);
                                continue;
                            }
                            break;
                        }
                        default:
                            break;
                    }

                    String title = getText(product, "title");
                    if (title.isEmpty()) {
                        logIllegal(product, "Titel leer", conn);
                        continue;
                    }


                    int salesrank = 0;
                    try {
                        salesrank = Integer.parseInt(product.getAttribute("salesrank"));
                    } catch (NumberFormatException ignored) {
                    }

                    double price = 0.0;
                    String pricestate = null, pricecurrency = null;
                    NodeList priceNodes = product.getElementsByTagName("price");
                    if (priceNodes.getLength() > 0) {
                        Element priceElement = (Element) priceNodes.item(0);
                        try {
                            double mult = priceElement.hasAttribute("mult")
                                    ? Double.parseDouble(priceElement.getAttribute("mult"))
                                    : 0.01;
                            int basePrice = Integer.parseInt(priceElement.getTextContent().trim());
                            price = basePrice * mult;
                        } catch (NumberFormatException ignored) {
                        }
                        pricestate = priceElement.getAttribute("state");
                        pricecurrency = priceElement.getAttribute("currency");
                    }
                    if (price <= 0.0 || pricecurrency == null || pricecurrency.isEmpty()){
                        logIllegal(product, "Preis oder Währung fehlen", conn);
                        continue;
                    }

                    String picture = product.getAttribute("picture");
                    if (picture.isEmpty())
                        picture = "No Picture";

                    String detailPage = product.getAttribute("detailpage");
                    if (detailPage.isEmpty())
                        detailPage = "No Detail Page";

                    String ean = product.getAttribute("ean");
                    if(ean.length() != 8 && ean.length() != 13) {
                        logIllegal(product, "EAN ungültig oder leer", conn);
                        continue;
                        }
                    
                    PreparedStatement psItem = conn.prepareStatement(
                            "INSERT INTO item ( asin, pgroup, title, salesrank, price, picture, detailpage,"
                                    + "ean, item_status, currency, shop_id) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (asin) DO NOTHING");
                    psItem.setString(1, asin);
                    psItem.setString(2, pgroup);
                    psItem.setString(3, title);
                    psItem.setInt(4, salesrank);
                    psItem.setDouble(5, price);
                    psItem.setString(6, picture);
                    psItem.setString(7, detailPage);
                    psItem.setString(8, ean);
                    psItem.setString(9, pricestate);
                    psItem.setString(10, pricecurrency);
                    psItem.setInt(11, shop_id);
                    psItem.executeUpdate();

                    System.out.println("ASIN: " + asin + " mit PGROUP: " + pgroup + " erfolgreich in die Datenbank eingefügt.");

                    switch (pgroup) {
                        case "Book":
                            insertBook(conn, product, asin);
                            insertRest(conn, product, asin);
                            break;
                        case "DVD":
                            insertDVD(conn, product, asin);
                            insertRest(conn, product, asin);
                            break;
                        case "Music":
                            insertMusic(conn, product, asin);
                            insertRest(conn, product, asin);
                            break;
                    }
                }

                for (int i = 0; i < 3; i++) {
                    System.out.println("");
                }
                System.out.println("Import abgeschlossen. "+ shopName +" mit " + productList.size() + " Produkten.");
                for (int i = 0; i < 3; i++) {
                    System.out.println("");
                }
            }
        } catch (Exception e) {
            System.err.println("An error occurred: " + e.getMessage());
        }
    }

    /**
     * Fügt die Buchspezifikationen in die Datenbank ein.
     * @param conn Die Datenbankverbindung
     * @param product Das Produkt-Element
     * @param asin Die ASIN des Produkts
     * @throws Exception Wenn ein Fehler auftritt
     */
    static void insertBook(Connection conn, Element product, String asin) throws Exception {
        // bookspecs
        Element bookspec = (Element) product.getElementsByTagName("bookspec").item(0);
        if (bookspec == null) {
            System.err.println("Kein <bookspec> gefunden für ASIN: " + asin);
            return;
        }

        // ISBN
        Element isbnele = (Element) bookspec.getElementsByTagName("isbn").item(0);
        String isbn = isbnele.getAttribute("val");
        if (isbn.isEmpty()) {
            logIllegal(product, "Isbn leer", conn);

        }

        // Binding
        String binding = getText(product, "binding");
        if (binding.isEmpty()) {
            binding = "Unknown";
        }

        // edition
        Element editionele = (Element) bookspec.getElementsByTagName("edition").item(0);
        String edition = editionele.getAttribute("val");
        if (edition.isEmpty()) {
            edition = "Unknown";
        }

        // Seitenanzahl
        String pagesStr = getText(product, "pages");
        int pages = 0;
        if (!pagesStr.isEmpty()) {
            try {
                pages = Integer.parseInt(pagesStr);
            } catch (NumberFormatException e) {
                pages = 0;
            }
        }

        // maße
        Element packageele = (Element) bookspec.getElementsByTagName("package").item(0);
        String weight = packageele.getAttribute("weight");
        String height = packageele.getAttribute("height");
        String length = packageele.getAttribute("length");
        if (weight.isEmpty()) {
            weight = "/";
        }
        if (height.isEmpty()) {
            height = "/";
        }
        if (length.isEmpty()) {
            length = "/";
        }

        // Veröffentlichungsdatum
        List<String> pubDateList = getAttributeList(bookspec, "publication", "date");
        LocalDate pubDate = null;
        for (String pubdateStr : pubDateList) {
            if (!pubdateStr.isEmpty()) {
                try {
                    pubDate = LocalDate.parse(pubdateStr);
                    break;
                } catch (Exception e) {
                    System.err.println("Ungültiges Veröffentlichungsdatum für ASIN " + asin + ": " + pubdateStr);
                }
            }
        }

        PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO bookspec (asin, binding, edition, isbn, pages, publication_date, height, weight, length)VALUES" +
            "(?, ?, ?, ?, ?, ?, ?, ?, ?) On CONFLICT (asin) DO NOTHING"
        );
        ps.setString(1, asin);
        ps.setString(2, binding);
        ps.setString(3, edition);
        ps.setString(4, isbn);
        ps.setInt(5, pages);
        if (pubDate != null) {
            ps.setDate(6, Date.valueOf(pubDate));
        } else {
            ps.setNull(6, Types.DATE);
        }
        ps.setString(7, height);
        ps.setString(8, weight);
        ps.setString(9, length);
        ps.executeUpdate();
    }

    /**
     * Fügt die DVD-Spezifikationen in die Datenbank ein.
     * @param conn Die Datenbankverbindung
     * @param product Das Produkt-Element
     * @param asin Die ASIN des Produkts
     * @throws Exception Wenn ein Fehler auftritt
     */
    static void insertDVD(Connection conn, Element product, String asin) throws Exception {
        // dvdspecs
        Element dvdspecs = (Element) product.getElementsByTagName("dvdspec").item(0);
        if (dvdspecs == null) {
            System.err.println("Kein <dvdspec> gefunden für ASIN: " + asin);
            return;
        }

        // Format (Textinhalt, kein Attribut!)
        String format = getText(dvdspecs, "format");
        if (format.isEmpty()) {
            format = "Unknown";
        }

        // Regioncode
        String regionStr = getText(dvdspecs, "regioncode");
        int region;
        try {
            region = Integer.parseInt(regionStr);
        } catch (NumberFormatException e) {
            region = 0;
        }

        // Laufzeit (runningtime!)
        String runtimeStr = getText(dvdspecs, "runningtime");
        int runtime;
        try {
            runtime = Integer.parseInt(runtimeStr);
        } catch (NumberFormatException e) {
            runtime = 0;
        }

        // UPC (Attribut val!)
        Element upcElement = (Element) dvdspecs.getElementsByTagName("upc").item(0);
        String upc = "Unknown";
        if (upcElement != null && upcElement.hasAttribute("val")) {
            upc = upcElement.getAttribute("val");
        }

        // Aspect Ratio (Kleingeschrieben!)
        String aspectratio = getText(dvdspecs, "aspectratio");
        if (aspectratio.isEmpty()) {
            aspectratio = "Unknown";
        }

        // Veröffentlichungsdatum
        String pubDateStr = getText(dvdspecs, "releasedate");
        LocalDate pubDate = null;
        if (!pubDateStr.isEmpty()) {
            pubDate = LocalDate.parse(pubDateStr);
        }

        // Theatr. Release
        String theatrReleaseStr = getText(dvdspecs, "theatr_release");
        int theatr_elease;
        try {
            theatr_elease = Integer.parseInt(theatrReleaseStr);
        } catch (NumberFormatException e) {
            theatr_elease = 0;
        }

        PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO dvdspec (asin, format, regioncode, runningtime, releasedate, aspectratio, upc, theatr_release)" +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (asin) DO NOTHING"
        );
        ps.setString(1, asin);
        ps.setString(2, format);
        ps.setInt(3, region);
        ps.setInt(4, runtime);
        if (pubDate != null) {
            ps.setDate(5, Date.valueOf(pubDate));
        } else {
            ps.setNull(5, Types.DATE);
        }
        ps.setString(6, aspectratio);
        ps.setString(7, upc);
        ps.setInt(8, theatr_elease);
        ps.executeUpdate();
    }

    /**
     * Fügt die Musik-Spezifikationen in die Datenbank ein.
     * @param conn Die Datenbankverbindung
     * @param product Das Produkt-Element
     * @param asin Die ASIN des Produkts
     * @throws Exception Wenn ein Fehler auftritt
     */
    static void insertMusic(Connection conn, Element product, String asin) throws Exception {
        // musicspecs
        Element musicspecs = (Element) product.getElementsByTagName("musicspec").item(0);
        if (musicspecs == null) {
            System.err.println("Kein <bookspec> gefunden für ASIN: " + asin);
            return;
        }

        // Binding
        String binding = getText(musicspecs, "binding");
        if (binding.isEmpty()) {
            binding = "Unknown";
        }

        // Format
        Element formatele = (Element) musicspecs.getElementsByTagName("format").item(0);
        String format = formatele.getAttribute("value");
        if (format.isEmpty()) {
            format = "Unknown";
        }

        // Anzahl der Discs
        String numDiscsStr = getText(musicspecs, "num_discs");
        int numDiscs = 1; // Default-Wert
        if (!numDiscsStr.isEmpty()) {
            try {
                numDiscs = Integer.parseInt(numDiscsStr);
            } catch (NumberFormatException e) {
                System.err.println("Ungültige num_discs für ASIN " + asin + ", setze auf 1.");
            }
        }

        // Veröffentlichungsdatum
        String pubDateStr = getText(musicspecs, "releasedate");
        LocalDate pubDate = null;
        if (!pubDateStr.isEmpty()) {
            pubDate = LocalDate.parse(pubDateStr);
        }

        // UPC (Universal Product Code)
        String upc = getText(musicspecs, "upc");
        if (upc.isEmpty()) {
            upc = "Unknown";
        }

        PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO musicspec (asin, binding, format, releasedate, upc, num_discs) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (asin) DO NOTHING"
        );
        ps.setString(1, asin);
        ps.setString(2, binding);
        ps.setString(3, format);
        if (pubDate != null) {
            ps.setDate(4, Date.valueOf(pubDate));
        } else {
            ps.setNull(4, Types.DATE);
        }
        ps.setString(5, upc);
        ps.setInt(6, numDiscs);
        ps.executeUpdate();
    }

    /**
     * Fügt die restlichen Informationen (Labels, Tracks, Listmania, Studios, etc.) in die Datenbank ein.
     * @param conn Die Datenbankverbindung
     * @param product Das Produkt-Element
     * @param asin Die ASIN des Produkts
     * @throws Exception Wenn ein Fehler auftritt
     */
    static void insertRest(Connection conn, Element product, String asin) throws Exception {
        // label
        List<String> labelNames = getAttributeList(product, "label", "name");
        for (String labelName : labelNames) {
            if (!labelName.isEmpty()) {
                int labelId = getOrCreate(conn, "label", labelName);
                PreparedStatement pslabel = conn.prepareStatement("INSERT INTO item_label (asin, label_id) VALUES (?, ?) ON CONFLICT DO NOTHING");
                pslabel.setString(1, asin);
                pslabel.setInt(2, labelId);
                pslabel.executeUpdate();
            }
        }

        // track
        NodeList trackTitles = ((Element) product.getElementsByTagName("tracks").item(0)).getElementsByTagName("title");
        for (int t = 0; t < trackTitles.getLength(); t++) {
            String trackTitle = trackTitles.item(t).getTextContent().trim();
            if (!trackTitle.isEmpty()) {
                int track_id = getOrCreate(conn, "track", trackTitle);
                PreparedStatement pstrack = conn.prepareStatement("INSERT INTO item_track (asin, track_id) VALUES (?, ?) ON CONFLICT DO NOTHING");
                pstrack.setString(1, asin);
                pstrack.setInt(2, track_id);
                pstrack.executeUpdate();
            }
        }

        // listmania (Kategorien????????)
        Element listmaniaElement = (Element) product.getElementsByTagName("listmania").item(0);
        List<String> listmaniaNames = new ArrayList<>();
        if (listmaniaElement != null) {
            listmaniaNames = getAttributeList(listmaniaElement, "list", "name");
        }
        for (String listmaniaName : listmaniaNames) {
            if (!listmaniaName.isEmpty()) {
                int listmaniaId = getOrCreateListmania(conn, listmaniaName);
                PreparedStatement pslistmania = conn.prepareStatement("INSERT INTO item_listmania (asin, list_id) VALUES (?, ?) ON CONFLICT DO NOTHING");
                pslistmania.setString(1, asin);
                pslistmania.setInt(2, listmaniaId);
                pslistmania.executeUpdate();
            }
        }

        // Studios
        Element studiosElement = (Element) product.getElementsByTagName("studios").item(0);
        if (studiosElement != null) {
            List<String> studioNames = getAttributeList(studiosElement, "studio", "name");
            for (String studioName : studioNames) {
                if (!studioName.isEmpty()) {
                    int studioId = getOrCreate(conn, "studio", studioName);
                    try (PreparedStatement ps = conn.prepareStatement("INSERT INTO item_studio (asin, studio_id) VALUES (?, ?) ON CONFLICT DO NOTHING")) {
                        ps.setString(1, asin);
                        ps.setInt(2, studioId);
                        ps.executeUpdate();
                    }
                }
            }
        }

        // Ähnliche Produkte
        List<Map<String, String>> similarProducts = getSimilarProducts(product);
        for (Map<String, String> simProduct : similarProducts) {
            String simAsin = simProduct.get("asin");
            if (simAsin != null && !simAsin.isEmpty()) {
                PreparedStatement pssim = conn.prepareStatement("INSERT INTO similar_product (asin, sim_asin) VALUES (?, ?) ON CONFLICT DO NOTHING");
                pssim.setString(1, asin);
                pssim.setString(2, simAsin);
                pssim.executeUpdate();
            }
        }

        // Personen (Schauspieler, Künstler, Autor, etc.)
        // Actor
        List<String> actorNames = getNamesForRole(product, "actors", "actor");
        for (String name : actorNames) {
            if (!name.isEmpty()) {
                int personId = getOrCreate(conn, "person", name);
                try (PreparedStatement ps = conn.prepareStatement("INSERT INTO item_person (asin, person_id, person_role) VALUES (?, ?, 'actor') ON CONFLICT DO NOTHING")) {
                    ps.setString(1, asin);
                    ps.setInt(2, personId);
                    ps.executeUpdate();
                }
            }
        }

        // Artist
        List<String> artistNames = getNamesForRole(product, "artists", "artist");
        for (String name : artistNames) {
            if (!name.isEmpty()) {
                int personId = getOrCreate(conn, "person", name);
                try (PreparedStatement ps = conn.prepareStatement("INSERT INTO item_person (asin, person_id, person_role) VALUES (?, ?, 'artist') ON CONFLICT DO NOTHING")) {
                    ps.setString(1, asin);
                    ps.setInt(2, personId);
                    ps.executeUpdate();
                }
            }
        }
        // Author
        List<String> authorNames = getNamesForRole(product, "authors", "author");
        for (String name : authorNames) {
            if (!name.isEmpty()) {
                int personId = getOrCreate(conn, "person", name);
                try (PreparedStatement ps = conn.prepareStatement("INSERT INTO item_person (asin, person_id, person_role) VALUES (?, ?, 'author') ON CONFLICT DO NOTHING")) {
                    ps.setString(1, asin);
                    ps.setInt(2, personId);
                    ps.executeUpdate();
                }
            }
        }

        // Creator
        List<String> creatorNames = getNamesForRole(product, "creators", "creator");
        for (String name : creatorNames) {
            if (!name.isEmpty()) {
                int personId = getOrCreate(conn, "person", name);
                try (PreparedStatement ps = conn.prepareStatement("INSERT INTO item_person (asin, person_id, person_role) VALUES (?, ?, 'creator') ON CONFLICT DO NOTHING")) {
                    ps.setString(1, asin);
                    ps.setInt(2, personId);
                    ps.executeUpdate();
                }
            }
        }

        // publisher (Verlag)
        List<String> publisherNames = getAttributeList(product, "publisher", "name");
        for (String publisherName : publisherNames) {
            if (!publisherName.isEmpty()) {
                int publisherId = getOrCreate(conn, "publisher", publisherName);
                PreparedStatement pspublisher = conn.prepareStatement("INSERT INTO item_publisher (asin, publisher_id) VALUES (?, ?) ON CONFLICT DO NOTHING");
                pspublisher.setString(1, asin);
                pspublisher.setInt(2, publisherId);
                pspublisher.executeUpdate();
            }
        }

        // Audiotext (Mehrsprachige Angaben)
        NodeList audioTextList = product.getElementsByTagName("audiotext");
        for (int k = 0; k < audioTextList.getLength(); k++) {
            Element audioTextElement = (Element) audioTextList.item(k);

            NodeList languages = audioTextElement.getElementsByTagName("language");
            NodeList formats = audioTextElement.getElementsByTagName("audioformat");
            int count = Math.min(languages.getLength(), formats.getLength());
            for (int j = 0; j < count; j++) {
                Element langElement = (Element) languages.item(j);
                String langType = langElement.getAttribute("type");
                String language = langElement.getTextContent().trim();
                String audioFormat = formats.item(j).getTextContent().trim();
                int audiotextId = getOrCreateAtext(conn, langType, language, audioFormat);
                PreparedStatement psItemAudio = conn.prepareStatement("INSERT INTO item_audiotext (asin, audiotext_id) VALUES (?, ?) ON CONFLICT DO NOTHING");
                psItemAudio.setString(1, asin);
                psItemAudio.setInt(2, audiotextId);
                psItemAudio.executeUpdate();
            }
        }
    }

    /**
     * Holt die ID eines Eintrags aus der Datenbank oder erstellt einen neuen Eintrag, falls dieser nicht existiert.
     * @param conn Die Datenbankverbindung
     * @param table Der Name der Tabelle
     * @param name Der Name des Eintrags
     * @return Die ID des Eintrags
     * @throws Exception Wenn ein Fehler auftritt
     */
    static int getOrCreate(Connection conn, String table, String name) throws Exception {
        PreparedStatement psSel = conn.prepareStatement(
            "SELECT " + table + "_id FROM " + table + " WHERE name = ?"
        );
        psSel.setString(1, name);
        ResultSet rs = psSel.executeQuery();
        if (rs.next()) return rs.getInt(1);

        PreparedStatement psIns = conn.prepareStatement(
            "INSERT INTO " + table + " (name) VALUES (?) RETURNING " + table + "_id"
        );
        psIns.setString(1, name);
        rs = psIns.executeQuery();
        rs.next();
        return rs.getInt(1);
    }

    /**
     * Holt die ID eines Audiotexts oder erstellt einen neuen Eintrag, falls dieser nicht existiert.
     * @param conn Die Datenbankverbindung
     * @param langType Der Typ der Sprache (z.B. "audio", "text")
     * @param language Die Sprache (z.B. "Deutsch", "Englisch")
     * @param audioFormat Das Audioformat (z.B. "MP3", "WAV")
     * @return Die ID des Audiotexts
     * @throws Exception Wenn ein Fehler auftritt
     */
    static int getOrCreateAtext(Connection conn, String langType, String language, String audioFormat) throws Exception {
        PreparedStatement psSel = conn.prepareStatement(
            "SELECT audiotext_id FROM audiotext WHERE lang_type = ? AND language = ? AND audioformat = ?"
        );
        psSel.setString(1, langType);
        psSel.setString(2, language);
        psSel.setString(3, audioFormat);
        ResultSet rs = psSel.executeQuery();
        if (rs.next()) return rs.getInt(1);

        PreparedStatement psIns = conn.prepareStatement(
            "INSERT INTO audiotext (lang_type, language, audioformat) VALUES (?, ?, ?) RETURNING audiotext_id"
        );
        psIns.setString(1, langType);
        psIns.setString(2, language);
        psIns.setString(3, audioFormat);
        rs = psIns.executeQuery();
        rs.next();
        return rs.getInt(1);
    }

    /**
     * Holt die ID eines Listmania-Eintrags oder erstellt einen neuen Eintrag, falls dieser nicht existiert.
     * @param conn Die Datenbankverbindung
     * @param name Der Name der Listmania
     * @return Die ID des Listmania-Eintrags
     * @throws Exception Wenn ein Fehler auftritt
     */
    static int getOrCreateListmania(Connection conn, String name) throws Exception {
        // Erst prüfen, ob der Name schon existiert
        PreparedStatement psSel = conn.prepareStatement(
            "SELECT list_id FROM listmania WHERE name = ?"
        );
        psSel.setString(1, name);
        ResultSet rs = psSel.executeQuery();
        if (rs.next()) return rs.getInt(1);

        // Falls nicht vorhanden: einfügen und ID zurückgeben
        PreparedStatement psIns = conn.prepareStatement(
            "INSERT INTO listmania (name) VALUES (?) RETURNING list_id"
        );
        psIns.setString(1, name);
        rs = psIns.executeQuery();
        rs.next();
        return rs.getInt(1);
    }

    static String getText(Element element, String tag) {
        NodeList nl = element.getElementsByTagName(tag);
        if (nl.getLength() > 0 && nl.item(0).getTextContent() != null) {
            return nl.item(0).getTextContent().trim();
        }
        return "";
    }

    public static List<Map<String, String>> getSimilarProducts(Element product) {
        List<Map<String, String>> result = new ArrayList<>();
        NodeList similarList = product.getElementsByTagName("sim_product");
        for (int i = 0; i < similarList.getLength(); i++) {
            Element sim = (Element) similarList.item(i);
            String asin = getTextContent(sim, "asin");
            String title = getTextContent(sim, "title");
            Map<String, String> map = new HashMap<>();
            map.put("asin", asin);
            map.put("title", title);
            result.add(map);
        }
        return result;
    }

    /**
     * Holt den Textinhalt eines bestimmten Tags innerhalb eines Produkt-Elements.
     * @param product Das Produkt-Element
     * @param tagName Der Name des Tags
     * @return Der Textinhalt des Tags oder ein leerer String, wenn der Tag nicht gefunden wurde
     */
    private static String getTextContent(Element product, String tagName) {
        NodeList list = product.getElementsByTagName(tagName);
        if (list.getLength() > 0) {
            return list.item(0).getTextContent().trim();
        }
        return "";
    }

    /**
     * Holt eine Liste von Attributwerten für ein bestimmtes Tag innerhalb eines Eltern-Elements.
     * @param parent Das Eltern-Element
     * @param tagName Der Name des Tags
     * @param attributeName Der Name des Attributs, dessen Werte gesammelt werden sollen
     * @return Eine Liste der Attributwerte
     */
    static List<String> getAttributeList(Element parent, String tagName, String attributeName) {
        List<String> result = new ArrayList<>();
        NodeList nodeList = parent.getElementsByTagName(tagName);
        for (int i = 0; i < nodeList.getLength(); i++) {
            Element element = (Element) nodeList.item(i);
            String attr = element.getAttribute(attributeName);
            if (!attr.isEmpty()) {
                result.add(attr);
            }
        }
        return result;
    }

    /**
     * Holt eine Liste von Namen für eine bestimmte Rolle (z.B. "author", "actor") innerhalb eines Container-Tags.
     * @param product Das Produkt-Element
     * @param containerTag Der Name des Container-Tags (z.B. "authors", "actors")
     * @param elementTag Der Name des Element-Tags, dessen Namen gesammelt werden sollen (z.B. "author", "actor")
     * @return Eine Liste der Namen
     */
    public static List<String> getNamesForRole(Element product, String containerTag, String elementTag) {
        List<String> result = new ArrayList<>();
        NodeList containerNodes = product.getElementsByTagName(containerTag);
        if (containerNodes.getLength() == 0) return result;

        Element containerElem = (Element) containerNodes.item(0);
        NodeList roleNodes = containerElem.getElementsByTagName(elementTag);
        for (int i = 0; i < roleNodes.getLength(); i++) {
            Element el = (Element) roleNodes.item(i);
            String name = el.getAttribute("name");
            if (name != null && !name.isEmpty()) {
                result.add(name);
            }
        }
        return result;
    }
     
    /**
    * Protokolliert einen illegalen Eintrag in der Datenbank.
    * @param product Das Produkt-Element, das den illegalen Eintrag repräsentiert
    * @param reason  Der Grund, warum der Eintrag als illegal betrachtet wird
    * @param conn    Die Datenbankverbindung
    */
    private static void logIllegal(Element product, String reason, Connection conn) {
        try {
            String asin = product.getAttribute("asin");
            String pgroup = product.getAttribute("pgroup");
            String title = getText(product, "title");

            PreparedStatement stmt = conn.prepareStatement("INSERT INTO illegal_data (asin, pgroup, title, error_message) VALUES (?, ?, ?, ?)");
            stmt.setString(1, asin.isEmpty() ? null : asin);
            stmt.setString(2, pgroup.isEmpty() ? null : pgroup);
            stmt.setString(3, title.isEmpty() ? null : title);
            stmt.setString(4, reason);
            stmt.executeUpdate();
            System.err.println("Illegaler Eintrag protokolliert: " + asin + " - " + reason);
        } catch (java.sql.SQLException e) {
            System.err.println("Fehler beim Eintrag in illegal_data: " + e.getMessage());
        }
    }
}
