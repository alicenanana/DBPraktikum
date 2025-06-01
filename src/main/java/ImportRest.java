import java.io.File;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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


        // Leipzig
        try (Connection conn = DriverManager.getConnection(url, user, password)) {

            File xmlFile = new File("data", "leipzig_transformed.xml");
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(xmlFile);
            doc.getDocumentElement().normalize();

            for (int i=0;i<3;i++) {System.out.println("");}
            System.out.println("Starte Import für Leipzig...");
            for (int i=0;i<3;i++) {System.out.println("");}

            NodeList productList = doc.getElementsByTagName("item");
            for (int i = 0; i < productList.getLength(); i++) {
                
                // Aktuell Betrachtetes Produkt
                Element product = (Element) productList.item(i);
                
                //asin 
                String asin = product.getAttribute("asin");
                if (asin.length() > 20 || asin.isEmpty()) {
                    System.err.println("Ungültige ASIN übersprungen: '" + asin + "'");
                    continue;
                }

                //PGROUP
                String pgroup = product.getAttribute("pgroup").trim();
                System.out.println("Verarbeite nun PGROUP: " + pgroup + " fuer ASIN: " + asin);

                //Salesrank
                int salesrank;
                try {
                    salesrank = Integer.parseInt(product.getAttribute("salesrank"));
                } catch (NumberFormatException e) {
                    salesrank = 0; 
                }
                

                // Titel
                String title = getText(product, "title");
                if (title.isEmpty()) {
                    title = "Unbekannter Titel"; 
                }
                
                
                //Preis
                double price = 0.0;
                String pricestate = null, pricecurrency = null;
                NodeList priceNodes = product.getElementsByTagName("price");
                if (priceNodes.getLength() > 0) {
                    Element priceElement = (Element) priceNodes.item(0);
                    try {
                        double mult = priceElement.hasAttribute("mult") ? Double.parseDouble(priceElement.getAttribute("mult")) : 0.01;
                        int basePrice = Integer.parseInt(priceElement.getTextContent().trim());
                        price = basePrice * mult;
                        } catch (NumberFormatException ignored) {}
                    pricestate = priceElement.getAttribute("state");
                    pricecurrency = priceElement.getAttribute("currency");
                }


                // Bild
                String picture = product.getAttribute("picture"); 
                if (picture.isEmpty()) {
                    picture = "No Picture";
                }
                
                //detailpage
                String detailPage = product.getAttribute("detailpage");
                if (detailPage.isEmpty()) {
                    detailPage = "No Detail Page";
                }

                //ean
                String ean = product.getAttribute("ean");     

               
                // item einfügen
                PreparedStatement psItem = conn.prepareStatement("INSERT INTO item (shop_id, asin, pgroup, title, salesrank, price, picture, detailpage , ean, item_status, currency) " +
                "VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (asin) DO NOTHING");
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
                psItem.executeUpdate();

                switch (pgroup) {
                    case "Book":
                        insertBook(conn, product, asin);
                        insertRest(conn, product, asin);
                        break;
                    case "DVD":
                        insertDVD(conn, product, asin);
                        insertRest(conn, product, asin);
                        break;
                    case "MUSIC":
                        insertMusic(conn, product, asin);
                        insertRest(conn, product, asin);
                        break;
                }
            }
            
            for (int i=0;i<3;i++) {System.out.println("");}
            System.out.println("Import abgeschlossen. Leipzig mit " + productList.getLength() + " Produkten.");
            for (int i=0;i<3;i++) {System.out.println("");}
        } catch (Exception e) {
            System.err.println("An error occurred: " + e.getMessage());
        }

        // Dresden
        try (Connection conn = DriverManager.getConnection(url, user, password)) {

            File xmlFile = new File("data", "dresden.xml");
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(xmlFile);
            doc.getDocumentElement().normalize();

            // Filtere nur die Haupt-<item>-Elemente (direkte Kinder des Wurzelelements)
            NodeList allItems = doc.getElementsByTagName("item");
            List<Element> productList = new ArrayList<>();
            for (int i = 0; i < allItems.getLength(); i++) {
                Element item = (Element) allItems.item(i);
                // Prüfe, ob das Parent-Element das Wurzelelement ist (also KEIN <similars>-Kind)
                if (item.getParentNode().equals(doc.getDocumentElement())) {
                    productList.add(item);
                }
            }
            List<Element> illegalProductList = new ArrayList<>();

            for (int i = 0; i < productList.size(); i++) {
            Element product = productList.get(i);
            String asin = product.getAttribute("asin");
            if (asin.isEmpty() || asin.length() > 20) {
            illegalProductList.add(product);
            continue; 
            }

            //  PGROUP ermitteln
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

                if (hasBookSpec || hasAuthors) pgroup = "Book";
                else if (hasDvdSpec || hasActors) pgroup = "DVD";
                else if (hasMusicSpec || hasTracks) pgroup = "Music";
                else pgroup = "Unknown";
            }
            System.out.println("Verarbeite nun PGROUP: " + pgroup + " fuer ASIN: " + asin);

            String title = getText(product, "title");
            if (title.isEmpty()) title = "Unbekannter Titel";

            int salesrank = 0;
            try {
                salesrank = Integer.parseInt(product.getAttribute("salesrank"));
            } catch (NumberFormatException ignored) {}

            double price = 0.0;
            String pricestate = null, pricecurrency = null;
            NodeList priceNodes = product.getElementsByTagName("price");
            if (priceNodes.getLength() > 0) {
                Element priceElement = (Element) priceNodes.item(0);
                try {
                double mult = priceElement.hasAttribute("mult") ? Double.parseDouble(priceElement.getAttribute("mult")) : 0.01;
                int basePrice = Integer.parseInt(priceElement.getTextContent().trim());
                price = basePrice * mult;
                } catch (NumberFormatException ignored) {}
                pricestate = priceElement.getAttribute("state");
                pricecurrency = priceElement.getAttribute("currency");
            }

            String picture = product.getAttribute("picture");
            if (picture.isEmpty()) picture = "No Picture";

            String detailPage = product.getAttribute("detailpage");
            if (detailPage.isEmpty()) detailPage = "No Detail Page";

            String ean = product.getAttribute("ean");

            // --- In Datenbank einfügen ---
            PreparedStatement psItem = conn.prepareStatement(
                "INSERT INTO item (shop_id, asin, pgroup, title, salesrank, price, picture, detailpage," +
                "ean, item_status, currency) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (asin) DO NOTHING"
            );
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
            psItem.executeUpdate();

            switch (pgroup) {
                case "Book":
                insertBook(conn, product, asin);
                System.out.println("Buch eingefügt: " + asin);
                insertRest(conn, product, asin);
                break;
                case "DVD":
                insertDVD(conn, product, asin);
                System.out.println("DVD eingefügt: " + asin);
                insertRest(conn, product, asin);
                break;
                case "Music":
                insertMusic(conn, product, asin);
                System.out.println("Musik eingefügt: " + asin);
                insertRest(conn, product, asin);
                break;
            }
            }
            // Entferne ungültige Produkte
            for (Element illegalProduct : illegalProductList) {
                String asin = illegalProduct.getAttribute("asin");
                System.err.println("Ungültige ASIN übersprungen: '" + asin + "'");
                productList.remove(illegalProduct);
            }

            for (int i = 0; i < 3; i++) {System.out.println("");}
            System.out.println("Import abgeschlossen. Dresden mit " + productList.size() + " Produkten.");
            for (int i = 0; i < 3; i++) {System.out.println("");}
        } catch (Exception e) {
            System.err.println("An error occurred: " + e.getMessage());
        }
    }

    /**
     * Fügt ein Buch in die Datenbank ein.
     * 
     * @param conn   Die Datenbankverbindung
     * @param product Das XML-Element, das die Buchinformationen enthält
     * @param asin   Die ASIN des Buches
     * @throws Exception Wenn ein Fehler beim Einfügen auftritt
     */
    static void insertBook(Connection conn, Element product, String asin) throws Exception {
        
        

        //bookspecs
        Element bookspec = (Element) product.getElementsByTagName("bookspec").item(0);
        if (bookspec == null) {
            System.err.println("Kein <bookspec> gefunden für ASIN: " + asin);
            return;
        }

        // ISBN
        Element isbnele = (Element) bookspec.getElementsByTagName("isbn").item(0);
        String isbn = isbnele.getAttribute("val");
        if (isbn.isEmpty()) {
            return; 
        }

        // Binding
        String binding = getText(product, "binding");
        if (binding.isEmpty()) {
            binding = "Unknown"; 
        }

        //edition
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

        //maße
        Element packageele = (Element) bookspec.getElementsByTagName("package").item(0);
        String weight = packageele.getAttribute("weight");
        String height = packageele.getAttribute("height");
        String length = packageele.getAttribute("length");
        if (weight.isEmpty() || height.isEmpty() || length.isEmpty()) {
            weight = "/";    
            height = "/";
            length = "/";
        }


        // Veröffentlichungsdatum
        List<String> pubDateList = getAttributeList(bookspec, "publication", "date");
        LocalDate pubDate = null;
        for (String pubdateStr :pubDateList  ){
            if (!pubdateStr.isEmpty()) {
                try {
                    pubDate = LocalDate.parse(pubdateStr);
                    break; 
                } catch (Exception e) {
                    System.err.println("Ungültiges Veröffentlichungsdatum für ASIN " + asin + ": " + pubdateStr);
                }
            } 
        }
        




        PreparedStatement ps = conn.prepareStatement("INSERT INTO bookspec (asin, binding, edition, isbn, pages, publication_date, height, weight, length)VALUES" + 
        "(?, ?, ?, ?, ?, ?, ?, ?, ?) On CONFLICT (asin) DO NOTHING");
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
     * Fügt eine DVD in die Datenbank ein.
     * 
     * @param conn   Die Datenbankverbindung
     * @param product Das XML-Element, das die DVD-Informationen enthält
     * @param asin   Die ASIN der DVD
     * @throws Exception Wenn ein Fehler beim Einfügen auftritt
     */
    static void insertDVD(Connection conn, Element product, String asin) throws Exception {

        //dvdspecs
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
        if (aspectratio.isEmpty()){
        aspectratio = "Unknown"; 
        }
        // Releasedate
        String relDateStr = getText(dvdspecs, "releasedate");
        LocalDate relDate;
        try {
            relDate = LocalDate.parse(relDateStr);
        } catch (Exception e) {
             relDate = LocalDate.now();
        }

        // Theatr. Release
        String theatrReleaseStr = getText(dvdspecs, "theatr_release");
        int theatr_elease;
        try {
            theatr_elease = Integer.parseInt(theatrReleaseStr);
            } catch (NumberFormatException e) {
                theatr_elease = 0;
                }

        PreparedStatement ps = conn.prepareStatement("INSERT INTO dvdspec (asin, format, regioncode, runningtime, releasedate, aspectratio, upc, theatr_release)" +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (asin) DO NOTHING");
        ps.setString(1, asin);
        ps.setString(2, format);
        ps.setInt(3, region);
        ps.setInt(4, runtime);
        ps.setDate(5, Date.valueOf(relDate));
        ps.setString(6, aspectratio);
        ps.setString(7, upc);
        ps.setInt(8, theatr_elease);
        ps.executeUpdate();
        }

    /**
     * Fügt Musik in die Datenbank ein.
     * 
     * @param conn   Die Datenbankverbindung
     * @param product Das XML-Element, das die Musikinformationen enthält
     * @param asin   Die ASIN der Musik
     * @throws Exception Wenn ein Fehler beim Einfügen auftritt
     */
    static void insertMusic(Connection conn, Element product, String asin) throws Exception {
        

        //musicspecs
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
        Element formatele= (Element) musicspecs.getElementsByTagName("format").item(0);
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
        LocalDate pubDate;
        if (pubDateStr == null || pubDateStr.isEmpty()) {
            pubDate = LocalDate.now(); // oder null, je nach Wunsch
        } else {
            pubDate = LocalDate.parse(pubDateStr);
        }
        
        

        // UPC (Universal Product Code)
        String upc = getText(musicspecs, "upc");
        if (upc.isEmpty()) {
            upc = "Unknown"; 
        }
        

        PreparedStatement ps = conn.prepareStatement("INSERT INTO musicspec (asin, binding, format, releasedate, upc, num_discs) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (asin) DO NOTHING");
        ps.setString(1, asin);
        ps.setString(2, binding);
        ps.setString(3, format);
        ps.setInt(6, numDiscs);
        ps.setDate(4, Date.valueOf(pubDate));
        ps.setString(5, upc);
        ps.executeUpdate();

      
       
    }

    /**
     * Fügt zusätzliche Informationen zu einem Produkt in die Datenbank ein.
     * 
     * @param conn   Die Datenbankverbindung
     * @param product Das XML-Element, das die Produktinformationen enthält
     * @param asin   Die ASIN des Produkts
     * @throws Exception Wenn ein Fehler beim Einfügen auftritt
     */
    static void insertRest(Connection conn, Element product, String asin) throws Exception {

        //label
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

        //track
        NodeList trackTitles = ((Element) product.getElementsByTagName("tracks").item(0)).getElementsByTagName("title");
        for (int t = 0; t < trackTitles.getLength(); t++) {
            String trackTitle = trackTitles.item(t).getTextContent().trim();
            if (!trackTitle.isEmpty()) {
                int track_id = getOrCreate(conn, "track", trackTitle);
                PreparedStatement pstrack = conn.prepareStatement("INSERT INTO item_track (asin, track_id, track_no) VALUES (?, ? ,?) ON CONFLICT DO NOTHING");
                pstrack.setString(1, asin);
                pstrack.setInt(2, track_id);
                pstrack.setInt(3, t+1);
                pstrack.executeUpdate();
            }
        }

        //listmania (Kategorien????????)
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



        //Ähnliche Produkte
        List<Map<String, String>> similarProducts = getSimilarProducts(product);
        for (Map<String, String> simProduct : similarProducts) {
            String simAsin = simProduct.get("asin");
            String simTitle = simProduct.get("title");
            if (simAsin != null && !simAsin.isEmpty()) {
                PreparedStatement pssim = conn.prepareStatement("INSERT INTO similar_product (asin, sim_asin, sim_title) VALUES (?, ?, ?) ON CONFLICT DO NOTHING");
                pssim.setString(1, asin);      
                pssim.setString(2, simAsin);   
                pssim.setString(3, simTitle);  
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


        //publisher (Verlag)
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

        //Audiotext (Mehrsprachige Angaben)
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
                PreparedStatement psAudio = conn.prepareStatement("INSERT INTO audiotext (lang_type, language, audioformat) VALUES (?, ?, ?)");
                int audiotextId = getOrCreateAtext(conn, langType, language, audioFormat);
                PreparedStatement psItemAudio = conn.prepareStatement("INSERT INTO item_audiotext (asin, audiotext_id) VALUES (?, ?) ON CONFLICT DO NOTHING");
                psAudio.setString(1, langType);
                psAudio.setString(2, language);
                psAudio.setString(3, audioFormat);
                psAudio.executeUpdate();
                psItemAudio.setString(1, asin);
                psItemAudio.setInt(2, audiotextId);
                psItemAudio.executeUpdate();
                }
            }
        } 
    
    /**
    * Bekommt die ID eines Eintrags in der angegebenen Tabelle mit dem gegebenen Namen.
    * Wenn der Eintrag nicht existiert, wird er erstellt und die neue ID zurückgegeben.
    * 
    * @param conn   Die Datenbankverbindung
    * @param table  Der Name der Tabelle, in der nach dem Eintrag gesucht wird
    * @param name   Der Name des Eintrags, dessen ID abgerufen oder erstellt werden soll
    * @return       Die ID des Eintrags in der Tabelle
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
     * Bekommt die ID eines Audiotext-Eintrags mit dem angegebenen Sprachtyp, Sprache und Audioformat.
     * 
     * @param conn        Die Datenbankverbindung
     * @param langType    Der Sprachtyp (z.B. "original", "translated")
     * @param language    Die Sprache (z.B. "de", "en")
     * @param audioFormat Das Audioformat (z.B. "mp3", "wav")
     * @return            Die ID des Audiotext-Eintrags, entweder existierend oder neu erstellt
     * @throws Exception Wenn ein Fehler beim Zugriff auf die Datenbank auftritt
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
     * Bekommt die ID einer Listmania-Liste mit dem angegebenen Namen.
     * 
     * @param conn   Die Datenbankverbindung
     * @param name   Der Name der Listmania-Liste
     * @return       Die ID der Listmania-Liste, entweder existierend oder neu erstellt
     * @throws Exception Wenn ein Fehler beim Zugriff auf die Datenbank auftritt
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

    /**
     * bekommt den Textinhalt des ersten Elements mit dem angegebenen Tag-Namen innerhalb des übergebenen Elements.
     * 
     * @param element Das Element, in dem nach dem Tag gesucht wird
     * @param tag    Der Name des Tags, dessen Textinhalt extrahiert werden soll
     * @return Der Textinhalt des Tags, oder ein leerer String, wenn der Tag nicht gefunden wurde
     */
    static String getText(Element element, String tag) {
        NodeList nl = element.getElementsByTagName(tag);
        if (nl.getLength() > 0 && nl.item(0).getTextContent() != null) {
            return nl.item(0).getTextContent().trim();
        }
        return "";
    }

    /**
     * bekommt eine Liste von ähnlichen Produkten aus dem angegebenen Produkt-Element.
     * 
     * @param product Das Produkt-Element, aus dem die ähnlichen Produkte extrahiert werden sollen
     * @return Eine Liste von Maps, die die ASIN und den Titel der ähnlichen Produkte enthalten
     */
    public static List<Map<String, String>> getSimilarProducts(Element product) {
        List<Map<String, String>> result = new ArrayList<>();
        NodeList similarList = product.getElementsByTagName("sim_product");
        for (int i = 0; i < similarList.getLength(); i++) {
            Element sim = (Element) similarList.item(i);
            String asin = getTextContent(sim, "asin");
            String title = getTextContent(sim, "title");
            Map<String, String> map = new HashMap<>();
            map.put("asin", asin);
            map.put("title", title); // optional, je nachdem ob du den Titel brauchst
            result.add(map);
        }

    return result;
    }
   
    /**
     * bekommt den Textinhalt des ersten Elements mit dem angegebenen Tag-Namen innerhalb des übergebenen Produkt-Elements.
     * 
     * @param product Das Produkt-Element, in dem nach dem Tag gesucht wird
     * @param tagName Der Name des Tags, dessen Textinhalt extrahiert werden soll
     * @return Der Textinhalt des Tags, oder ein leerer String, wenn der Tag nicht gefunden wurde
     */
    private static String getTextContent(Element product, String tagName) {
        NodeList list = product.getElementsByTagName(tagName);
        if (list.getLength() > 0) {
            return list.item(0).getTextContent().trim();
        }
    return "";
    }

    /**
     * bekommt eine Liste von Attributwerten für ein bestimmtes Tag innerhalb des übergebenen Elements.
     * 
     * @param parent Das Element, in dem nach den Tags gesucht wird
     * @param tagName Der Name des Tags, dessen Attribute extrahiert werden sollen
     * @param attributeName Der Name des Attributs, dessen Werte extrahiert werden sollen
     * @return Eine Liste von Attributwerten
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
     * bekommt eine Liste von Namen für eine bestimmte Rolle (z.B. "author", "actor") innerhalb des übergebenen Produkt-Elements.
     * 
     * @param product Das Produkt-Element, in dem nach den Rollen gesucht wird
     * @param containerTag Der Name des Containers, der die Rollen enthält (z.B. "authors", "actors")
     * @param elementTag Der Name des Elements, das die Rolle repräsentiert (z.B. "author", "actor")
     * @return Eine Liste von Namen für die angegebene Rolle
     */
    public static List<String> getNamesForRole(Element product, String containerTag, String elementTag) {
List<String> result = new ArrayList<>();
NodeList containerNodes = product.getElementsByTagName(containerTag);
if (containerNodes.getLength() == 0) return result;  // Keine solche Rolle vorhanden

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

}
