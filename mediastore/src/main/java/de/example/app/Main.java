package de.example.app;

import de.example.api.MediaStoreService;
import de.example.impl.entity.Item;
import de.example.impl.entity.Kategorie;
import de.example.impl.entity.Kunde;
import de.example.impl.entity.Angebot;

import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        try (InputStream in = Main.class.getResourceAsStream("/app.properties")) {
            if (in == null) throw new IllegalStateException("app.properties not found");
            props.load(in);
        }

        String implClass = props.getProperty("orm.impl");
        MediaStoreService svc = (MediaStoreService) Class.forName(implClass).getDeclaredConstructor().newInstance();
        svc.init(props);

        try (Scanner sc = new Scanner(System.in)) {
            boolean running = true;
            while (running) {
                System.out.println("\n=== MediaStore Menü ===");
                System.out.println("1. Item suchen (getItem)");
                System.out.println("2. Items mit Pattern suchen (getItems)");
                System.out.println("3. Kategorienbaum anzeigen (getKategorieTree)");
                System.out.println("4. Items nach Kategoriepfad (getItemsByKategoriePath)");
                System.out.println("5. Top-Items (getTopItems)");
                System.out.println("6. Ähnliche günstigere Items (getSimilarCheaperItems)");
                System.out.println("7. Review hinzufügen (addNewReview)");
                System.out.println("8. Trolle finden (getTrolls)");
                System.out.println("9. Angebote für Item (getOffers)");
                System.out.println("0. Beenden");
                System.out.print("Auswahl: ");

                int choice = Integer.parseInt(sc.nextLine());

                switch (choice) {
                    case 1 -> {
                        System.out.print("ASIN eingeben: ");
                        String asin = sc.nextLine();
                        Item item = svc.getItem(asin);
                        System.out.println(item == null ? "(nicht gefunden)" : item);
                    }
                    case 2 -> {
                        System.out.print("Pattern eingeben (z.B. %ABBA%): ");
                        String pat = sc.nextLine();
                        List<Item> items = svc.getItems(pat.isBlank() ? null : pat);
                        items.stream().limit(20).forEach(System.out::println);
                        System.out.println("Gefunden: " + items.size());
                    }
                    case 3 -> {
                        Kategorie root = svc.getKategorieTree();
                        printKategorie(root, 0);
                    }
                    case 4 -> {
                        System.out.print("Kategoriepfad (z.B. Musik/Pop): ");
                        String path = sc.nextLine();
                        List<Item> items = svc.getItemsByKategoriePath(path);
                        items.forEach(System.out::println);
                        System.out.println("Gefunden: " + items.size());
                    }
                    case 5 -> {
                        System.out.print("Top wie viele? ");
                        int k = Integer.parseInt(sc.nextLine());
                        List<Item> items = svc.getTopItems(k);
                        items.forEach(System.out::println);
                    }
                    case 6 -> {
                        System.out.print("ASIN eingeben: ");
                        String asin = sc.nextLine();
                        List<Item> items = svc.getSimilarCheaperItems(asin);
                        items.forEach(System.out::println);
                    }
                    case 7 -> {
                        System.out.print("ASIN: ");
                        String asin = sc.nextLine();
                        System.out.print("Kunden-ID: ");
                        String kid = sc.nextLine();
                        System.out.print("Bewertung (1-5): ");
                        int rating = Integer.parseInt(sc.nextLine());
                        System.out.print("Text: ");
                        String text = sc.nextLine();
                        svc.addNewReview(asin, kid, rating, text);
                        System.out.println("Review hinzugefügt!");
                    }
                    case 8 -> {
                        System.out.print("Max. Durchschnittsrating: ");
                        double max = Double.parseDouble(sc.nextLine());
                        List<Kunde> trolls = svc.getTrolls(max);
                        trolls.forEach(System.out::println);
                    }
                    case 9 -> {
                        System.out.print("ASIN: ");
                        String asin = sc.nextLine();
                        List<Angebot> offers = svc.getOffers(asin);
                        offers.forEach(System.out::println);
                    }
                    case 0 -> running = false;
                }
            }
        } finally {
            svc.finish();
        }
    }

    private static void printKategorie(Kategorie k, int level) {
        if (k == null) return;
        System.out.println("  ".repeat(level) + "- " + k.getName());
        if (k.getChildren() != null) {
            for (Kategorie child : k.getChildren()) {
                printKategorie(child, level + 1);
            }
        }
    }
}
