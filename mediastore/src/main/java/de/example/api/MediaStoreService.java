package de.example.api;

import java.util.List;
import java.util.Properties;

import de.example.impl.entity.Item;
import de.example.impl.entity.Kategorie;
import de.example.impl.entity.Rezension;
import de.example.impl.entity.Kunde;
import de.example.impl.entity.Angebot;

public interface MediaStoreService {
    void init(Properties props) throws Exception;
    void finish();

    // Item-Methoden
    Item getItem(String asin);
    List<Item> getItems(String pattern);

    // Kategorien
    List<Kategorie> getKategorieTree();
    List<Item> getItemsByKategoriePath(String categoryPath);

    // Top / ähnliche Produkte
    List<Item> getTopItems(int k);
    List<Item> getSimilarCheaperItems(String asin);

    // Reviews
    void addNewReview(String asin, String kundenId, int rating, String text);

    // Trolle
    List<Kunde> getTrolls(double maxAverageRating);

    // Angebote
    List<Angebot> getOffers(String asin);
}
