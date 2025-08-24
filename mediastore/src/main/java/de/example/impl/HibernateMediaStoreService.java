package de.example.impl;
import de.example.api.MediaStoreService;
import de.example.impl.entity.Item;
import de.example.impl.entity.Kategorie;
import de.example.impl.entity.Rezension;
import de.example.impl.entity.Kunde;
import de.example.impl.entity.Angebot;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import jakarta.persistence.TypedQuery;

import org.hibernate.cfg.AvailableSettings;

import java.math.BigDecimal;  
import java.util.*;




import java.util.*;

public class HibernateMediaStoreService implements MediaStoreService {
  private EntityManagerFactory emf;

  @Override
  public void init(Properties props) {
    Map<String,Object> cfg = new HashMap<>();
    cfg.put(AvailableSettings.JAKARTA_JDBC_URL,      props.getProperty("jdbc.url"));
    cfg.put(AvailableSettings.JAKARTA_JDBC_USER,     props.getProperty("jdbc.user"));
    cfg.put(AvailableSettings.JAKARTA_JDBC_PASSWORD, props.getProperty("jdbc.password"));
    cfg.put(AvailableSettings.DIALECT,               props.getProperty("hibernate.dialect"));
    cfg.put(AvailableSettings.SHOW_SQL,              props.getProperty("hibernate.show_sql","false"));
    cfg.put(AvailableSettings.HBM2DDL_AUTO,          props.getProperty("hibernate.hbm2ddl.auto","validate"));
    emf = Persistence.createEntityManagerFactory("mediastore-unit", cfg);
  }

  @Override public void finish() { if (emf != null) emf.close(); }

  @Override
  public Item getItem(String asin) {
    EntityManager em = emf.createEntityManager();
    try { return em.find(Item.class, asin); }
    finally { em.close(); }
  }

 @Override
  public List<Item> getItems(String pattern) {
    EntityManager em = emf.createEntityManager();
    try {
        String jpql = "SELECT i FROM Item i";
        if (pattern != null && !pattern.isBlank()) {
            jpql += " WHERE LOWER(i.title) LIKE LOWER(:pattern)";
        }
        jpql += " ORDER BY i.title";

        TypedQuery<Item> query = em.createQuery(jpql, Item.class);

        if (pattern != null && !pattern.isBlank()) {
            query.setParameter("pattern", pattern);
        }

        return query.getResultList();
    } finally {
        em.close();
    }
    
}

@Override
public List<Kategorie> getKategorieTree() {
    EntityManager em = emf.createEntityManager();
    try {
        TypedQuery<Kategorie> query = em.createQuery(
            "SELECT k FROM Kategorie k WHERE k.parent IS NULL", 
            Kategorie.class
        );
        List<Kategorie> roots = query.getResultList();

        // Für jeden Root-Knoten rekursiv alle Children laden
        for (Kategorie root : roots) {
            initializeChildren(root);
        }

        return roots;
    } finally {
        em.close();
    }
}

private void initializeChildren(Kategorie kategorie) {
    if (kategorie.getChildren() != null) {
        // Nur einmal über die Liste iterieren, damit Hibernate sie lädt
        kategorie.getChildren().size();
        for (Kategorie child : kategorie.getChildren()) {
            initializeChildren(child);
        }
    }
}



@Override
public List<Item> getItemsByKategoriePath(String categoryPath) {
    EntityManager em = emf.createEntityManager();
    try {
        if (categoryPath == null) return java.util.Collections.emptyList();

        String[] parts = java.util.Arrays.stream(categoryPath.split("/"))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toArray(String[]::new);

        if (parts.length == 0) return java.util.Collections.emptyList();

        Kategorie current = null;

        for (String part : parts) {
            TypedQuery<Kategorie> q = (current == null)
                    ? em.createQuery(
                        "SELECT k FROM Kategorie k " +
                        "WHERE k.parent IS NULL AND k.name = :name",
                        Kategorie.class)
                    : em.createQuery(
                        "SELECT k FROM Kategorie k " +
                        "WHERE k.parent = :parent AND k.name = :name",
                        Kategorie.class)
                      .setParameter("parent", current);

            java.util.List<Kategorie> matches = q.setParameter("name", part)
                                                 .setMaxResults(1)
                                                 .getResultList();

            if (matches.isEmpty()) {
                return java.util.Collections.emptyList();
            }

            current = matches.get(0);
        }

        if (current == null) return java.util.Collections.emptyList();

        return em.createQuery(
            "SELECT i FROM Item i JOIN i.kategorien k WHERE k = :kategorie",
            Item.class
        )
        .setParameter("kategorie", current)
        .getResultList();

    } finally {
        em.close();
    }
}

@Override
public List<Item> getTopItems(int k) {
    EntityManager em = emf.createEntityManager();
    try {
        TypedQuery<Item> query = em.createQuery(
            "SELECT i FROM Item i ORDER BY i.rating DESC",
            Item.class
        );
        query.setMaxResults(k);
        return query.getResultList();
    } finally {
        em.close();
    }
}

@Override
public List<Item> getSimilarCheaperItems(String asin) {
    EntityManager em = emf.createEntityManager();
    try {
        Item original = em.find(Item.class, asin);
        if (original == null) return List.of();

        // Erste Kategorie nehmen
        Kategorie kategorie = original.getKategorien().isEmpty() ? null : original.getKategorien().get(0);
        if (kategorie == null) return List.of();

        // Preis vom Original (kleinster Angebotspreis)
        BigDecimal originalPreis = original.getOffers().stream()
            .map(Angebot::getPreis)
            .filter(Objects::nonNull)
            .min(BigDecimal::compareTo)
            .orElse(BigDecimal.valueOf(Double.MAX_VALUE));

        // Ähnliche Items aus derselben Kategorie
        TypedQuery<Item> query = em.createQuery(
            "SELECT DISTINCT i FROM Item i JOIN i.kategorien k " +
            "WHERE k = :kat AND i <> :orig", Item.class
        );
        query.setParameter("kat", kategorie);
        query.setParameter("orig", original);

        List<Item> result = query.getResultList();

        // Nur billigere zurückgeben
        return result.stream()
            .filter(it -> it.getOffers().stream()
                .anyMatch(o -> o.getPreis() != null &&
                               o.getPreis().compareTo(originalPreis) < 0)) // compareTo statt <
            .toList();

    } finally {
        em.close();
    }
}


@Override
public void addNewReview(String asin, String kundenId, int rating, String text) {
    EntityManager em = emf.createEntityManager();
    try {
        em.getTransaction().begin();

        Item item = em.find(Item.class, asin);
        Kunde kunde = em.find(Kunde.class, kundenId);

        Rezension rez = new Rezension();
        rez.setItem(item);
        rez.setKunde(kunde);
        rez.setTitel(text);
        rez.setBewertung(rating);
        rez.setText(text);
        rez.setRezensionsdatum(java.time.LocalDateTime.now());

        em.persist(rez);

        // Item-Bewertung aktualisieren
        int counter = (item.getRatingCounter() == null) ? 0 : item.getRatingCounter();
        float oldRating = (item.getRating() == null) ? 0f : item.getRating();

        counter++;
        float total = oldRating * (counter - 1) + rating;
        float newAverage = total / counter;

        item.setRatingCounter(counter);
        item.setRating(newAverage);

        em.merge(item);

        em.getTransaction().commit();
    } finally {
        if (em.getTransaction().isActive()) em.getTransaction().rollback();
        em.close();
    }
}

@Override
public List<Kunde> getTrolls(double maxAverageRating) {
    EntityManager em = emf.createEntityManager();
    try {
        TypedQuery<Kunde> query = em.createQuery(
            "SELECT r.kunde FROM Rezension r " +
            "GROUP BY r.kunde " +
            "HAVING AVG(r.bewertung) < :maxRating", // Spalte in Rezension ist 'bewertung'
            Kunde.class
        );
        query.setParameter("maxRating", maxAverageRating);
        return query.getResultList();
    } finally {
        em.close();
    }
}


@Override
public List<Angebot> getOffers(String asin) {
    EntityManager em = emf.createEntityManager();
    try {
        TypedQuery<Angebot> query = em.createQuery(
            "SELECT a FROM Angebot a WHERE a.item.asin = :asin",
            Angebot.class
        );
        query.setParameter("asin", asin);
        return query.getResultList();
    } finally {
        em.close();
    }
}

}