package de.example.impl.entity;

import jakarta.persistence.*;
import java.util.List;

@Entity
@Table(name = "item")
public class Item {

    @Id
    @Column(name = "asin", length = 40, nullable = false)
    private String asin;

    @Column(name = "pgroup", nullable = false, length = 40)
    private String pgroup;  // "Book", "DVD", "Music"

    @Column(name = "title", nullable = false)
    private String title;

    @Column(name = "salesrank")
    private Integer salesrank;

    @Column(name = "picture")
    private String picture;

    @Column(name = "detailpage")
    private String detailpage;

    @Column(name = "ean", length = 60)
    private String ean;

    @Column(name = "rating")
    private Float rating;

    @Column(name = "rating_counter")
    private Integer ratingCounter;

    @ManyToOne
    @JoinColumn(name = "shop_id")
    private Shop shop;

    @OneToMany(mappedBy = "item", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<Angebot> angebote;

    @OneToMany(mappedBy = "item", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<ItemStatus> status;

    @ManyToMany
    @JoinTable(
    name = "item_track",
    joinColumns = @JoinColumn(name = "asin"),
    inverseJoinColumns = @JoinColumn(name = "track_id")
    )
    private List<Track> tracks;

    @ManyToMany
    @JoinTable(
    name = "item_kategorie",
    joinColumns = @JoinColumn(name = "asin"),
    inverseJoinColumns = @JoinColumn(name = "kategorie_id")
    )
    private List<Kategorie> kategorien;

    @OneToMany(mappedBy = "item", cascade = CascadeType.ALL)
    private List<Angebot> offers;

    public List<Kategorie> getKategorien() { return kategorien; }
    public void setKategorien(List<Kategorie> kategorien) { this.kategorien = kategorien; }

    public List<Angebot> getOffers() { return offers; }
    public void setOffers(List<Angebot> offers) { this.offers = offers; }





    // --- Konstruktoren ---
    public Item() {}

    public Item(String asin, String title, String pgroup) {
        this.asin = asin;
        this.title = title;
        this.pgroup = pgroup;
    }

    // --- Getter/Setter ---
    public String getAsin() {
        return asin;
    }

    public void setAsin(String asin) {
        this.asin = asin;
    }

    public String getPgroup() {
        return pgroup;
    }

    public void setPgroup(String pgroup) {
        this.pgroup = pgroup;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Integer getSalesrank() {
        return salesrank;
    }

    public void setSalesrank(Integer salesrank) {
        this.salesrank = salesrank;
    }

    public String getPicture() {
        return picture;
    }

    public void setPicture(String picture) {
        this.picture = picture;
    }

    public String getDetailpage() {
        return detailpage;
    }

    public void setDetailpage(String detailpage) {
        this.detailpage = detailpage;
    }

    public String getEan() {
        return ean;
    }

    public void setEan(String ean) {
        this.ean = ean;
    }

    public Float getRating() {
        return rating;
    }

    public void setRating(Float rating) {
        this.rating = rating;
    }

    public Integer getRatingCounter() {
        return ratingCounter;
    }

    public void setRatingCounter(Integer ratingCounter) {
        this.ratingCounter = ratingCounter;
    }

    // --- Beziehungen ---
    @OneToMany(mappedBy = "item", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private List<Rezension> rezensionen;

    public List<Rezension> getRezensionen() {
        return rezensionen;
    }

    public void setRezensionen(List<Rezension> rezensionen) {
        this.rezensionen = rezensionen;
    }

    @Override
public String toString() {
    return "Item{" +
            "asin='" + asin + '\'' +
            ", title='" + title + '\'' +
            ", pgroup='" + pgroup + '\'' +
            (rating != null ? ", rating=" + rating : "") +
            (ratingCounter != null ? ", ratingCounter=" + ratingCounter : "") +
            (shop != null ? ", shop=" + shop.getName() : "") +
            '}';
}

}
