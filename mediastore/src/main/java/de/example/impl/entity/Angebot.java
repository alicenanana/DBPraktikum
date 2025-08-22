package de.example.impl.entity;

import jakarta.persistence.*;
import java.math.BigDecimal;

@Entity
@Table(name = "angebot")
public class Angebot {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "angebot_id")
    private Integer angebotId;

    @ManyToOne
    @JoinColumn(name = "shop_id", nullable = false)
    private Shop shop;

    @ManyToOne
    @JoinColumn(name = "asin", nullable = false)
    private Item item;

    @Column(name = "preis", precision = 5, scale = 2)
    private BigDecimal preis;

    @Column(name = "verfuegbar")
    private Boolean verfuegbar;

    @Column(name = "zustand")
    private String zustand;

    // --- Konstruktoren ---
    public Angebot() {}

    public Angebot(Shop shop, Item item, BigDecimal preis, Boolean verfuegbar, String zustand) {
        this.shop = shop;
        this.item = item;
        this.preis = preis;
        this.verfuegbar = verfuegbar;
        this.zustand = zustand;
    }

    // --- Getter/Setter ---
    public Integer getAngebotId() {
        return angebotId;
    }

    public void setAngebotId(Integer angebotId) {
        this.angebotId = angebotId;
    }

    public Shop getShop() {
        return shop;
    }

    public void setShop(Shop shop) {
        this.shop = shop;
    }

    public Item getItem() {
        return item;
    }

    public void setItem(Item item) {
        this.item = item;
    }

    public BigDecimal getPreis() {
        return preis;
    }

    public void setPreis(BigDecimal preis) {
        this.preis = preis;
    }

    public Boolean getVerfuegbar() {
        return verfuegbar;
    }

    public void setVerfuegbar(Boolean verfuegbar) {
        this.verfuegbar = verfuegbar;
    }

    public String getZustand() {
        return zustand;
    }

    public void setZustand(String zustand) {
        this.zustand = zustand;
    }

    @Override
    public String toString() {
        return "Angebot{" +
                "id=" + angebotId +
                ", shop=" + (shop != null ? shop.getName() : null) +
                ", item=" + (item != null ? item.getTitle() : null) +
                ", preis=" + preis +
                ", verfuegbar=" + verfuegbar +
                ", zustand='" + zustand + '\'' +
                '}';
    }
}
