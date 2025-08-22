package de.example.impl.entity;

import jakarta.persistence.*;
import java.util.List;

@Entity
@Table(name = "shop")
public class Shop {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "shop_id")
    private Integer shopId;

    @Column(name = "name", unique = true, length = 50)
    private String name;

    @Column(name = "street", length = 50)
    private String street;

    @Column(name = "zip", length = 20)
    private String zip;

    @OneToMany(mappedBy = "shop", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<Item> items;

    @OneToMany(mappedBy = "shop", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<Angebot> angebote;

    // --- Konstruktoren ---
    public Shop() {}

    public Shop(String name, String street, String zip) {
        this.name = name;
        this.street = street;
        this.zip = zip;
    }

    // --- Getter/Setter ---
    public Integer getShopId() {
        return shopId;
    }

    public void setShopId(Integer shopId) {
        this.shopId = shopId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public String getZip() {
        return zip;
    }

    public void setZip(String zip) {
        this.zip = zip;
    }

    public List<Item> getItems() {
        return items;
    }

    public void setItems(List<Item> items) {
        this.items = items;
    }

    public List<Angebot> getAngebote() {
        return angebote;
    }

    public void setAngebote(List<Angebot> angebote) {
        this.angebote = angebote;
    }

    @Override
    public String toString() {
        return "Shop{" +
                "id=" + shopId +
                ", name='" + name + '\'' +
                ", street='" + street + '\'' +
                ", zip='" + zip + '\'' +
                ", items=" + (items != null ? items.size() : 0) +
                ", angebote=" + (angebote != null ? angebote.size() : 0) +
                '}';
    }
}
