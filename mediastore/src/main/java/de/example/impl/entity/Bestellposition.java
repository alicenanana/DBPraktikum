package de.example.impl.entity;

import jakarta.persistence.*;

@Entity
@Table(name = "bestellposition")
public class Bestellposition {

    @EmbeddedId
    private BestellpositionId id;

    @ManyToOne
    @MapsId("bestellungId")
    @JoinColumn(name = "bestellung_id", nullable = false)
    private Bestellung bestellung;

    @ManyToOne
    @MapsId("asin")
    @JoinColumn(name = "asin", nullable = false)
    private Item item;

    // --- Konstruktoren ---
    public Bestellposition() {}

    public Bestellposition(Bestellung bestellung, Item item) {
        this.bestellung = bestellung;
        this.item = item;
        this.id = new BestellpositionId(bestellung.getBestellungId(), item.getAsin());
    }

    // --- Getter/Setter ---
    public BestellpositionId getId() {
        return id;
    }

    public void setId(BestellpositionId id) {
        this.id = id;
    }

    public Bestellung getBestellung() {
        return bestellung;
    }

    public void setBestellung(Bestellung bestellung) {
        this.bestellung = bestellung;
    }

    public Item getItem() {
        return item;
    }

    public void setItem(Item item) {
        this.item = item;
    }

    @Override
    public String toString() {
        return "Bestellposition{" +
                "bestellung=" + (bestellung != null ? bestellung.getBestellungId() : null) +
                ", item=" + (item != null ? item.getTitle() : null) +
                '}';
    }
}
