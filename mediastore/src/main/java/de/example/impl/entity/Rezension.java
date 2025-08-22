package de.example.impl.entity;

import jakarta.persistence.*;
import java.time.LocalDate;

@Entity
@Table(name = "rezension")
public class Rezension {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "rezension_id")
    private Integer rezensionId;

    @ManyToOne
    @JoinColumn(name = "asin", nullable = false)
    private Item item;

    @ManyToOne
    @JoinColumn(name = "kunden_id", nullable = false)
    private Kunde kunde;

    @Column(name = "bewertung")
    private Integer bewertung;

    @Column(name = "titel")
    private String titel;

    @Column(name = "text")
    private String text;

    @Column(name = "rezensionsdatum")
    private java.time.LocalDateTime rezensionsdatum;


    // --- Konstruktoren ---
    public Rezension() {}

    // --- Getter/Setter ---
    public Integer getRezensionId() {
        return rezensionId;
    }

    public void setRezensionId(Integer rezensionId) {
        this.rezensionId = rezensionId;
    }

    public Item getItem() {
        return item;
    }

    public void setItem(Item item) {
        this.item = item;
    }

    public Kunde getKunde() {
        return kunde;
    }

    public void setKunde(Kunde kunde) {
        this.kunde = kunde;
    }

    public Integer getBewertung() {
        return bewertung;
    }

    public void setBewertung(Integer bewertung) {
        this.bewertung = bewertung;
    }

    public String getTitel() {
        return titel;
    }

    public void setTitel(String titel) {
        this.titel = titel;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public java.time.LocalDateTime getRezensionsdatum() {
        return rezensionsdatum;
    }

    public void setRezensionsdatum(java.time.LocalDateTime rezensionsdatum) {
        this.rezensionsdatum = rezensionsdatum;
    }

    @Override
    public String toString() {
        return "Rezension{" +
                "id=" + rezensionId +
                ", bewertung=" + bewertung +
                ", titel='" + titel + '\'' +
                ", text='" + text + '\'' +
                ", rezensionsdatum=" + rezensionsdatum +
                '}';
    }
}
