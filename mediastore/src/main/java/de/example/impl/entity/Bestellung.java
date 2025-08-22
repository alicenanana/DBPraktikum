package de.example.impl.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;
import java.util.List;

@Entity
@Table(name = "bestellung")
public class Bestellung {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "bestellung_id")
    private Integer bestellungId;

    @ManyToOne
    @JoinColumn(name = "kunden_id", nullable = false)
    private Kunde kunde;

    @Column(name = "kaufdatum")
    private LocalDateTime kaufdatum;

    @OneToMany(mappedBy = "bestellung", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<Bestellposition> positionen;

    // --- Konstruktoren ---
    public Bestellung() {}

    public Bestellung(Kunde kunde, LocalDateTime kaufdatum) {
        this.kunde = kunde;
        this.kaufdatum = kaufdatum;
    }

    // --- Getter/Setter ---
    public Integer getBestellungId() {
        return bestellungId;
    }

    public void setBestellungId(Integer bestellungId) {
        this.bestellungId = bestellungId;
    }

    public Kunde getKunde() {
        return kunde;
    }

    public void setKunde(Kunde kunde) {
        this.kunde = kunde;
    }

    public LocalDateTime getKaufdatum() {
        return kaufdatum;
    }

    public void setKaufdatum(LocalDateTime kaufdatum) {
        this.kaufdatum = kaufdatum;
    }

    public List<Bestellposition> getPositionen() {
        return positionen;
    }

    public void setPositionen(List<Bestellposition> positionen) {
        this.positionen = positionen;
    }

    @Override
    public String toString() {
        return "Bestellung{" +
                "id=" + bestellungId +
                ", kunde=" + (kunde != null ? kunde.getUsername() : null) +
                ", kaufdatum=" + kaufdatum +
                ", positionen=" + (positionen != null ? positionen.size() : 0) +
                '}';
    }
}
