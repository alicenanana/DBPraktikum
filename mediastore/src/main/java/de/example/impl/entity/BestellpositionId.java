package de.example.impl.entity;

import jakarta.persistence.Embeddable;
import java.io.Serializable;
import java.util.Objects;

@Embeddable
public class BestellpositionId implements Serializable {

    private Integer bestellungId;
    private String asin;

    public BestellpositionId() {}

    public BestellpositionId(Integer bestellungId, String asin) {
        this.bestellungId = bestellungId;
        this.asin = asin;
    }

    // --- Getter/Setter ---
    public Integer getBestellungId() {
        return bestellungId;
    }

    public void setBestellungId(Integer bestellungId) {
        this.bestellungId = bestellungId;
    }

    public String getAsin() {
        return asin;
    }

    public void setAsin(String asin) {
        this.asin = asin;
    }

    // equals & hashCode (für Composite Key!)
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BestellpositionId)) return false;
        BestellpositionId that = (BestellpositionId) o;
        return Objects.equals(bestellungId, that.bestellungId) &&
               Objects.equals(asin, that.asin);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bestellungId, asin);
    }
}
