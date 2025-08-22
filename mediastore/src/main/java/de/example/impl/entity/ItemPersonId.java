package de.example.impl.entity;

import jakarta.persistence.Embeddable;
import java.io.Serializable;
import java.util.Objects;

@Embeddable
public class ItemPersonId implements Serializable {

    private String asin;
    private Integer personId;

    public ItemPersonId() {}

    public ItemPersonId(String asin, Integer personId) {
        this.asin = asin;
        this.personId = personId;
    }

    // --- Getter/Setter ---
    public String getAsin() { return asin; }
    public void setAsin(String asin) { this.asin = asin; }

    public Integer getPersonId() { return personId; }
    public void setPersonId(Integer personId) { this.personId = personId; }

    // equals & hashCode
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ItemPersonId)) return false;
        ItemPersonId that = (ItemPersonId) o;
        return Objects.equals(asin, that.asin) &&
               Objects.equals(personId, that.personId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(asin, personId);
    }
}
