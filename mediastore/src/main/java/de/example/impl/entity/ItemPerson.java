package de.example.impl.entity;

import jakarta.persistence.*;

@Entity
@Table(name = "item_person")
public class ItemPerson {

    @EmbeddedId
    private ItemPersonId id;

    @ManyToOne
    @MapsId("asin")
    @JoinColumn(name = "asin")
    private Item item;

    @ManyToOne
    @MapsId("personId")
    @JoinColumn(name = "person_id")
    private Person person;

    @Column(name = "person_role", length = 40)
    private String personRole;

    // --- Konstruktoren ---
    public ItemPerson() {}

    public ItemPerson(Item item, Person person, String role) {
        this.item = item;
        this.person = person;
        this.personRole = role;
        this.id = new ItemPersonId(item.getAsin(), person.getPersonId());
    }

    // --- Getter/Setter ---
    public ItemPersonId getId() { return id; }
    public void setId(ItemPersonId id) { this.id = id; }

    public Item getItem() { return item; }
    public void setItem(Item item) { this.item = item; }

    public Person getPerson() { return person; }
    public void setPerson(Person person) { this.person = person; }

    public String getPersonRole() { return personRole; }
    public void setPersonRole(String personRole) { this.personRole = personRole; }

    @Override
    public String toString() {
        return "ItemPerson{" +
                "item=" + (item != null ? item.getTitle() : null) +
                ", person=" + (person != null ? person.getName() : null) +
                ", role='" + personRole + '\'' +
                '}';
    }
}
