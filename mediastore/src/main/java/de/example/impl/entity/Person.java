package de.example.impl.entity;

import jakarta.persistence.*;
import java.util.List;

@Entity
@Table(name = "person")
public class Person {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "person_id")
    private Integer personId;

    private String name;

    @OneToMany(mappedBy = "person")
    private List<ItemPerson> itemPersons;

    // --- Getter/Setter ---
    public Integer getPersonId() { return personId; }
    public void setPersonId(Integer personId) { this.personId = personId; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public List<ItemPerson> getItemPersons() { return itemPersons; }
    public void setItemPersons(List<ItemPerson> itemPersons) { this.itemPersons = itemPersons; }
}
