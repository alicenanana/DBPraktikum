package de.example.impl.entity;

import jakarta.persistence.*;
import java.util.List;

@Entity
@Table(name = "kategorie")
public class Kategorie {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "kategorie_id")
    private Integer kategorieId;

    private String name;

    @ManyToOne
    @JoinColumn(name = "eltern_id")
    private Kategorie parent;

    @OneToMany(mappedBy = "parent", cascade = CascadeType.ALL)
    private List<Kategorie> children;

    @ManyToMany(mappedBy = "kategorien")
    private List<Item> items;

    // --- Getter/Setter ---
    public Integer getKategorieId() { return kategorieId; }
    public void setKategorieId(Integer kategorieId) { this.kategorieId = kategorieId; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public Kategorie getParent() { return parent; }
    public void setParent(Kategorie parent) { this.parent = parent; }

    public List<Kategorie> getChildren() { return children; }
    public void setChildren(List<Kategorie> children) { this.children = children; }

    public List<Item> getItems() { return items; }
    public void setItems(List<Item> items) { this.items = items; }
}
