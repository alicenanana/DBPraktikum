package de.example.impl.entity;

import jakarta.persistence.*;
import java.util.List;

@Entity
@Table(name = "track")
public class Track {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "track_id")
    private Integer trackId;

    private String name;

    @ManyToMany(mappedBy = "tracks")
    private List<Item> items;

    // --- Getter/Setter ---
    public Integer getTrackId() { return trackId; }
    public void setTrackId(Integer trackId) { this.trackId = trackId; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public List<Item> getItems() { return items; }
    public void setItems(List<Item> items) { this.items = items; }
}
