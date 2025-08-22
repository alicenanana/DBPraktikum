package de.example.impl.entity;

import jakarta.persistence.*;
import java.time.LocalDate;

@Entity
@Table(name = "musicspec")
public class MusicSpec {

    @Id
    @Column(name = "asin", length = 40)
    private String asin;

    @OneToOne
    @MapsId
    @JoinColumn(name = "asin")
    private Item item;

    private String binding;
    private String format;

    @Column(name = "num_discs")
    private Integer numDiscs;

    @Column(name = "releasedate")
    private LocalDate releaseDate;

    private String upc;

    // --- Getter/Setter ---
    public String getAsin() { return asin; }
    public void setAsin(String asin) { this.asin = asin; }

    public Item getItem() { return item; }
    public void setItem(Item item) { this.item = item; }

    public String getBinding() { return binding; }
    public void setBinding(String binding) { this.binding = binding; }

    public String getFormat() { return format; }
    public void setFormat(String format) { this.format = format; }

    public Integer getNumDiscs() { return numDiscs; }
    public void setNumDiscs(Integer numDiscs) { this.numDiscs = numDiscs; }

    public LocalDate getReleaseDate() { return releaseDate; }
    public void setReleaseDate(LocalDate releaseDate) { this.releaseDate = releaseDate; }

    public String getUpc() { return upc; }
    public void setUpc(String upc) { this.upc = upc; }
}
