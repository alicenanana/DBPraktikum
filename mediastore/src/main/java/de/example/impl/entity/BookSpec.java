package de.example.impl.entity;

import jakarta.persistence.*;
import java.time.LocalDate;

@Entity
@Table(name = "bookspec")
public class BookSpec {

    @Id
    @Column(name = "asin", length = 40)
    private String asin;

    @OneToOne
    @MapsId
    @JoinColumn(name = "asin")
    private Item item;

    private String binding;
    private String edition;

    @Column(nullable = false)
    private String isbn;

    private String weight;
    private String height;
    private String length;
    private Integer pages;

    @Column(name = "publication_date")
    private LocalDate publicationDate;

    // --- Getter/Setter ---
    public String getAsin() { return asin; }
    public void setAsin(String asin) { this.asin = asin; }

    public Item getItem() { return item; }
    public void setItem(Item item) { this.item = item; }

    public String getBinding() { return binding; }
    public void setBinding(String binding) { this.binding = binding; }

    public String getEdition() { return edition; }
    public void setEdition(String edition) { this.edition = edition; }

    public String getIsbn() { return isbn; }
    public void setIsbn(String isbn) { this.isbn = isbn; }

    public String getWeight() { return weight; }
    public void setWeight(String weight) { this.weight = weight; }

    public String getHeight() { return height; }
    public void setHeight(String height) { this.height = height; }

    public String getLength() { return length; }
    public void setLength(String length) { this.length = length; }

    public Integer getPages() { return pages; }
    public void setPages(Integer pages) { this.pages = pages; }

    public LocalDate getPublicationDate() { return publicationDate; }
    public void setPublicationDate(LocalDate publicationDate) { this.publicationDate = publicationDate; }
}
