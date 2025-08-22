package de.example.impl.entity;

import jakarta.persistence.*;
import java.time.LocalDate;

@Entity
@Table(name = "dvdspec")
public class DvdSpec {

    @Id
    @Column(name = "asin", length = 40)
    private String asin;

    @OneToOne
    @MapsId
    @JoinColumn(name = "asin")
    private Item item;

    private String aspectratio;
    private String format;
    private Integer regioncode;

    @Column(name = "releasedate")
    private LocalDate releaseDate;

    private Integer runningtime;

    @Column(name = "theatr_release")
    private Integer theatricalRelease;

    private String upc;

    // --- Getter/Setter ---
    public String getAsin() { return asin; }
    public void setAsin(String asin) { this.asin = asin; }

    public Item getItem() { return item; }
    public void setItem(Item item) { this.item = item; }

    public String getAspectratio() { return aspectratio; }
    public void setAspectratio(String aspectratio) { this.aspectratio = aspectratio; }

    public String getFormat() { return format; }
    public void setFormat(String format) { this.format = format; }

    public Integer getRegioncode() { return regioncode; }
    public void setRegioncode(Integer regioncode) { this.regioncode = regioncode; }

    public LocalDate getReleaseDate() { return releaseDate; }
    public void setReleaseDate(LocalDate releaseDate) { this.releaseDate = releaseDate; }

    public Integer getRunningtime() { return runningtime; }
    public void setRunningtime(Integer runningtime) { this.runningtime = runningtime; }

    public Integer getTheatricalRelease() { return theatricalRelease; }
    public void setTheatricalRelease(Integer theatricalRelease) { this.theatricalRelease = theatricalRelease; }

    public String getUpc() { return upc; }
    public void setUpc(String upc) { this.upc = upc; }
}
