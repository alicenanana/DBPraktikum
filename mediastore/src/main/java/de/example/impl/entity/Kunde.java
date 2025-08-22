package de.example.impl.entity;

import jakarta.persistence.*;

@Entity
@Table(name = "kunde")
public class Kunde {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "kunden_id")
    private Integer kundenId;

    @Column(name = "username", unique = true, nullable = false)
    private String username;

    @Column(name = "mail", unique = true)
    private String mail;

    @Column(name = "adresse")
    private String adresse;

    @Column(name = "konto_nr")
    private String kontoNr;

    // --- Konstruktoren ---
    public Kunde() {}

    public Kunde(String username, String mail, String adresse, String kontoNr) {
        this.username = username;
        this.mail = mail;
        this.adresse = adresse;
        this.kontoNr = kontoNr;
    }

    // --- Getter/Setter ---
    public Integer getKundenId() {
        return kundenId;
    }

    public void setKundenId(Integer kundenId) {
        this.kundenId = kundenId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getMail() {
        return mail;
    }

    public void setMail(String mail) {
        this.mail = mail;
    }

    public String getAdresse() {
        return adresse;
    }

    public void setAdresse(String adresse) {
        this.adresse = adresse;
    }

    public String getKontoNr() {
        return kontoNr;
    }

    public void setKontoNr(String kontoNr) {
        this.kontoNr = kontoNr;
    }

    @Override
    public String toString() {
        return "Kunde{" +
                "id=" + kundenId +
                ", username='" + username + '\'' +
                ", mail='" + mail + '\'' +
                ", adresse='" + adresse + '\'' +
                ", kontoNr='" + kontoNr + '\'' +
                '}';
    }
}
