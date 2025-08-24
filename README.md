# Datenbankpraktikum

**Ausführen in dieser Reihenfolge:**
1. ImportRest
2. triggers_ratings.sql
3. ImportRatings
4. ImportCategories

(dafür immer in ``\alt\pom.xml`` diese Zeile anpassen: 
`` <mainClass>ImportCategories</mainClass>``)

Danach steht die Datenbank und mvn clean compile und mvn exec:java kann ausgeführt werden


# Anforderung

Die Applikation soll folgende Methoden umfassen. Definieren Sie hierfür vorab ein Interface welches alle Methoden umfasst. Für die Bestimmung der Ergebnisse soll kein SQL verwendet werden, sondern lediglich Hibernate inklusive der Hibernate Query Language.

Methoden der Schnittstelle


 ``init``

 Hier sollte die Datenbankverbindung für die anderen Methodenaufrufe erstellt, sowie weitere Aktionen, die zur Initialisierung notwendig sind, ausgeführt werden. Alle notwendigen Parameter sollen aus dem übergebenen Property-Objekt entnommen werden.

``finish``

Damit die Mittelschicht alle Ressourcen kontrolliert wieder freigeben kann, wird diese Methode bei Beendigung der Anwendung aufgerufen. Hier sollten speziell die Datenbankobjekte wieder freigegeben werden.

``getProduct``

Für eine bestimmte Produkt-Id werden mit dieser Methode die Detailinformationen des Produkts ermittelt.

``getProducts(String pattern)``

Diese Methode soll eine Liste der in der Datenbank enthaltenen Produkte, deren Titel mit dem übergebenen Pattern übereinstimmen, zurückliefern. Beachten Sie, dass im Falle von pattern=null die komplette Liste zurückgeliefert wird. Das Pattern kann SQL-Wildcards enthalten.

Hinweis: der Patternvergleich kann mittels des SQL-Operators like durchgeführt werden.

``getCategoryTree``

Diese Methode ermittelt den kompletten Kategorienbaum durch Rückgabe des Wurzelknotens. Jeder Knoten ist dabei vom Typ Category und kann eine Liste von Unterknoten (d.h. Unterkategorien) enthalten.

``getProductsByCategoryPath``

Nach Angabe einer Kategorie (definiert durch den Pfad von der Wurzel zu sich selbst) soll die Liste der zugeordneten Produkte ermittelt werden. Die Angabe des Pfades ist notwendig, da der Kategorienname allein nicht eindeutig ist.

``getTopProducts``

Diese Methode liefert eine Liste aller Produkte zurück, die unter den Top k sind basierend auf dem Rating.

``getSimilarCheaperProduct``

Diese Methode liefert für ein Produkt(Id) eine List von Produkten, die ähnlich und billiger sind als das spezifizierte.

``addNewReview``

Die Rahmenapplikation erlaubt sowohl das Ansehen als auch Hinzufügen von Reviews. MIt Hilfe der Methode wird ein neues Review in der Datenbank gespeichert.

``getTrolls``

Die Methode soll eine Liste von Nutzern ausgeben, deren Durchschnittsbewertung unter einem spezifizierten Rating ist.

``getOffers``

Für das übergegebene Produkt(Id) werden alle verfügbaren Angebote zurückgeliefert.

 
 # Testszenarien

 ## 1. getItem(String asin)

    Eingabe: B0000668PG (In a Pig's Eye: Reflections on the Police State Re).

    *Erwartung: Ausgabe des Items (Titel, pgroup, Rating, evtl. Shop). Falls die ASIN nicht existiert → (nicht gefunden).*

## 2. getItems(String pattern)

    Eingabe: %ABBA%

    *Erwartung: Liste aller Items mit „ABBA“ im Titel.*

    *Besonderheit: Wenn du nur Enter drückst (also pattern leer) → alle Items werden angezeigt.*

## 3. getKategorieTree()

    Eingabe: keine.

    *Erwartung: Hierarchische Ausgabe:*

    -  Musik
        - Pop
        - Rock
    - Bücher
        - Romane

    ... je nachdem, was du in deiner DB hast.

## 4. getItemsByKategoriePath(String path)

    Eingabe: Musik/Pop

    *Erwartung: Liste aller Items in der Kategorie „Pop“ unter „Musik“.*

    *Wenn Pfad nicht existiert → Exception oder leere Liste.*

## 5. getTopItems(int k)


    Eingabe: 5

    Erwartung: Die 5 Items mit dem höchsten rating (float-Wert) in absteigender Reihenfolge.

    Teste auch 1 → sollte nur das am besten bewertete Item zeigen.

## 6. getSimilarCheaperItems(String asin)

    Eingabe: B00004RDTU

    Erwartung: Items aus derselben Kategorie, die einen günstigeren Preis haben als das Original.

    Falls keine gefunden werden → leere Liste.

## 7. addNewReview(String asin, String kundenId, int rating, String text)


    Eingabe:

    ASIN: B00005JPLW
    Kunden-ID: K123
    Bewertung: 4
    Text: Gute CD!


    Erwartung:

    Meldung „Review hinzugefügt!“.

    In DB: Neue Rezension in Tabelle rezension.

    Item-Rating (rating, rating_counter) sollte aktualisiert sein.

## 8. getTrolls(double maxAverageRating)

    Eingabe: 3.8

    Erwartung: Alle Kunden, deren durchschnittliche Bewertung < 3.8 liegt.

    Gut, wenn du einen Testkunden hast, der nur 1-Sterne-Reviews abgibt.

    Vorzeigekunden: 44 (Bewertung 1 , 4, 5, 5), 46 (Bewertung 2, 2, 2, 2, 5, 5, 5, 5), 66 (Bewertung 1, 1)

## 9. getOffers(String asin)

    Eingabe: B00005JPLW

    Erwartung: Liste aller Angebote für dieses Item, z. B. verschiedene Shops mit Preisen.
