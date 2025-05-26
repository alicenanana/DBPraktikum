public class ImportErrors {
    public static void log(String entity, String attribute, String message) {
        System.err.printf("ERROR: Entity=%s, Attribut=%s, Fehler=%s%n", entity, attribute, message);
        // Optional: Schreibe das auch in eine Textdatei
    }
}
