import java.util.Map;

public class CsvRecord {

    private final Map<String, String> values;

    public CsvRecord(Map<String, String> values) {
        this.values = values;
    }

    public String get(String headerName) {
        if (headerName == null) {
            return null;
        }
        return values.get(normalize(headerName));
    }

    public boolean contains(String headerName) {
        if (headerName == null) {
            return false;
        }
        return values.containsKey(normalize(headerName));
    }

    @Override
    public String toString() {
        return values.toString();
    }

    private String normalize(String value) {
        return value.replace("\uFEFF", "").trim().toLowerCase();
    }
}