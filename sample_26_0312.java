import java.util.Map;

public class CsvRecord {

    private final Map<String, String> values;

    public CsvRecord(Map<String, String> values) {
        this.values = values;
    }

    public String get(String headerName) {
        return values.get(headerName);
    }

    public String getOrDefault(String headerName, String defaultValue) {
        return values.getOrDefault(headerName, defaultValue);
    }

    public boolean contains(String headerName) {
        return values.containsKey(headerName);
    }

    @Override
    public String toString() {
        return values.toString();
    }
}