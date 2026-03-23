import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CsvResultReader implements AutoCloseable {

    private final BufferedReader reader;
    private final CsvLineParser parser;
    private List<String> headers;

    public CsvResultReader(Reader reader) {
        this.reader = new BufferedReader(reader);
        this.parser = new CsvLineParser();
    }

    public void open() throws IOException {
        String headerLine = reader.readLine();
        if (headerLine == null) {
            throw new IllegalStateException("CSVヘッダがありません");
        }

        headers = parser.parse(headerLine);
    }

    public CsvRecord readRecord() throws IOException {
        String line = reader.readLine();
        if (line == null) {
            return null;
        }

        List<String> values = parser.parse(line);
        Map<String, String> row = new LinkedHashMap<>();

        for (int i = 0; i < headers.size(); i++) {
            String header = normalize(headers.get(i));
            String value = i < values.size() ? values.get(i) : "";
            row.put(header, value);
        }

        return new CsvRecord(row);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    private String normalize(String value) {
        return value.replace("\uFEFF", "").trim().toLowerCase();
    }
}