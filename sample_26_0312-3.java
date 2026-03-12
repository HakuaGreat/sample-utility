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
        this.headers = parser.parse(headerLine);
    }

    public CsvRecord readRecord() throws IOException {
        String line = reader.readLine();
        if (line == null) {
            return null;
        }

        List<String> values = parser.parse(line);
        Map<String, String> map = new LinkedHashMap<>();

        for (int i = 0; i < headers.size(); i++) {
            String value = i < values.size() ? values.get(i) : "";
            map.put(headers.get(i), value);
        }

        return new CsvRecord(map);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}