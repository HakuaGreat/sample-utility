import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class GenericCsvReader {

    public <T> List<T> read(Reader reader, CsvRowFilter filter, CsvRowMapper<T> mapper) throws IOException {
        List<T> result = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(reader)) {
            String headerLine = br.readLine();
            if (headerLine == null || headerLine.trim().isEmpty()) {
                return result;
            }

            List<String> headers = parseCsvLine(headerLine);

            String line;
            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }

                List<String> values = parseCsvLine(line);
                Map<String, String> rowMap = toRowMap(headers, values);

                if (filter == null || filter.test(rowMap)) {
                    T dto = mapper.map(rowMap);
                    if (dto != null) {
                        result.add(dto);
                    }
                }
            }
        }

        return result;
    }

    public <T> void readAndProcess(Reader reader, CsvRowFilter filter, CsvRowMapper<T> mapper, DtoProcessor<T> processor)
            throws Exception {
        try (BufferedReader br = new BufferedReader(reader)) {
            String headerLine = br.readLine();
            if (headerLine == null || headerLine.trim().isEmpty()) {
                return;
            }

            List<String> headers = parseCsvLine(headerLine);

            String line;
            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }

                List<String> values = parseCsvLine(line);
                Map<String, String> rowMap = toRowMap(headers, values);

                if (filter == null || filter.test(rowMap)) {
                    T dto = mapper.map(rowMap);
                    if (dto != null) {
                        processor.process(dto);
                    }
                }
            }
        }
    }

    private Map<String, String> toRowMap(List<String> headers, List<String> values) {
        Map<String, String> rowMap = new LinkedHashMap<>();
        for (int i = 0; i < headers.size(); i++) {
            String value = i < values.size() ? values.get(i) : null;
            rowMap.put(headers.get(i), value);
        }
        return rowMap;
    }

    private List<String> parseCsvLine(String line) {
        List<String> values = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);

            if (ch == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    sb.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (ch == ',' && !inQuotes) {
                values.add(sb.toString());
                sb.setLength(0);
            } else {
                sb.append(ch);
            }
        }

        values.add(sb.toString());
        return values;
    }
}