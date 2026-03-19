import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class GenericCsvReader {

    public <T> List<T> read(Reader reader, CsvRowFilter filter, CsvRowMapper<T> mapper) throws IOException {
        return read(reader, filter, mapper, null);
    }

    public <T> List<T> read(Reader reader,
                            CsvRowFilter filter,
                            CsvRowMapper<T> mapper,
                            CsvHeaderValidator headerValidator) throws IOException {
        List<T> result = new ArrayList<>();

        if (reader == null) {
            return result;
        }

        try (BufferedReader br = new BufferedReader(reader)) {
            String headerLine = br.readLine();

            // ファイル自体が空
            if (headerLine == null) {
                return result;
            }

            // ヘッダ行が空文字
            if (headerLine.trim().isEmpty()) {
                return result;
            }

            List<String> headers = parseCsvLine(headerLine);

            if (headerValidator != null) {
                headerValidator.validate(headers);
            }

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

    public <T> int readAndProcess(Reader reader,
                                  CsvRowFilter filter,
                                  CsvRowMapper<T> mapper,
                                  DtoListProcessor<T> processor) throws Exception {
        return readAndProcess(reader, filter, mapper, null, processor);
    }

    public <T> int readAndProcess(Reader reader,
                                  CsvRowFilter filter,
                                  CsvRowMapper<T> mapper,
                                  CsvHeaderValidator headerValidator,
                                  DtoListProcessor<T> processor) throws Exception {

        List<T> dtoList = read(reader, filter, mapper, headerValidator);

        if (dtoList == null || dtoList.isEmpty()) {
            return 0;
        }

        processor.process(dtoList);
        return dtoList.size();
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