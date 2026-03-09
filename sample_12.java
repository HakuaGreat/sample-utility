Properties mapper = new Properties();

try (InputStream in =
     getClass().getResourceAsStream("/sf-mapping.properties")) {

    mapper.load(in);
}




----------------------------

public <T> T mapRow(
        Map<String,String> row,
        Class<T> dtoClass,
        Properties mapper) {

    try {

        T dto = dtoClass.getDeclaredConstructor().newInstance();

        for (Map.Entry<String,String> e : row.entrySet()) {

            String sfField = e.getKey();
            String value = e.getValue();

            String dtoField = mapper.getProperty(sfField);

            if (dtoField == null) {
                continue;
            }

            Field field = dtoClass.getDeclaredField(dtoField);
            field.setAccessible(true);

            Object converted = convert(field.getType(), value);

            field.set(dto, converted);
        }

        return dto;

    } catch (Exception ex) {
        throw new RuntimeException(ex);
    }
}


----------------------------



private Object convert(Class<?> type, String value) {

    if (value == null || value.isEmpty()) {
        return null;
    }

    if (type == String.class) {
        return value;
    }

    if (type == Integer.class || type == int.class) {
        return Integer.valueOf(value);
    }

    if (type == Long.class || type == long.class) {
        return Long.valueOf(value);
    }

    if (type == Boolean.class || type == boolean.class) {
        return Boolean.valueOf(value);
    }

    if (type == Timestamp.class) {
        return Timestamp.from(
            java.time.OffsetDateTime.parse(value).toInstant()
        );
    }

    return value;
}





----------------------------






while ((line = br.readLine()) != null) {

    List<String> values = parseCsvLine(line);

    Map<String,String> row = toRowMap(headers, values);

    RetailStoreDto dto =
        mapRow(row, RetailStoreDto.class, mapper);

    repository.merge(dto);
}





----------------------------

import java.util.*;

public class CsvParser {

    public static List<String> parseLine(String line) {

        List<String> result = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {

            char c = line.charAt(i);

            if (c == '"') {

                if (inQuotes && i + 1 < line.length()
                        && line.charAt(i + 1) == '"') {

                    sb.append('"');
                    i++;

                } else {

                    inQuotes = !inQuotes;
                }

            } else if (c == ',' && !inQuotes) {

                result.add(sb.toString());
                sb.setLength(0);

            } else {

                sb.append(c);
            }
        }

        result.add(sb.toString());

        return result;
    }
}




----------------------------



import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.*;

public class SfCsvMapper<T> {

    private final Map<String,String> mapping;
    private final Map<String,Field> fieldCache;
    private final Class<T> dtoClass;

    public SfCsvMapper(Properties props, Class<T> dtoClass) {

        this.mapping = new HashMap<>();
        this.fieldCache = new HashMap<>();
        this.dtoClass = dtoClass;

        for (String name : props.stringPropertyNames()) {

            mapping.put(name.toLowerCase(), props.getProperty(name));
        }

        for (Field f : dtoClass.getDeclaredFields()) {

            f.setAccessible(true);
            fieldCache.put(f.getName(), f);
        }
    }

    public T map(Map<String,String> row) {

        try {

            T dto = dtoClass.getDeclaredConstructor().newInstance();

            for (Map.Entry<String,String> e : row.entrySet()) {

                String sfField = e.getKey().toLowerCase();
                String value = e.getValue();

                String dtoField = mapping.get(sfField);

                if (dtoField == null) continue;

                Field field = fieldCache.get(dtoField);

                Object converted = convert(field.getType(), value);

                field.set(dto, converted);
            }

            return dto;

        } catch (Exception ex) {

            throw new RuntimeException(ex);
        }
    }

    private Object convert(Class<?> type, String value) {

        if (value == null || value.isEmpty()) return null;

        if (type == String.class) return value;

        if (type == Integer.class || type == int.class)
            return Integer.valueOf(value);

        if (type == Long.class || type == long.class)
            return Long.valueOf(value);

        if (type == Boolean.class || type == boolean.class)
            return Boolean.valueOf(value);

        if (type == Timestamp.class)
            return Timestamp.from(
                    OffsetDateTime.parse(value).toInstant());

        return value;
    }
}


----------------------------



import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.*;

public class SalesforceCsvSyncService {

    public void sync(
            InputStream csvStream,
            Connection conn) throws Exception {

        Properties props = new Properties();

        try (InputStream in =
                getClass().getResourceAsStream(
                        "/sf-mapping.properties")) {

            props.load(in);
        }

        SfCsvMapper<RetailStoreDto> mapper =
                new SfCsvMapper<>(props, RetailStoreDto.class);

        RetailStoreRepository repo =
                new RetailStoreRepository();

        BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        csvStream,
                        StandardCharsets.UTF_8));

        String headerLine = br.readLine();

        List<String> headers =
                CsvParser.parseLine(headerLine);

        String line;

        while ((line = br.readLine()) != null) {

            List<String> values =
                    CsvParser.parseLine(line);

            Map<String,String> row =
                    new LinkedHashMap<>();

            for (int i = 0; i < headers.size(); i++) {

                row.put(
                        headers.get(i),
                        i < values.size()
                                ? values.get(i)
                                : "");
            }

            RetailStoreDto dto = mapper.map(row);

            repo.merge(conn, dto);
        }
    }
}



----------------------------



List<RetailStoreDto> list = new ArrayList<>();

while ((line = br.readLine()) != null) {
    Map<String, String> row = ...
    RetailStoreDto dto = mapper.map(row);
    list.add(dto);
}

repository.insert(list);