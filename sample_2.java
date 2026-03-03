import org.apache.ibatis.cursor.Cursor;

import java.io.*;
import java.lang.reflect.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public final class SfBulkCsvExporter {

    public static <T> void export(
            Class<T> dtoClass,
            Cursor<T> cursor,
            OutputStream out,
            InputStream mappingStream,
            boolean mappingIsDtoToSf
    ) throws IOException {

        Properties p = new Properties();
        try (Reader r = new InputStreamReader(mappingStream, StandardCharsets.UTF_8)) {
            p.load(r);
        }

        LinkedHashMap<String, String> sfToSpec = new LinkedHashMap<>();
        for (String key : p.stringPropertyNames()) {
            String value = p.getProperty(key);
            if (!mappingIsDtoToSf) {
                sfToSpec.put(key.trim(), value.trim());  // SF -> spec
            } else {
                sfToSpec.put(value.trim(), key.trim());  // 反転（ただし spec が壊れる可能性あるので基本非推奨）
            }
        }

        Map<String, Field> dtoFields = new HashMap<>();
        for (Class<?> c = dtoClass; c != null && c != Object.class; c = c.getSuperclass()) {
            for (Field f : c.getDeclaredFields()) {
                if (Modifier.isStatic(f.getModifiers())) continue;
                f.setAccessible(true);
                dtoFields.putIfAbsent(f.getName(), f);
            }
        }

        List<Column<T>> columns = new ArrayList<>();
        for (Map.Entry<String, String> e : sfToSpec.entrySet()) {
            String sfName = e.getKey();
            String spec = e.getValue(); // dtoField|formatter

            MappingSpec ms = MappingSpec.parse(spec);
            Field f = dtoFields.get(ms.dtoFieldName);
            if (f == null) {
                System.err.println("[INFO] Skip column (DTO field not found): "
                        + sfName + " -> " + ms.dtoFieldName);
                continue;
            }
            columns.add(new Column<>(sfName, f, ms.format));
        }

        if (columns.isEmpty()) throw new IllegalStateException("出力可能な列が1つもありません");

        try (PrintWriter w = new PrintWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))) {
            w.println(columns.stream().map(c -> c.sfName).collect(Collectors.joining(",")));

            for (T dto : cursor) {
                List<String> row = new ArrayList<>(columns.size());
                for (Column<T> col : columns) {
                    Object raw;
                    try {
                        raw = col.field.get(dto);
                    } catch (IllegalAccessException ex) {
                        throw new RuntimeException(ex);
                    }
                    String formatted = formatForSalesforce(raw, col.format);
                    row.add(escapeCsv(formatted));
                }
                w.println(String.join(",", row));
            }
            w.flush();
        }
    }

    // --- 変換ルール ---
    private static final DateTimeFormatter DATE = DateTimeFormatter.ISO_LOCAL_DATE; // yyyy-MM-dd
    private static final DateTimeFormatter DATETIME_UTC = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX")
            .withZone(ZoneOffset.UTC);

    private static String formatForSalesforce(Object v, String format) {
        if (v == null) return "";

        // 指定なし：基本は toString
        if (format == null || format.isBlank() || format.equals("raw")) {
            return String.valueOf(v);
        }

        switch (format) {
            case "date": { // yyyy-MM-dd
                if (v instanceof LocalDate ld) return DATE.format(ld);
                if (v instanceof java.sql.Date sd) return sd.toLocalDate().format(DATE);
                if (v instanceof java.util.Date ud) {
                    Instant i = ud.toInstant();
                    return LocalDateTime.ofInstant(i, ZoneOffset.UTC).toLocalDate().format(DATE);
                }
                // 文字列が来た場合はそのまま（既に整形済み想定）
                return String.valueOf(v);
            }
            case "datetime_utc": { // yyyy-MM-ddTHH:mm:ss.SSSZ
                Instant instant = null;
                if (v instanceof Instant i) instant = i;
                else if (v instanceof OffsetDateTime odt) instant = odt.toInstant();
                else if (v instanceof ZonedDateTime zdt) instant = zdt.toInstant();
                else if (v instanceof LocalDateTime ldt) instant = ldt.toInstant(ZoneOffset.UTC);
                else if (v instanceof java.util.Date ud) instant = ud.toInstant();

                if (instant != null) return DATETIME_UTC.format(instant);
                return String.valueOf(v);
            }
            case "bool": { // TRUE/FALSE
                if (v instanceof Boolean b) return b ? "TRUE" : "FALSE";
                return String.valueOf(v);
            }
            case "int": {
                if (v instanceof Number n) return String.valueOf(n.longValue());
                return String.valueOf(v);
            }
            case "decimal": {
                if (v instanceof BigDecimal bd) return bd.toPlainString();
                if (v instanceof Number n) return BigDecimal.valueOf(n.doubleValue()).toPlainString();
                return String.valueOf(v);
            }
            case "decimal2": {
                BigDecimal bd = null;
                if (v instanceof BigDecimal b) bd = b;
                else if (v instanceof Number n) bd = BigDecimal.valueOf(n.doubleValue());
                if (bd != null) return bd.setScale(2, BigDecimal.ROUND_HALF_UP).toPlainString();
                return String.valueOf(v);
            }
            default:
                // 将来拡張用：未知のformatは素通し
                return String.valueOf(v);
        }
    }

    private static String escapeCsv(String s) {
        if (s == null) return "";
        if (s.contains(",") || s.contains("\n") || s.contains("\r") || s.contains("\"")) {
            return "\"" + s.replace("\"", "\"\"") + "\"";
        }
        return s;
    }

    private static final class Column<T> {
        final String sfName;
        final Field field;
        final String format;

        Column(String sfName, Field field, String format) {
            this.sfName = sfName;
            this.field = field;
            this.format = format;
        }
    }

    private static final class MappingSpec {
        final String dtoFieldName;
        final String format;

        private MappingSpec(String dtoFieldName, String format) {
            this.dtoFieldName = dtoFieldName;
            this.format = format;
        }

        static MappingSpec parse(String spec) {
            // "field" or "field|format"
            String[] parts = spec.split("\\|", 2);
            String field = parts[0].trim();
            String fmt = (parts.length == 2) ? parts[1].trim() : null;
            return new MappingSpec(field, fmt);
        }
    }
}