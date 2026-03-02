import org.apache.ibatis.cursor.Cursor;

import java.io.*;
import java.lang.reflect.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public final class SfBulkCsvExporter {

    public static <T> void export(
            Class<T> dtoClass,
            Cursor<T> cursor,
            OutputStream out,
            InputStream mappingStream,
            boolean mappingIsDtoToSf // trueなら DTO=SF を反転
    ) throws IOException {

        // ① mapping読み込み
        Properties p = new Properties();
        try (Reader r = new InputStreamReader(mappingStream, StandardCharsets.UTF_8)) {
            p.load(r);
        }

        // ② SF→DTO に統一
        LinkedHashMap<String, String> sfToDto = new LinkedHashMap<>();
        for (String key : p.stringPropertyNames()) {
            String value = p.getProperty(key);
            if (!mappingIsDtoToSf) {
                sfToDto.put(key.trim(), value.trim());
            } else {
                sfToDto.put(value.trim(), key.trim());
            }
        }

        // ③ DTOフィールド取得（継承対応）
        Map<String, Field> dtoFields = new HashMap<>();
        for (Class<?> c = dtoClass; c != null && c != Object.class; c = c.getSuperclass()) {
            for (Field f : c.getDeclaredFields()) {
                if (Modifier.isStatic(f.getModifiers())) continue;
                f.setAccessible(true);
                dtoFields.putIfAbsent(f.getName(), f);
            }
        }

        // ④ DTOに存在する列だけ残す
        List<Column<T>> columns = new ArrayList<>();

        for (Map.Entry<String, String> e : sfToDto.entrySet()) {
            String sfName = e.getKey();
            String dtoFieldName = e.getValue();

            Field f = dtoFields.get(dtoFieldName);
            if (f == null) {
                System.err.println("[INFO] Skip column (DTO field not found): "
                        + sfName + " -> " + dtoFieldName);
                continue; // ← 列ごとスキップ
            }

            columns.add(new Column<>(sfName, f));
        }

        if (columns.isEmpty()) {
            throw new IllegalStateException("出力可能な列が1つもありません");
        }

        // ⑤ CSV出力
        try (PrintWriter w = new PrintWriter(
                new OutputStreamWriter(out, StandardCharsets.UTF_8))) {

            // ヘッダー
            w.println(columns.stream()
                    .map(c -> c.sfName)
                    .collect(Collectors.joining(",")));

            // データ行
            for (T dto : cursor) {
                List<String> row = new ArrayList<>(columns.size());

                for (Column<T> col : columns) {
                    try {
                        Object value = col.field.get(dto);
                        row.add(escapeCsv(value == null ? "" : String.valueOf(value)));
                    } catch (IllegalAccessException ex) {
                        throw new RuntimeException(ex);
                    }
                }

                w.println(String.join(",", row));
            }

            w.flush();
        }
    }

    private static String escapeCsv(String s) {
        if (s.contains(",") || s.contains("\n")
                || s.contains("\r") || s.contains("\"")) {
            return "\"" + s.replace("\"", "\"\"") + "\"";
        }
        return s;
    }

    private static class Column<T> {
        final String sfName;
        final Field field;

        Column(String sfName, Field field) {
            this.sfName = sfName;
            this.field = field;
        }
    }
}