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
            InputStream mappingPropertiesStream,
            boolean mappingIsDtoToSf // trueなら dto=sf を反転して使う
    ) throws IOException {

        Properties p = new Properties();
        // propertiesはISO-8859-1前提なので、UTF-8で書いてるならReaderで読む
        try (Reader r = new InputStreamReader(mappingPropertiesStream, StandardCharsets.UTF_8)) {
            p.load(r);
        }

        // SF->DTO の Map に揃える
        Map<String, String> sfToDto = new LinkedHashMap<>();
        for (String k : p.stringPropertyNames()) {
            String v = p.getProperty(k);

            if (!mappingIsDtoToSf) {
                // k=SF, v=DTO
                sfToDto.put(k.trim(), v.trim());
            } else {
                // k=DTO, v=SF なので反転
                sfToDto.put(v.trim(), k.trim());
            }
        }

        // DTOのFieldを解決（1回だけ）
        Map<String, Field> dtoFields = new HashMap<>();
        for (Field f : dtoClass.getDeclaredFields()) {
            if (Modifier.isStatic(f.getModifiers())) continue;
            f.setAccessible(true);
            dtoFields.put(f.getName(), f);
        }

        // 出力
        try (PrintWriter w = new PrintWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))) {

            // ヘッダー（SF名）
            w.println(String.join(",", sfToDto.keySet()));

            // 行
            for (T dto : cursor) {
                List<String> cols = new ArrayList<>(sfToDto.size());

                for (Map.Entry<String, String> e : sfToDto.entrySet()) {
                    String dtoFieldName = e.getValue();
                    Field f = dtoFields.get(dtoFieldName);
                    if (f == null) {
                        throw new IllegalArgumentException("DTOにフィールドが無い: " + dtoFieldName + " (SF列: " + e.getKey() + ")");
                    }
                    Object v;
                    try {
                        v = f.get(dto);
                    } catch (IllegalAccessException ex) {
                        throw new RuntimeException(ex);
                    }
                    cols.add(escapeCsv(v == null ? "" : String.valueOf(v)));
                }

                w.println(String.join(",", cols));
            }

            w.flush();
        }
    }

    private static String escapeCsv(String s) {
        // BulkのCSVで事故りやすいので最低限のエスケープ
        if (s.contains(",") || s.contains("\n") || s.contains("\r") || s.contains("\"")) {
            return "\"" + s.replace("\"", "\"\"") + "\"";
        }
        return s;
    }
}