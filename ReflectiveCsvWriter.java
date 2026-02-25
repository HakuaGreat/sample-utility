package your.pkg.csv;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * DTOクラスのフィールドからCSVを自動生成
 * - @CsvName でヘッダ名変更、@CsvIgnore で除外
 */
public final class ReflectiveCsvWriter<T> {

    private final List<Field> fields;
    private final List<String> headers;

    private ReflectiveCsvWriter(Class<T> dtoClass) {
        this.fields = new ArrayList<>();
        this.headers = new ArrayList<>();

        for (Field f : dtoClass.getDeclaredFields()) {
            // static / synthetic は除外
            if (Modifier.isStatic(f.getModifiers())) continue;
            if (f.isSynthetic()) continue;

            // 明示除外
            if (f.getAnnotation(CsvIgnore.class) != null) continue;

            f.setAccessible(true);
            fields.add(f);

            CsvName name = f.getAnnotation(CsvName.class);
            headers.add(name != null ? name.value() : f.getName());
        }
    }

    import org.apache.ibatis.cursor.Cursor;

// （既存のwriteUtf8(Stream...)はそのまま）

public void writeUtf8(Path out, Cursor<T> cursor) throws IOException {
    try (BufferedWriter bw = Files.newBufferedWriter(out, StandardCharsets.UTF_8);
         PrintWriter pw = new PrintWriter(bw);
         Cursor<T> c = cursor) { // ← ★MyBatis CursorはAutoCloseableなのでtryで閉じられる

        // header
        writeRow(pw, headers);

        // data（逐次）
        for (T row : c) {
            writeDtoRow(pw, row);
        }

        pw.flush();
        if (pw.checkError()) {
            throw new IllegalStateException("CSV write failed (PrintWriter error).");
        }
    }
}


    public static <T> ReflectiveCsvWriter<T> forClass(Class<T> dtoClass) {
        return new ReflectiveCsvWriter<>(dtoClass);
    }

    public void writeUtf8(Path out, Stream<T> rows) throws IOException {
        try (BufferedWriter bw = Files.newBufferedWriter(out, StandardCharsets.UTF_8);
             PrintWriter pw = new PrintWriter(bw)) {

            // header
            writeRow(pw, headers);

            // data
            rows.forEach(row -> writeDtoRow(pw, row));

            pw.flush();
            if (pw.checkError()) {
                throw new IllegalStateException("CSV write failed (PrintWriter error).");
            }
        }
    }

    private void writeDtoRow(PrintWriter pw, T row) {
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) pw.print(',');
            pw.print(escape(getAsString(fields.get(i), row)));
        }
        pw.print('\n');
    }

    private static String getAsString(Field f, Object row) {
        try {
            Object v = f.get(row);
            return v == null ? "" : String.valueOf(v);
        } catch (IllegalAccessException e) {
            // setAccessible(true)済みなので基本起きない想定
            throw new IllegalStateException("Cannot access field: " + f.getName(), e);
        }
    }

    private static void writeRow(PrintWriter pw, List<String> values) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) pw.print(',');
            pw.print(escape(values.get(i)));
        }
        pw.print('\n');
    }

    /** RFC4180基本のエスケープ */
    private static String escape(String s) {
        if (s == null) return "";

        boolean needQuote = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"' || c == ',' || c == '\n' || c == '\r') {
                needQuote = true;
                break;
            }
        }
        if (!needQuote) return s;

        StringBuilder sb = new StringBuilder(s.length() + 2);
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') sb.append("\"\"");
            else sb.append(c);
        }
        sb.append('"');
        return sb.toString();
    }
}
