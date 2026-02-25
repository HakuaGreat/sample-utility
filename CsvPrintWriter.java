package your.pkg.csv;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public final class CsvPrintWriter {

    private CsvPrintWriter() {}

    public static <T> void writeUtf8(Path out,
                                     List<CsvCol<T>> cols,
                                     Stream<T> rows,
                                     boolean withBomForExcel) throws IOException {

        try (OutputStream os = Files.newOutputStream(out)) {

            if (withBomForExcel) {
                os.write(new byte[]{(byte)0xEF, (byte)0xBB, (byte)0xBF});
            }

            try (PrintWriter pw = new PrintWriter(
                    new OutputStreamWriter(os, StandardCharsets.UTF_8))) {

                // header
                writeRow(pw, extractHeaders(cols));

                // data
                rows.forEach(row -> writeRow(pw, extractValues(cols, row)));

                pw.flush();

                if (pw.checkError()) {
                    throw new IllegalStateException("CSV write failed (PrintWriter error).");
                }
            }
        }
    }

    private static <T> List<String> extractHeaders(List<CsvCol<T>> cols) {
        java.util.ArrayList<String> list = new java.util.ArrayList<>();
        for (CsvCol<T> col : cols) {
            list.add(col.getHeader());
        }
        return list;
    }

    private static <T> List<String> extractValues(List<CsvCol<T>> cols, T row) {
        java.util.ArrayList<String> list = new java.util.ArrayList<>();
        for (CsvCol<T> col : cols) {
            list.add(col.valueAsString(row));
        }
        return list;
    }

    private static void writeRow(PrintWriter pw, List<String> values) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) pw.print(',');
            pw.print(escape(values.get(i)));
        }
        pw.print("\r\n"); // Windows/Excel互換
    }

    /**
     * RFC4180基本準拠のエスケープ
     */
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
            if (c == '"') {
                sb.append("\"\"");
            } else {
                sb.append(c);
            }
        }

        sb.append('"');
        return sb.toString();
    }
}
