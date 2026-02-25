package your.pkg.csv;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public final class CsvPrintWriter {
    private CsvPrintWriter() {}

    /**
     * UTF-8でCSV出力（BOMなし、Excel非想定）
     * - ヘッダあり
     * - 列順は cols の順（ヘッダ順＝列順）
     * - CSVエスケープ対応
     * - rowsはStreamで逐次書き込み（大きくてもOK）
     */
    public static <T> void writeUtf8(Path out, List<CsvCol<T>> cols, Stream<T> rows) throws IOException {
        try (BufferedWriter bw = Files.newBufferedWriter(out, StandardCharsets.UTF_8);
             PrintWriter pw = new PrintWriter(bw)) {

            // header
            writeHeader(pw, cols);

            // data
            rows.forEach(row -> writeDataRow(pw, cols, row));

            pw.flush();
            if (pw.checkError()) {
                throw new IllegalStateException("CSV write failed (PrintWriter error).");
            }
        }
    }

    private static <T> void writeHeader(PrintWriter pw, List<CsvCol<T>> cols) {
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) pw.print(',');
            pw.print(escape(cols.get(i).getHeader()));
        }
        pw.print('\n');
    }

    private static <T> void writeDataRow(PrintWriter pw, List<CsvCol<T>> cols, T row) {
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) pw.print(',');
            pw.print(escape(cols.get(i).valueAsString(row)));
        }
        pw.print('\n');
    }

    /**
     * RFC4180の基本：
     * 値に , " 改行(\n/\r) が含まれる場合はダブルクォートで囲み、
     * 内部の " は "" にする。
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
            if (c == '"') sb.append("\"\"");
            else sb.append(c);
        }
        sb.append('"');
        return sb.toString();
    }
}
