import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * CSVをRFC4180寄りにパースして、全列 or 指定列だけを抽出して再出力する。
 *
 * 使い方例:
 *  java CsvExtract in.csv out.csv
 *  java CsvExtract in.csv out.csv --cols Name,PostalCode__c
 *  java CsvExtract in.csv out.csv --cols Name,PostalCode__c --encoding UTF-8
 *
 * 注意:
 * - 1行目はヘッダー扱い
 * - 区切りはカンマ固定
 */
public class CsvExtract {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: java CsvExtract <in.csv> <out.csv> [--cols A,B,C] [--encoding UTF-8]");
            System.exit(2);
        }

        String inPath = args[0];
        String outPath = args[1];

        String colsArg = null;
        Charset encoding = StandardCharsets.UTF_8;

        for (int i = 2; i < args.length; i++) {
            String a = args[i];
            if ("--cols".equals(a) && i + 1 < args.length) {
                colsArg = args[++i];
            } else if ("--encoding".equals(a) && i + 1 < args.length) {
                encoding = Charset.forName(args[++i]);
            } else {
                System.err.println("Unknown arg: " + a);
                System.exit(2);
            }
        }

        List<String> requestedCols = null;
        if (colsArg != null && !colsArg.trim().isEmpty()) {
            requestedCols = splitCols(colsArg);
        }

        try (Reader r = new BufferedReader(new InputStreamReader(new FileInputStream(inPath), encoding));
             Writer w = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outPath), encoding))) {

            CsvParser parser = new CsvParser(r);
            CsvWriter writer = new CsvWriter(w);

            // ヘッダー
            List<String> header = parser.nextRecord();
            if (header == null) {
                throw new IllegalStateException("CSVが空です");
            }

            // 抽出列インデックス決定
            List<Integer> keepIdx;
            List<String> outHeader;

            if (requestedCols == null) {
                keepIdx = new ArrayList<>();
                for (int i = 0; i < header.size(); i++) keepIdx.add(i);
                outHeader = header;
            } else {
                Map<String, Integer> headerIndex = new HashMap<>();
                for (int i = 0; i < header.size(); i++) {
                    headerIndex.put(header.get(i), i);
                }

                keepIdx = new ArrayList<>();
                outHeader = new ArrayList<>();
                for (String col : requestedCols) {
                    Integer idx = headerIndex.get(col);
                    if (idx == null) {
                        // 列が見つからない場合はスキップ or エラー。今回は「エラー」にして事故防止。
                        throw new IllegalArgumentException("指定列がヘッダーに存在しません: " + col);
                    }
                    keepIdx.add(idx);
                    outHeader.add(col);
                }
            }

            // 出力ヘッダー
            writer.writeRecord(outHeader);

            // データ
            List<String> record;
            long rowNo = 1; // ヘッダー=1行目
            while ((record = parser.nextRecord()) != null) {
                rowNo++;

                List<String> outRec = new ArrayList<>(keepIdx.size());
                for (int idx : keepIdx) {
                    // 行によって列数が足りないCSVもあるので、安全に拾う
                    outRec.add(idx < record.size() ? record.get(idx) : "");
                }

                writer.writeRecord(outRec);
            }

            writer.flush();
        }
    }

    private static List<String> splitCols(String s) {
        String[] parts = s.split(",");
        List<String> out = new ArrayList<>();
        for (String p : parts) {
            String t = p.trim();
            if (!t.isEmpty()) out.add(t);
        }
        return out;
    }

    /**
     * RFC4180寄り CSVパーサ:
     * - カンマ区切り
     * - "..." のクォート対応
     * - セル内の "" は " として復元
     * - クォート内の改行もセルの一部として扱う
     */
    static final class CsvParser {
        private final Reader r;
        private int pushed = -2; // -2: none, -1: EOF, else: char

        CsvParser(Reader r) {
            this.r = r;
        }

        List<String> nextRecord() throws IOException {
            List<String> row = new ArrayList<>();
            StringBuilder cell = new StringBuilder();

            boolean inQuotes = false;
            boolean anyRead = false;

            while (true) {
                int ch = read();
                if (ch == -1) {
                    if (!anyRead) return null; // 完全にEOF
                    // EOFでレコード確定
                    row.add(cell.toString());
                    return row;
                }

                anyRead = true;

                if (inQuotes) {
                    if (ch == '"') {
                        int next = read();
                        if (next == '"') {
                            // "" -> "
                            cell.append('"');
                        } else {
                            // クォート終わり
                            inQuotes = false;
                            unread(next);
                        }
                    } else {
                        cell.append((char) ch);
                    }
                    continue;
                }

                // quotes外
                if (ch == '"') {
                    // セル開始直後の " をクォート開始として扱う
                    // （厳密には前に文字がある場合の扱いはCSV方言差あり。ここでは開始とみなす）
                    inQuotes = true;
                    continue;
                }

                if (ch == ',') {
                    row.add(cell.toString());
                    cell.setLength(0);
                    continue;
                }

                if (ch == '\r') {
                    int next = read();
                    if (next != '\n') unread(next);
                    row.add(cell.toString());
                    return row;
                }

                if (ch == '\n') {
                    row.add(cell.toString());
                    return row;
                }

                cell.append((char) ch);
            }
        }

        private int read() throws IOException {
            if (pushed != -2) {
                int c = pushed;
                pushed = -2;
                return c;
            }
            return r.read();
        }

        private void unread(int c) {
            if (c == -2) throw new IllegalStateException("cannot unread -2");
            pushed = c;
        }
    }

    /**
     * CSV出力（安全側）:
     * - , " CR LF を含む場合は "..." で囲う
     * - " は "" にエスケープ
     * - 先頭/末尾空白やタブも囲う（事故防止）
     */
    static final class CsvWriter {
        private final Writer w;

        CsvWriter(Writer w) {
            this.w = w;
        }

        void writeRecord(List<String> cols) throws IOException {
            for (int i = 0; i < cols.size(); i++) {
                if (i > 0) w.write(',');
                w.write(escape(cols.get(i)));
            }
            w.write("\n");
        }

        void flush() throws IOException {
            w.flush();
        }

        private String escape(String s) {
            if (s == null) return "";

            boolean mustQuote = false;
            if (s.indexOf(',') >= 0 || s.indexOf('"') >= 0 || s.indexOf('\n') >= 0 || s.indexOf('\r') >= 0) {
                mustQuote = true;
            }
            if (!mustQuote && !s.isEmpty()) {
                char first = s.charAt(0);
                char last = s.charAt(s.length() - 1);
                if (first == ' ' || first == '\t' || last == ' ' || last == '\t') {
                    mustQuote = true;
                }
            }

            if (s.indexOf('"') >= 0) {
                s = s.replace("\"", "\"\"");
                mustQuote = true;
            }

            return mustQuote ? "\"" + s + "\"" : s;
        }
    }
}