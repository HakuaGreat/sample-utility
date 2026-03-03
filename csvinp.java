import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * RFC4180寄りのCSVを「レコード単位」でストリーミング処理するためのツール群（Java11 / 標準のみ）。
 *
 * 対応:
 * - カンマ区切り
 * - "..." クォート
 * - セル内 "" は " として復元
 * - クォート内改行を許容
 *
 * 使いどころ:
 * - 既存CSVの正規化（再エスケープ）
 * - 全列 or 指定列の抽出
 * - SF Bulk向けに「列ズレ」しないCSVを生成
 */
public final class CsvToolkit {

    private CsvToolkit() {}

    // -------------------------
    // Public API
    // -------------------------

    /** 先頭行をヘッダーとして読み、以降レコードを順に consumer に渡す（ストリーミング）。 */
    public static void forEachRecordWithHeader(
            Reader reader,
            CsvRecordConsumer consumer
    ) throws IOException {
        Objects.requireNonNull(reader, "reader");
        Objects.requireNonNull(consumer, "consumer");

        CsvParser p = new CsvParser(reader);
        List<String> header = p.nextRecord();
        if (header == null) return;

        consumer.onHeader(Collections.unmodifiableList(header));

        List<String> rec;
        long rowNo = 1; // header = 1
        while ((rec = p.nextRecord()) != null) {
            rowNo++;
            consumer.onRecord(rowNo, Collections.unmodifiableList(rec));
        }
    }

    /**
     * 全列または指定列のみ抽出してCSVとして書き出す。
     * - selectedColumns == null の場合は全列
     * - selectedColumns がある場合は「ヘッダー名」で指定した列のみ出力（順番も指定順）
     *
     * @param strictMissingColumn true: 指定列が無ければ例外 / false: 無い列は空欄として出す
     */
    public static void extractCsv(
            Reader in,
            Writer out,
            List<String> selectedColumns,
            boolean strictMissingColumn
    ) throws IOException {
        Objects.requireNonNull(in, "in");
        Objects.requireNonNull(out, "out");

        CsvParser parser = new CsvParser(in);
        CsvWriter writer = new CsvWriter(out);

        List<String> header = parser.nextRecord();
        if (header == null) return;

        ColumnSelection sel = ColumnSelection.build(header, selectedColumns, strictMissingColumn);

        // output header
        writer.writeRecord(sel.outputHeader);

        // output records
        List<String> rec;
        while ((rec = parser.nextRecord()) != null) {
            writer.writeRecord(sel.pick(rec));
        }
        writer.flush();
    }

    /** CSVを「正規化」して書き出す（全列そのまま、ただし正しいクォートで再出力）。 */
    public static void normalizeCsv(Reader in, Writer out) throws IOException {
        extractCsv(in, out, null, true);
    }

    /** ファイルやHTTPなどで便利な UTF-8 デフォルトの Reader を作る。 */
    public static Reader newReader(InputStream in) {
        return new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
    }

    /** ファイルやHTTPなどで便利な Writer を作る（UTF-8）。 */
    public static Writer newWriter(OutputStream out) {
        return new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
    }

    /** 文字コード指定したい場合の Reader */
    public static Reader newReader(InputStream in, Charset cs) {
        return new BufferedReader(new InputStreamReader(in, cs));
    }

    /** 文字コード指定したい場合の Writer */
    public static Writer newWriter(OutputStream out, Charset cs) {
        return new BufferedWriter(new OutputStreamWriter(out, cs));
    }

    // -------------------------
    // Callback interface
    // -------------------------

    public interface CsvRecordConsumer {
        /** ヘッダー行（1行目） */
        void onHeader(List<String> header);

        /**
         * データ行
         * @param rowNo 1-based 行番号（ヘッダー=1、データ開始=2）
         */
        void onRecord(long rowNo, List<String> record);
    }

    // -------------------------
    // Internals: Column selection
    // -------------------------

    private static final class ColumnSelection {
        final List<Integer> keepIndexes;  // indices in input record
        final List<String> outputHeader;  // output header names

        private ColumnSelection(List<Integer> keepIndexes, List<String> outputHeader) {
            this.keepIndexes = keepIndexes;
            this.outputHeader = outputHeader;
        }

        static ColumnSelection build(List<String> header, List<String> selected, boolean strictMissing) {
            if (selected == null) {
                List<Integer> idx = new ArrayList<>();
                for (int i = 0; i < header.size(); i++) idx.add(i);
                return new ColumnSelection(idx, new ArrayList<>(header));
            }

            Map<String, Integer> map = new HashMap<>();
            for (int i = 0; i < header.size(); i++) {
                map.put(header.get(i), i);
            }

            List<Integer> idx = new ArrayList<>(selected.size());
            List<String> outHeader = new ArrayList<>(selected.size());

            for (String col : selected) {
                String c = (col == null) ? "" : col.trim();
                if (c.isEmpty()) continue;

                Integer i = map.get(c);
                if (i == null) {
                    if (strictMissing) {
                        throw new IllegalArgumentException("指定列がヘッダーに存在しません: " + c);
                    } else {
                        // 入力に無い列は -1 として扱い、空欄で出す
                        idx.add(-1);
                        outHeader.add(c);
                        continue;
                    }
                }
                idx.add(i);
                outHeader.add(c);
            }

            return new ColumnSelection(idx, outHeader);
        }

        List<String> pick(List<String> record) {
            List<String> out = new ArrayList<>(keepIndexes.size());
            for (int idx : keepIndexes) {
                if (idx < 0) {
                    out.add("");
                } else {
                    out.add(idx < record.size() ? record.get(idx) : "");
                }
            }
            return out;
        }
    }

    // -------------------------
    // Internals: CSV Parser (RFC4180-ish)
    // -------------------------

    public static final class CsvParser {
        private final Reader r;
        private int pushed = -2; // -2: none, -1: EOF, else: char

        public CsvParser(Reader r) {
            this.r = Objects.requireNonNull(r, "reader");
        }

        /**
         * 次のレコードを返す。EOFなら null。
         * クォート内改行に対応するため、「行」ではなく「レコード」単位で読む。
         */
        public List<String> nextRecord() throws IOException {
            List<String> row = new ArrayList<>();
            StringBuilder cell = new StringBuilder();
            boolean inQuotes = false;
            boolean anyRead = false;

            while (true) {
                int ch = read();
                if (ch == -1) {
                    if (!anyRead) return null;
                    row.add(cell.toString());
                    return row;
                }

                anyRead = true;

                if (inQuotes) {
                    if (ch == '"') {
                        int next = read();
                        if (next == '"') {
                            cell.append('"'); // "" -> "
                        } else {
                            inQuotes = false; // quote end
                            unread(next);
                        }
                    } else {
                        cell.append((char) ch);
                    }
                    continue;
                }

                // outside quotes
                if (ch == '"') {
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
            pushed = c;
        }
    }

    // -------------------------
    // Internals: CSV Writer (safe)
    // -------------------------

    public static final class CsvWriter {
        private final Writer w;

        public CsvWriter(Writer w) {
            this.w = Objects.requireNonNull(w, "writer");
        }

        public void writeRecord(List<String> cols) throws IOException {
            for (int i = 0; i < cols.size(); i++) {
                if (i > 0) w.write(',');
                w.write(escape(cols.get(i)));
            }
            w.write("\n");
        }

        public void flush() throws IOException {
            w.flush();
        }

        /**
         * CSV安全エスケープ:
         * - , " CR LF を含む -> "..." で囲う
         * - " は "" に
         * - 先頭/末尾の空白/タブも囲う（事故防止）
         */
        public static String escape(String s) {
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