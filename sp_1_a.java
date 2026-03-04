import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * RFC4180寄りのCSVパーサ（Java11 / 標準のみ）
 * - カンマ区切り
 * - "..." クォート対応
 * - セル内の "" は " に復元
 * - クォート内改行を許容（Bulk結果CSVでも安全）
 */
public final class CsvToolkit {

    private CsvToolkit() {}

    public static final class CsvParser {
        private final Reader r;
        private int pushed = -2; // -2: none, else: char or -1

        public CsvParser(Reader r) {
            this.r = Objects.requireNonNull(r, "reader");
        }

        /** 次のレコードを返す。EOFなら null。 */
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
}