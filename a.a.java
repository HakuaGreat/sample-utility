public static List<String> readCsvRecord(BufferedReader br)
        throws IOException {

    String line;

    StringBuilder record = new StringBuilder();

    boolean inQuotes = false;

    while ((line = br.readLine()) != null) {

        if (record.length() > 0) {
            record.append("\n");
        }

        record.append(line);

        int quoteCount = 0;

        for (int i = 0; i < line.length(); i++) {

            if (line.charAt(i) == '"') {

                // "" は除外
                if (i + 1 < line.length()
                        && line.charAt(i + 1) == '"') {

                    i++;

                } else {

                    quoteCount++;
                }
            }
        }

        if (quoteCount % 2 != 0) {
            inQuotes = !inQuotes;
        }

        if (!inQuotes) {
            break;
        }
    }

    if (record.length() == 0) {
        return null;
    }

    return parseCsv(record.toString());
}


----------------------------

public static List<String> parseCsv(String record) {

    List<String> result = new ArrayList<>();

    StringBuilder sb = new StringBuilder();

    boolean inQuotes = false;

    for (int i = 0; i < record.length(); i++) {

        char c = record.charAt(i);

        if (c == '"') {

            // ""
            if (inQuotes
                    && i + 1 < record.length()
                    && record.charAt(i + 1) == '"') {

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


----------------------------

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CsvParser {

    public static List<List<String>> parse(BufferedReader br)
            throws IOException {

        List<List<String>> records = new ArrayList<>();

        List<String> row = new ArrayList<>();

        StringBuilder field = new StringBuilder();

        boolean inQuotes = false;

        int ch;

        while ((ch = br.read()) != -1) {

            char c = (char) ch;

            // ダブルクォート
            if (c == '"') {

                // クォート内の ""
                if (inQuotes) {

                    br.mark(1);

                    int next = br.read();

                    if (next == '"') {

                        field.append('"');

                    } else {

                        inQuotes = false;

                        br.reset();
                    }

                } else {

                    inQuotes = true;
                }

                continue;
            }

            // カンマ区切り
            if (c == ',' && !inQuotes) {

                row.add(field.toString());

                field.setLength(0);

                continue;
            }

            // 改行
            if ((c == '\n' || c == '\r') && !inQuotes) {

                // CRLF対応
                if (c == '\r') {

                    br.mark(1);

                    int next = br.read();

                    if (next != '\n') {
                        br.reset();
                    }
                }

                row.add(field.toString());

                field.setLength(0);

                // 空行対策
                if (!row.isEmpty()) {

                    records.add(new ArrayList<>(row));

                    row.clear();
                }

                continue;
            }

            // クォート内改行は普通に文字として入る
            field.append(c);
        }

        // EOF残処理
        if (field.length() > 0 || !row.isEmpty()) {

            row.add(field.toString());

            records.add(row);
        }

        return records;
    }
}

----------------------------

public static List<String> parseCsvRecord(BufferedReader br)
        throws IOException {

    String line;

    StringBuilder record = new StringBuilder();

    boolean inQuotes = false;

    while ((line = br.readLine()) != null) {

        // 既存行があるなら改行復元
        if (record.length() > 0) {
            record.append("\n");
        }

        record.append(line);

        // クォート状態確認
        for (int i = 0; i < line.length(); i++) {

            char c = line.charAt(i);

            if (c == '"') {

                // ""
                if (i + 1 < line.length()
                        && line.charAt(i + 1) == '"') {

                    i++;

                } else {

                    inQuotes = !inQuotes;
                }
            }
        }

        // クォート閉じたなら1レコード完成
        if (!inQuotes) {
            break;
        }
    }

    return parseCsv(record.toString());
}