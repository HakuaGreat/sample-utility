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