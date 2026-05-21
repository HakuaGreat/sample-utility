public class CsvEscapeUtil {

    private CsvEscapeUtil() {
    }

    public static String escape(String value) {

        if (value == null) {
            return "";
        }

        boolean needQuote =
                value.contains(",")
             || value.contains("\"")
             || value.contains("\n")
             || value.contains("\r");

        String escaped =
                value.replace("\"", "\"\"");

        if (needQuote) {
            return "\"" + escaped + "\"";
        }

        return escaped;
    }
}


----------------------------

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class CsvWriterUtil {

    private CsvWriterUtil() {
    }

    public static BufferedWriter create(Path path)
            throws IOException {

        return Files.newBufferedWriter(path);
    }

    public static void writeLine(
            BufferedWriter bw,
            List<String> columns)
            throws IOException {

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < columns.size(); i++) {

            if (i > 0) {
                sb.append(",");
            }

            sb.append(
                CsvEscapeUtil.escape(columns.get(i))
            );
        }

        bw.write(sb.toString());
        bw.newLine();
    }
}

----------------------------

import java.util.ArrayList;
import java.util.List;

public class CsvParserUtil {

    private CsvParserUtil() {
    }

    public static List<String> parseCsv(
            String record) {

        List<String> result =
                new ArrayList<>();

        StringBuilder sb =
                new StringBuilder();

        boolean inQuotes = false;

        for (int i = 0;
             i < record.length();
             i++) {

            char c = record.charAt(i);

            if (c == '"') {

                if (inQuotes
                        && i + 1 < record.length()
                        && record.charAt(i + 1) == '"') {

                    sb.append('"');
                    i++;

                } else {

                    inQuotes = !inQuotes;
                }

            } else if (c == ','
                    && !inQuotes) {

                result.add(sb.toString());
                sb.setLength(0);

            } else {

                sb.append(c);
            }
        }

        result.add(sb.toString());

        return result;
    }
}

----------------------------

import java.io.BufferedReader;
import java.io.IOException;

public class CsvRecordReader {

    private CsvRecordReader() {
    }

    public static String readRecord(
            BufferedReader br)
            throws IOException {

        StringBuilder sb =
                new StringBuilder();

        String line;

        boolean inQuotes = false;

        while ((line = br.readLine()) != null) {

            if (sb.length() > 0) {
                sb.append("\n");
            }

            sb.append(line);

            for (int i = 0;
                 i < line.length();
                 i++) {

                if (line.charAt(i) == '"') {

                    if (i + 1 < line.length()
                            && line.charAt(i + 1)
                               == '"') {

                        i++;

                    } else {

                        inQuotes = !inQuotes;
                    }
                }
            }

            if (!inQuotes) {
                break;
            }
        }

        if (sb.length() == 0) {
            return null;
        }

        return sb.toString();
    }
}

----------------------------

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class MappingLoader {

    private MappingLoader() {
    }

    public static Properties load(
            String fileName)
            throws IOException {

        Properties props =
                new Properties();

        try (InputStream is =
                     MappingLoader.class
                        .getClassLoader()
                        .getResourceAsStream(
                            fileName)) {

            if (is == null) {

                throw new IOException(
                    "mapping file not found"
                );
            }

            props.load(is);
        }

        return props;
    }
}

----------------------------

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class OracleCsvConverter {

    private OracleCsvConverter() {
    }

    public static void convert(
            Path inputCsv,
            Path outputCsv,
            Properties mapping)
            throws IOException {

        try (
            BufferedReader br =
                Files.newBufferedReader(inputCsv);

            BufferedWriter bw =
                CsvWriterUtil.create(outputCsv)
        ) {

            // Salesforceヘッダ
            String headerRecord =
                    CsvRecordReader
                        .readRecord(br);

            if (headerRecord == null) {

                throw new IOException(
                    "CSV header not found"
                );
            }

            List<String> sfHeaders =
                    CsvParserUtil
                        .parseCsv(headerRecord);

            List<String> dbHeaders =
                    new ArrayList<>();

            for (String sfHeader
                    : sfHeaders) {

                String dbColumn =
                        mapping.getProperty(
                            sfHeader);

                if (dbColumn != null) {

                    dbHeaders.add(dbColumn);

                } else {

                    dbHeaders.add(sfHeader);
                }
            }

            // DBヘッダ出力
            CsvWriterUtil.writeLine(
                bw,
                dbHeaders
            );

            // データ行
            String record;

            while ((record =
                    CsvRecordReader
                        .readRecord(br))
                    != null) {

                List<String> columns =
                        CsvParserUtil
                            .parseCsv(record);

                CsvWriterUtil.writeLine(
                    bw,
                    columns
                );
            }
        }
    }
}

----------------------------

import java.io.IOException;

public class SqlLoaderExecutor {

    private SqlLoaderExecutor() {
    }

    public static void execute(
            String user,
            String password,
            String connect,
            String controlFile)
            throws IOException,
                   InterruptedException {

        ProcessBuilder pb =
            new ProcessBuilder(
                "sqlldr",
                user + "/" + password
                    + "@"
                    + connect,
                "control=" + controlFile,
                "log=load.log",
                "bad=load.bad",
                "DIRECT=TRUE"
            );

        pb.inheritIO();

        Process process = pb.start();

        int exitCode =
                process.waitFor();

        if (exitCode != 0) {

            throw new RuntimeException(
                "SQLLoader failed code="
                    + exitCode
            );
        }
    }
}

----------------------------

Properties mapping =
    MappingLoader.load(
        "mapping.properties"
    );

OracleCsvConverter.convert(
    Path.of("./sf.csv"),
    Path.of("./oracle_load.csv"),
    mapping
);

SqlLoaderExecutor.execute(
    "USER",
    "PASSWORD",
    "ORCL",
    "./load.ctl"
);