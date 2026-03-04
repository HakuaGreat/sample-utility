import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class BulkSuccessCsvToDbUpdater {

    private BulkSuccessCsvToDbUpdater() {}

    /**
     * 成功結果CSV（保存済みファイル）から sf__Id を抜いてDBへバッチ更新する（高速・省メモリ）。
     *
     * @param conn                 JDBC Connection（呼び出し側で用意）
     * @param successCsvFile       保存済みの成功結果CSVファイル
     * @param csvCharset           CSVの文字コード（通常UTF-8）
     * @param externalKeyColName   CSV内の突合キー列名（例: "ExternalId__c"）
     * @param updateSql            更新SQL（例: "UPDATE t SET sf_id=? WHERE external_id=?"）
     * @param batchSize            バッチサイズ（例: 1000）
     * @param commitEveryBatches   何バッチごとにcommitするか（例: 10 → 1000*10=1万件ごと）
     *
     * @return 更新処理サマリ
     */
    public static Summary updateSfIdsFromSuccessCsv(
            Connection conn,
            File successCsvFile,
            Charset csvCharset,
            String externalKeyColName,
            String updateSql,
            int batchSize,
            int commitEveryBatches
    ) throws Exception {

        Objects.requireNonNull(conn, "conn");
        Objects.requireNonNull(successCsvFile, "successCsvFile");
        Objects.requireNonNull(csvCharset, "csvCharset");
        Objects.requireNonNull(externalKeyColName, "externalKeyColName");
        Objects.requireNonNull(updateSql, "updateSql");

        if (batchSize <= 0) throw new IllegalArgumentException("batchSize must be > 0");
        if (commitEveryBatches <= 0) throw new IllegalArgumentException("commitEveryBatches must be > 0");

        boolean prevAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);

        long rowsRead = 0;
        long rowsApplied = 0;
        long rowsSkipped = 0;
        long batchesExecuted = 0;

        try (Reader reader = new BufferedReader(new InputStreamReader(new FileInputStream(successCsvFile), csvCharset));
             PreparedStatement ps = conn.prepareStatement(updateSql)) {

            CsvToolkit.CsvParser parser = new CsvToolkit.CsvParser(reader);

            // ヘッダー
            List<String> header = parser.nextRecord();
            if (header == null) return new Summary(0, 0, 0, 0);

            Map<String, Integer> idx = indexHeader(header);

            Integer sfIdIdx = idx.get("sf__Id");
            Integer extIdx = idx.get(externalKeyColName);

            if (sfIdIdx == null) {
                throw new IllegalArgumentException("成功結果CSVに sf__Id がありません。ヘッダー=" + header);
            }
            if (extIdx == null) {
                throw new IllegalArgumentException("成功結果CSVにキー列がありません: " + externalKeyColName + " ヘッダー=" + header);
            }

            int batchCount = 0;
            int batchesSinceCommit = 0;

            List<String> rec;
            while ((rec = parser.nextRecord()) != null) {
                rowsRead++;

                String sfId = get(rec, sfIdIdx);
                String externalKey = get(rec, extIdx);

                if (isBlank(sfId) || isBlank(externalKey)) {
                    rowsSkipped++;
                    continue;
                }

                // ★ SQLのプレースホルダ順：sf_id -> external_key の順を想定
                ps.setString(1, sfId);
                ps.setString(2, externalKey);
                ps.addBatch();

                batchCount++;
                if (batchCount >= batchSize) {
                    int affected = sum(ps.executeBatch());
                    rowsApplied += affected;
                    batchesExecuted++;
                    batchesSinceCommit++;

                    batchCount = 0;

                    if (batchesSinceCommit >= commitEveryBatches) {
                        conn.commit();
                        batchesSinceCommit = 0;
                    }
                }
            }

            // 残り flush
            if (batchCount > 0) {
                int affected = sum(ps.executeBatch());
                rowsApplied += affected;
                batchesExecuted++;
                batchesSinceCommit++;
            }

            conn.commit();

            return new Summary(rowsRead, rowsApplied, rowsSkipped, batchesExecuted);

        } catch (Exception e) {
            try { conn.rollback(); } catch (Exception ignore) {}
            throw e;
        } finally {
            try { conn.setAutoCommit(prevAutoCommit); } catch (Exception ignore) {}
        }
    }

    private static Map<String, Integer> indexHeader(List<String> header) {
        Map<String, Integer> idx = new HashMap<>();
        for (int i = 0; i < header.size(); i++) {
            idx.put(header.get(i), i);
        }
        return idx;
    }

    private static String get(List<String> rec, int i) {
        if (i < 0 || i >= rec.size()) return "";
        return rec.get(i);
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    private static int sum(int[] batchResult) {
        int total = 0;
        if (batchResult == null) return 0;
        for (int r : batchResult) {
            // JDBCは SUCCESS_NO_INFO(-2) を返すことがある。ここは「0扱い」ではなく「1扱い」にするか悩みどころ。
            // 更新件数が重要ならDB側で確認が必要。今回はざっくり合計。
            if (r > 0) total += r;
        }
        return total;
    }

    public static final class Summary {
        public final long rowsRead;
        public final long rowsApplied;
        public final long rowsSkipped;
        public final long batchesExecuted;

        public Summary(long rowsRead, long rowsApplied, long rowsSkipped, long batchesExecuted) {
            this.rowsRead = rowsRead;
            this.rowsApplied = rowsApplied;
            this.rowsSkipped = rowsSkipped;
            this.batchesExecuted = batchesExecuted;
        }

        @Override
        public String toString() {
            return "Summary{rowsRead=" + rowsRead +
                    ", rowsApplied=" + rowsApplied +
                    ", rowsSkipped=" + rowsSkipped +
                    ", batchesExecuted=" + batchesExecuted + "}";
        }
    }
}


------------------------------


Connection conn = dataSource.getConnection();

File csv = new File("/path/to/success.csv");
String keyCol = "ExternalId__c"; // ←DBと突合する列名
String sql = "UPDATE your_table SET sf_id = ? WHERE external_id = ?";

BulkSuccessCsvToDbUpdater.Summary summary =
        BulkSuccessCsvToDbUpdater.updateSfIdsFromSuccessCsv(
                conn,
                csv,
                StandardCharsets.UTF_8,
                keyCol,
                sql,
                1000, // batchSize
                10    // commitEveryBatches（= 1万件ごとにcommit）
        );

System.out.println(summary);
conn.close();