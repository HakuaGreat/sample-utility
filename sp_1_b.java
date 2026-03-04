import java.io.*;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 成功結果CSV（保存済み）から sf__Id を抽出し、DBへバッチ更新する（commitしない版）。
 * - 他の処理（Spring/MyBatis等）にトランザクション管理を寄せたい時に使う。
 */
public final class BulkSuccessCsvToDbUpdaterNoCommit {

    private BulkSuccessCsvToDbUpdaterNoCommit() {}

    /**
     * @param conn              既存DataSourceから取ったConnection（Oracle）
     * @param successCsvFile    成功結果CSV（保存済み）
     * @param csvCharset        文字コード（通常UTF-8）
     * @param keyColumnInCsv    突合キー列名（例: ExternalId__c / DbId__c）
     * @param updateSql         UPDATE文（例: UPDATE T SET SF_ID=? WHERE EXT_ID=?）
     * @param batchSize         例: 1000
     * @return executeBatch()の返却値合計（SUCCESS_NO_INFOは加算されない）
     */
    public static long updateSfIdsFromSuccessCsvNoCommit(
            Connection conn,
            File successCsvFile,
            Charset csvCharset,
            String keyColumnInCsv,
            String updateSql,
            int batchSize
    ) throws Exception {

        try (Reader reader = new BufferedReader(new InputStreamReader(new FileInputStream(successCsvFile), csvCharset));
             PreparedStatement ps = conn.prepareStatement(updateSql)) {

            CsvToolkit.CsvParser parser = new CsvToolkit.CsvParser(reader);

            List<String> header = parser.nextRecord();
            if (header == null) return 0;

            Map<String, Integer> idx = new HashMap<>();
            for (int i = 0; i < header.size(); i++) {
                idx.put(header.get(i), i);
            }

            Integer sfIdIdx = idx.get("sf__Id");
            Integer keyIdx = idx.get(keyColumnInCsv);

            if (sfIdIdx == null) {
                throw new IllegalArgumentException("成功結果CSVに sf__Id 列がありません。ヘッダー=" + header);
            }
            if (keyIdx == null) {
                throw new IllegalArgumentException("成功結果CSVにキー列がありません: " + keyColumnInCsv + " ヘッダー=" + header);
            }

            long applied = 0;
            int inBatch = 0;

            List<String> rec;
            while ((rec = parser.nextRecord()) != null) {
                String sfId = get(rec, sfIdIdx);
                String key = get(rec, keyIdx);

                if (isBlank(sfId) || isBlank(key)) continue;

                // プレースホルダ順：sf_id -> key
                ps.setString(1, sfId);
                ps.setString(2, key);
                ps.addBatch();

                inBatch++;
                if (inBatch >= batchSize) {
                    applied += sumPositive(ps.executeBatch());
                    inBatch = 0;
                }
            }

            if (inBatch > 0) {
                applied += sumPositive(ps.executeBatch());
            }

            return applied;
        }
    }

    private static String get(List<String> rec, int i) {
        return (i >= 0 && i < rec.size()) ? rec.get(i) : "";
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    private static int sumPositive(int[] rs) {
        int total = 0;
        if (rs == null) return 0;
        for (int r : rs) {
            if (r > 0) total += r;
        }
        return total;
    }
}