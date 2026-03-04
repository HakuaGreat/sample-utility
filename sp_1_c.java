import org.springframework.context.ApplicationContext;

import javax.sql.DataSource;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;

/**
 * Spring XMLの DataSource(bean id="Datasource") を使って、
 * 成功結果CSVから sf__Id を抽出しOracleへ反映する。
 */
public final class SfIdApplyRunner {

    private SfIdApplyRunner() {}

    /**
     * @param ctx        既存のSpring ApplicationContext
     * @param successCsv 保存済みの成功結果CSVファイル
     */
    public static void apply(ApplicationContext ctx, File successCsv) throws Exception {

        // ★XMLのbean idと完全一致させる（大文字小文字も一致）
        DataSource ds = ctx.getBean("Datasource", DataSource.class);

        // ★あなたの成功結果CSVに入れてる突合キー列名（SF側の列名）
        String keyColInCsv = "ExternalId__c"; // ←必要に応じて DbId__c 等に変更

        // ★DB側のカラム名はSQLで書く（CSV列名と一致不要）
        String updateSql = "UPDATE MY_TABLE SET SF_ID = ? WHERE EXT_ID = ?";

        int batchSize = 1000;

        try (Connection conn = ds.getConnection()) {
            boolean prev = conn.getAutoCommit();
            conn.setAutoCommit(false);

            try {
                long updated = BulkSuccessCsvToDbUpdaterNoCommit.updateSfIdsFromSuccessCsvNoCommit(
                        conn,
                        successCsv,
                        StandardCharsets.UTF_8,
                        keyColInCsv,
                        updateSql,
                        batchSize
                );

                conn.commit();
                // ログはお好みで
                System.out.println("updated(sum of executeBatch positive counts)=" + updated);

            } catch (Exception e) {
                try { conn.rollback(); } catch (Exception ignore) {}
                throw e;
            } finally {
                try { conn.setAutoCommit(prev); } catch (Exception ignore) {}
            }
        }
    }
}