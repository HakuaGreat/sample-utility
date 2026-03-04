import javax.sql.DataSource;
import java.sql.Connection;

public class Runner {

    public static void run(org.springframework.context.ApplicationContext ctx, File successCsv) throws Exception {
        DataSource ds = ctx.getBean("dataSource", DataSource.class);

        try (Connection conn = ds.getConnection()) {
            long updated = BulkSuccessCsvToDbUpdaterNoCommit.updateSfIdsFromSuccessCsvNoCommit(
                    conn,
                    successCsv,
                    java.nio.charset.StandardCharsets.UTF_8,
                    "ExternalId__c",
                    "UPDATE MY_TABLE SET SF_ID = ? WHERE EXT_ID = ?",
                    1000
            );
            conn.commit();
            System.out.println("updated=" + updated);
        }
    }
}