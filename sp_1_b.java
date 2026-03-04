import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class ApplySfIdFromSuccessCsv {

    private ApplySfIdFromSuccessCsv() {}

    public static void apply(
            SqlSessionFactory factory,
            File successCsv,
            String keyColName // 例: "ExternalId__c" / "DbId__c"
    ) throws Exception {

        int batchSize = 1000;

        try (SqlSession session = factory.openSession(ExecutorType.BATCH, false);
             Reader r = new BufferedReader(new InputStreamReader(new FileInputStream(successCsv), StandardCharsets.UTF_8))) {

            // ★ここはあなたの既存Repository/Mapperに置換
            YourRepo repo = session.getMapper(YourRepo.class);

            CsvToolkit.CsvParser parser = new CsvToolkit.CsvParser(r);

            List<String> header = parser.nextRecord();
            if (header == null) return;

            Map<String, Integer> idx = new HashMap<>();
            for (int i = 0; i < header.size(); i++) idx.put(header.get(i), i);

            Integer sfIdIdx = idx.get("sf__Id");
            Integer keyIdx  = idx.get(keyColName);

            if (sfIdIdx == null) throw new IllegalArgumentException("sf__Id が無い: " + header);
            if (keyIdx  == null) throw new IllegalArgumentException(keyColName + " が無い: " + header);

            int inBatch = 0;
            List<String> rec;

            while ((rec = parser.nextRecord()) != null) {
                String sfId = (sfIdIdx < rec.size()) ? rec.get(sfIdIdx) : "";
                String key  = (keyIdx  < rec.size()) ? rec.get(keyIdx)  : "";

                if (sfId == null || sfId.trim().isEmpty()) continue;
                if (key  == null || key.trim().isEmpty())  continue;

                // ★既存SQLXML呼び出し（例）
                repo.updateSfIdByExtId(key, sfId);

                inBatch++;
                if (inBatch >= batchSize) {
                    session.flushStatements();
                    session.commit();
                    inBatch = 0;
                }
            }

            if (inBatch > 0) {
                session.flushStatements();
                session.commit();
            }
        }
    }
}