import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class RetailStoreJobRunner {

    private final RetailStoreRepository repository;

    public RetailStoreJobRunner(RetailStoreRepository repository) {
        this.repository = repository;
    }

    public void run(InputStream csvStream) throws Exception {

        Properties mapping = new Properties();
        try (InputStream in = getClass().getResourceAsStream("/retailstore-sf-mapping.properties")) {
            if (in == null) {
                throw new IllegalStateException("retailstore-sf-mapping.properties が見つかりません");
            }
            mapping.load(in);
        }

        Map<String, Object> programValues = new HashMap<>();
        programValues.put("batchId", "20260323001");
        programValues.put("importType", "UPSERT");
        programValues.put("systemUser", "BULK_JOB");
        programValues.put("jobId", "750xx0000000001AAA");

        RetailStoreCsvDtoAssembler assembler =
                new RetailStoreCsvDtoAssembler(mapping, programValues);

        RetailStoreSyncExecutor executor =
                new RetailStoreSyncExecutor(repository, assembler);

        executor.execute(csvStream);
    }
}