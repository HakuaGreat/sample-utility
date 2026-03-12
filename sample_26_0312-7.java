import java.io.InputStream;
import java.util.Properties;

public class RetailStoreSyncExecutor {

    private final RetailStoreRepository repository;

    public RetailStoreSyncExecutor(RetailStoreRepository repository) {
        this.repository = repository;
    }

    public void execute(InputStream csvStream) throws Exception {
        Properties mapping = new Properties();

        try (InputStream in = getClass().getResourceAsStream("/sf-mapping.properties")) {
            if (in == null) {
                throw new IllegalStateException("sf-mapping.properties が見つかりません");
            }
            mapping.load(in);
        }

        RetailStoreCsvDtoAssembler assembler = new RetailStoreCsvDtoAssembler(mapping);

        BulkQueryResultBridgeService service =
                new BulkQueryResultBridgeService(repository, assembler);

        service.execute(csvStream);
    }
}