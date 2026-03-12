import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class BulkQueryResultBridgeService {

    private static final int CHUNK_SIZE = 1000;

    private final RetailStoreRepository repository;
    private final DtoAssembler<RetailStoreDto> assembler;

    public BulkQueryResultBridgeService(
            RetailStoreRepository repository,
            DtoAssembler<RetailStoreDto> assembler) {
        this.repository = repository;
        this.assembler = assembler;
    }

    public void execute(InputStream csvStream) throws Exception {
        try (CsvResultReader reader = new CsvResultReader(
                new InputStreamReader(csvStream, StandardCharsets.UTF_8))) {

            reader.open();

            List<RetailStoreDto> buffer = new ArrayList<>(CHUNK_SIZE);

            CsvRecord record;
            while ((record = reader.readRecord()) != null) {
                RetailStoreDto dto = assembler.assemble(record);
                buffer.add(dto);

                if (buffer.size() >= CHUNK_SIZE) {
                    repository.insert(buffer);
                    buffer = new ArrayList<>(CHUNK_SIZE);
                }
            }

            if (!buffer.isEmpty()) {
                repository.insert(buffer);
            }
        }
    }
}