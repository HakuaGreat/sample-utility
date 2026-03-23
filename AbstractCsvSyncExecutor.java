import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractCsvSyncExecutor<T> {

    private static final int CHUNK_SIZE = 1000;

    private final BulkInsertRepository<T> repository;
    private final AbstractCsvDtoAssembler<T> assembler;

    protected AbstractCsvSyncExecutor(
            BulkInsertRepository<T> repository,
            AbstractCsvDtoAssembler<T> assembler) {
        this.repository = repository;
        this.assembler = assembler;
    }

    public void execute(InputStream csvStream) throws Exception {
        try (CsvResultReader reader =
                     new CsvResultReader(new InputStreamReader(csvStream, StandardCharsets.UTF_8))) {

            reader.open();

            List<T> buffer = new ArrayList<>(CHUNK_SIZE);
            CsvRecord record;

            while ((record = reader.readRecord()) != null) {
                T dto = assembler.assemble(record);
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