public class RetailStoreCsvDtoAssembler
        extends AbstractCsvDtoAssembler<RetailStoreDto> {

    private final String batchId;

    public RetailStoreCsvDtoAssembler(Properties props, String batchId) {
        super(props, RetailStoreDto.class);
        this.batchId = batchId;
    }

    @Override
    protected void applyProgramValues(RetailStoreDto dto, CsvRecord record) {
        dto.setBatchId(batchId);
        dto.setDataSource("SF_BULK");
    }
}

----------------------------

public class RetailStoreBulkInsertRepository
        implements BulkInsertRepository<RetailStoreDto> {

    private final RetailStoreRepository repository;

    public RetailStoreBulkInsertRepository(RetailStoreRepository repository) {
        this.repository = repository;
    }

    @Override
    public void insert(List<RetailStoreDto> list) {
        repository.insert(list);
    }
}

----------------------------

public class RetailStoreSyncExecutor
        extends AbstractCsvSyncExecutor<RetailStoreDto> {

    private final String batchId;

    public RetailStoreSyncExecutor(RetailStoreRepository repository, String batchId) {
        super(new RetailStoreBulkInsertRepository(repository));
        this.batchId = batchId;
    }

    @Override
    protected Properties loadMapping() throws Exception {
        Properties props = new Properties();
        try (InputStream in = getClass().getResourceAsStream("/retailstore-sf-mapping.properties")) {
            if (in == null) {
                throw new IllegalStateException("mapping file が見つかりません");
            }
            props.load(in);
        }
        return props;
    }

    @Override
    protected DtoAssembler<RetailStoreDto> createAssembler(Properties mapping) {
        return new RetailStoreCsvDtoAssembler(mapping, batchId);
    }
}

----------------------------

public void execute(InputStream csvStream) throws Exception {

    try (CsvResultReader reader =
            new CsvResultReader(new InputStreamReader(csvStream, StandardCharsets.UTF_8))) {

        reader.open();

        List<RetailStoreDto> buffer = new ArrayList<>(1000);

        CsvRecord record;

        while ((record = reader.readRecord()) != null) {

            RetailStoreDto dto = assembler.assemble(record);

            buffer.add(dto);

            if (buffer.size() >= 1000) {
                repository.insert(buffer);
                buffer.clear();
            }
        }

        if (!buffer.isEmpty()) {
            repository.insert(buffer);
        }
    }
}