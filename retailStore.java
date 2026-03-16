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