public class RetailStoreSyncExecutor
        extends AbstractCsvSyncExecutor<RetailStoreDto> {

    public RetailStoreSyncExecutor(
            RetailStoreRepository repository,
            RetailStoreCsvDtoAssembler assembler) {
        super(new RetailStoreBulkInsertRepository(repository), assembler);
    }
}