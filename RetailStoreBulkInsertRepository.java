import java.util.List;

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