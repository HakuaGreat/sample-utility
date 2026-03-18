public class SalesforceResultProcessor implements DtoProcessor<SalesforceResultDto> {

    private final RetailStoreRepository repository;

    public SalesforceResultProcessor(RetailStoreRepository repository) {
        this.repository = repository;
    }

    @Override
    public void process(SalesforceResultDto dto) {
        repository.updateSfidByLinkKey(dto);
    }
}