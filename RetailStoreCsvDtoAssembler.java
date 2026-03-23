import java.util.Map;
import java.util.Properties;

public class RetailStoreCsvDtoAssembler
        extends AbstractCsvDtoAssembler<RetailStoreDto> {

    public RetailStoreCsvDtoAssembler(
            Properties props,
            Map<String, Object> programValues) {
        super(props, RetailStoreDto.class, programValues);
    }

    @Override
    protected void afterAssemble(RetailStoreDto dto, CsvRecord record) {
        // 必要ならここにRetailStore専用の補正処理を書く
    }
}