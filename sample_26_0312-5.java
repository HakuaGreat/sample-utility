import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class RetailStoreCsvDtoAssembler implements DtoAssembler<RetailStoreDto> {

    private final Properties mapping;
    private final Map<String, Field> fieldCache = new HashMap<>();

    public RetailStoreCsvDtoAssembler(Properties mapping) {
        this.mapping = mapping;

        for (Field field : RetailStoreDto.class.getDeclaredFields()) {
            field.setAccessible(true);
            fieldCache.put(field.getName(), field);
        }
    }

    @Override
    public RetailStoreDto assemble(CsvRecord record) {
        try {
            RetailStoreDto dto = new RetailStoreDto();

            for (String sfFieldName : mapping.stringPropertyNames()) {
                String dtoFieldName = mapping.getProperty(sfFieldName);

                String rawValue = record.get(sfFieldName);
                Field field = fieldCache.get(dtoFieldName);

                if (field == null) {
                    continue;
                }

                Object converted = convert(field.getType(), rawValue);
                field.set(dto, converted);
            }

            return dto;

        } catch (Exception e) {
            throw new RuntimeException("CSVからDTOへの変換に失敗しました", e);
        }
    }

    private Object convert(Class<?> type, String value) {
        String v = emptyToNull(value);
        if (v == null) {
            return null;
        }

        if (type == String.class) {
            return v;
        }

        if (type == Integer.class || type == int.class) {
            return Integer.valueOf(v);
        }

        if (type == Long.class || type == long.class) {
            return Long.valueOf(v);
        }

        if (type == Boolean.class || type == boolean.class) {
            return Boolean.valueOf(v);
        }

        if (type == Timestamp.class) {
            return Timestamp.from(OffsetDateTime.parse(v).toInstant());
        }

        return v;
    }

    private String emptyToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}