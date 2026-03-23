import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public abstract class AbstractCsvDtoAssembler<T> {

    private final Map<String, String> mapping = new HashMap<>();
    private final Map<String, Field> fieldCache = new HashMap<>();
    private final Class<T> dtoClass;
    private final Map<String, Object> programValues;

    protected AbstractCsvDtoAssembler(
            Properties props,
            Class<T> dtoClass,
            Map<String, Object> programValues) {

        this.dtoClass = dtoClass;
        this.programValues = programValues == null ? new HashMap<>() : new HashMap<>(programValues);

        for (String key : props.stringPropertyNames()) {
            mapping.put(normalize(key), props.getProperty(key));
        }

        for (Field field : dtoClass.getDeclaredFields()) {
            field.setAccessible(true);
            fieldCache.put(field.getName(), field);
        }
    }

    public T assemble(CsvRecord record) {
        try {
            T dto = dtoClass.getDeclaredConstructor().newInstance();

            applyCsvValues(dto, record);
            applyProgramValues(dto);

            afterAssemble(dto, record);

            return dto;

        } catch (Exception e) {
            throw new RuntimeException("CSVからDTOへの変換に失敗しました", e);
        }
    }

    private void applyCsvValues(T dto, CsvRecord record) throws IllegalAccessException {
        for (Map.Entry<String, String> entry : mapping.entrySet()) {
            String sfFieldName = entry.getKey();
            String dtoFieldName = entry.getValue();

            Field field = fieldCache.get(dtoFieldName);
            if (field == null) {
                continue;
            }

            String rawValue = record.get(sfFieldName);
            Object formatted = formatForDatabase(dtoFieldName, field.getType(), rawValue);
            field.set(dto, formatted);
        }
    }

    private void applyProgramValues(T dto) throws IllegalAccessException {
        for (Map.Entry<String, Object> entry : programValues.entrySet()) {
            String dtoFieldName = entry.getKey();
            Object rawProgramValue = entry.getValue();

            Field field = fieldCache.get(dtoFieldName);
            if (field == null) {
                continue;
            }

            Object formatted = formatProgramValue(dtoFieldName, field.getType(), rawProgramValue);
            field.set(dto, formatted);
        }
    }

    protected void afterAssemble(T dto, CsvRecord record) {
        // 必要なら子クラスで追加処理
    }

    protected Object formatForDatabase(String fieldName, Class<?> type, String rawValue) {
        String value = emptyToNull(rawValue);
        if (value == null) {
            return null;
        }

        if (type == String.class) {
            return normalizeScientificNotation(value);
        }
        if (type == Integer.class || type == int.class) {
            return Integer.valueOf(normalizeScientificNotation(value));
        }
        if (type == Long.class || type == long.class) {
            return Long.valueOf(normalizeScientificNotation(value));
        }
        if (type == Boolean.class || type == boolean.class) {
            return Boolean.valueOf(value);
        }
        if (type == BigDecimal.class) {
            return new BigDecimal(value);
        }
        if (type == Timestamp.class) {
            return Timestamp.from(OffsetDateTime.parse(value).toInstant());
        }

        return normalizeScientificNotation(value);
    }

    protected Object formatProgramValue(String fieldName, Class<?> type, Object rawValue) {
        if (rawValue == null) {
            return null;
        }

        if (type.isInstance(rawValue)) {
            return rawValue;
        }

        String value = String.valueOf(rawValue);

        if (type == String.class) {
            return value;
        }
        if (type == Integer.class || type == int.class) {
            return Integer.valueOf(value);
        }
        if (type == Long.class || type == long.class) {
            return Long.valueOf(value);
        }
        if (type == Boolean.class || type == boolean.class) {
            return Boolean.valueOf(value);
        }
        if (type == BigDecimal.class) {
            return new BigDecimal(value);
        }
        if (type == Timestamp.class) {
            return Timestamp.from(OffsetDateTime.parse(value).toInstant());
        }

        return value;
    }

    protected String emptyToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    protected String normalizeScientificNotation(String value) {
        if (value == null) {
            return null;
        }

        String trimmed = value.trim();

        try {
            if (trimmed.contains("E") || trimmed.contains("e")) {
                return new BigDecimal(trimmed).toPlainString();
            }
        } catch (NumberFormatException e) {
            // 数値でないならそのまま返す
        }

        return trimmed;
    }

    protected String normalize(String value) {
        return value.replace("\uFEFF", "").trim().toLowerCase();
    }
}