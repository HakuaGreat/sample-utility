public static class RetailStoreCsvDtoAssembler implements DtoAssembler<RetailStoreDto> {

    private final Map<String, String> mapping = new HashMap<>();
    private final Map<String, Field> fieldCache = new HashMap<>();

    public RetailStoreCsvDtoAssembler(Properties props) {
        for (String key : props.stringPropertyNames()) {
            mapping.put(normalize(key), props.getProperty(key));
        }

        for (Field field : RetailStoreDto.class.getDeclaredFields()) {
            field.setAccessible(true);
            fieldCache.put(field.getName(), field);
        }
    }

    @Override
    public RetailStoreDto assemble(CsvRecord record) {
        try {
            RetailStoreDto dto = new RetailStoreDto();

            for (Map.Entry<String, String> entry : mapping.entrySet()) {
                String sfFieldName = entry.getKey();
                String dtoFieldName = entry.getValue();

                String rawValue = record.get(sfFieldName);
                Field field = fieldCache.get(dtoFieldName);

                System.out.println("[DEBUG] sfFieldName=" + sfFieldName
                        + ", dtoFieldName=" + dtoFieldName
                        + ", rawValue=" + rawValue);

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

    private String normalize(String s) {
        if (s == null) {
            return null;
        }
        return s.trim().toLowerCase();
    }
}