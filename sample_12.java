Properties mapper = new Properties();

try (InputStream in =
     getClass().getResourceAsStream("/sf-mapping.properties")) {

    mapper.load(in);
}




----------------------------

public <T> T mapRow(
        Map<String,String> row,
        Class<T> dtoClass,
        Properties mapper) {

    try {

        T dto = dtoClass.getDeclaredConstructor().newInstance();

        for (Map.Entry<String,String> e : row.entrySet()) {

            String sfField = e.getKey();
            String value = e.getValue();

            String dtoField = mapper.getProperty(sfField);

            if (dtoField == null) {
                continue;
            }

            Field field = dtoClass.getDeclaredField(dtoField);
            field.setAccessible(true);

            Object converted = convert(field.getType(), value);

            field.set(dto, converted);
        }

        return dto;

    } catch (Exception ex) {
        throw new RuntimeException(ex);
    }
}


----------------------------



private Object convert(Class<?> type, String value) {

    if (value == null || value.isEmpty()) {
        return null;
    }

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

    if (type == Timestamp.class) {
        return Timestamp.from(
            java.time.OffsetDateTime.parse(value).toInstant()
        );
    }

    return value;
}





----------------------------






while ((line = br.readLine()) != null) {

    List<String> values = parseCsvLine(line);

    Map<String,String> row = toRowMap(headers, values);

    RetailStoreDto dto =
        mapRow(row, RetailStoreDto.class, mapper);

    repository.merge(dto);
}