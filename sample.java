try (SqlSession session = sqlSessionFactory.openSession();
     Cursor<MyDto> cursor = session.getMapper(MyMapper.class).selectForCsvCursor()) {

    ReflectiveCsvWriter.forClass(MyDto.class)
            .writeUtf8(Path.of("out.csv"), cursor);
}




private static final ObjectMapper MAPPER = new ObjectMapper();

static {
    MAPPER.findAndRegisterModules();
    MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
}

private <T> T parseJson(String json, TypeReference<T> typeRef) {
    try {
        return MAPPER.readValue(json, typeRef);
    } catch (JsonProcessingException e) {
        throw new RuntimeException("JSON parse error", e);
    }
}