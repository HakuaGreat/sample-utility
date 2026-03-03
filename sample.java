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





try (Cursor<MyDto> cursor = mapper.selectCursor();
     InputStream mapIn = getClass().getResourceAsStream("/Mapper.properties");
     OutputStream out = new FileOutputStream("retailstore.csv")) {

    SfBulkCsvExporter.export(
        MyDto.class,
        cursor,
        out,
        mapIn,
        false
    );
}






InputStream mapIn =
    Thread.currentThread()
          .getContextClassLoader()
          .getResourceAsStream("mapping.properties");

if (mapIn == null) {
    throw new IllegalStateException("mapping.properties がクラスパスに見つかりません");
}




String sfName = (e.getKey() == null) ? "" : e.getKey().trim();
String spec   = (e.getValue() == null) ? "" : e.getValue().trim();

if (sfName.isEmpty()) {
    System.err.println("[INFO] Skip column (empty SF column name in mapping.properties)");
    continue;
}
if (spec.isEmpty()) {
    System.err.println("[INFO] Skip column (empty mapping spec) : " + sfName);
    continue;
}

MappingSpec ms = MappingSpec.parse(spec);
if (ms.dtoFieldName == null || ms.dtoFieldName.trim().isEmpty()) {
    System.err.println("[INFO] Skip column (empty DTO field name) : " + sfName + " -> " + spec);
    continue;
}

Field f = dtoFields.get(ms.dtoFieldName);
if (f == null) {
    System.err.println("[INFO] Skip column (DTO field not found): "
            + sfName + " -> " + ms.dtoFieldName);
    continue;
}

columns.add(new Column<>(sfName, f, ms.format));






