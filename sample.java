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


#####################

private static String escapeCsv(String s) {
    if (s == null) return "";

    boolean mustQuote = false;

    // RFC4180: comma / quote / CR / LF があればクォート
    if (s.indexOf(',') >= 0 || s.indexOf('"') >= 0 || s.indexOf('\n') >= 0 || s.indexOf('\r') >= 0) {
        mustQuote = true;
    }

    // 先頭末尾の空白・タブもクォート（Excel/SF系の誤解釈回避）
    if (!mustQuote && !s.isEmpty()) {
        char first = s.charAt(0);
        char last = s.charAt(s.length() - 1);
        if (first == ' ' || first == '\t' || last == ' ' || last == '\t') {
            mustQuote = true;
        }
    }

    // quoteは2個にエスケープ
    if (s.indexOf('"') >= 0) {
        s = s.replace("\"", "\"\"");
        mustQuote = true; // quoteを含んでたら必ず囲う
    }

    return mustQuote ? "\"" + s + "\"" : s;
}




############################



String line = String.join(",", row);
int expected = columns.size();
int actual = 1 + (int) line.chars().filter(ch -> ch == ',').count(); // 雑だが検知には効く

if (actual != expected) {
    System.err.println("[ERROR] column mismatch expected=" + expected + " actual=" + actual);
    System.err.println("[ERROR] line=" + line);
    throw new IllegalStateException("CSV column mismatch");
}

w.println(line);

