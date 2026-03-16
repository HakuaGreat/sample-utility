す



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





#########################

try (InputStream in = new FileInputStream("input.csv");
     Reader reader = CsvToolkit.newReader(in);
     OutputStream out = new FileOutputStream("out.csv");
     Writer writer = CsvToolkit.newWriter(out)) {

    CsvToolkit.extractCsv(reader, writer,
            Arrays.asList("Name", "PostalCode__c"),
            true);
}





------------------------------




public static void extractStream(
        Reader reader,
        List<String> selectedColumns,
        boolean strictMissingColumn,
        ExtractConsumer consumer
) throws IOException {

    CsvToolkit.CsvParser parser = new CsvToolkit.CsvParser(reader);

    List<String> header = parser.nextRecord();
    if (header == null) return;

    CsvToolkit.ColumnSelection sel =
            CsvToolkit.ColumnSelection.build(header, selectedColumns, strictMissingColumn);

    List<String> rec;
    long rowNo = 1;

    while ((rec = parser.nextRecord()) != null) {
        rowNo++;

        List<String> picked = sel.pick(rec);

        Map<String, String> row = new LinkedHashMap<>();
        for (int i = 0; i < sel.outputHeader.size(); i++) {
            row.put(sel.outputHeader.get(i), picked.get(i));
        }

        consumer.accept(rowNo, row);
    }
}

public interface ExtractConsumer {
    void accept(long rowNo, Map<String, String> row);
}








--------------------------------------------------------
// ApplicationContext ctx = ...（既存）
// File successCsv = ...（保存した成功CSV）

SfIdApplyRunner.apply(ctx, successCsv);









--------------------------------------------------------
// 追加で必要：出力ディレクトリとファイル名ベース
File outDir = new File("/path/to/out");      // 例: 結果出力先ディレクトリ
String baseName = "payload";                 // 例: payload_001.csv, payload_002.csv...
long maxBytesPerFile = 9_000_000L;           // 例: 9MB（制限に合わせて調整）

List<String> header = columns.stream()
        .map(c -> c.sfName)
        .collect(Collectors.toList());

try (RotatingCsvWriter w = RotatingCsvWriter.open(
        outDir,
        baseName,
        maxBytesPerFile,
        StandardCharsets.UTF_8,
        true,        // 各ファイルにヘッダーを書きたいなら true
        header
)) {
    for (T dto : cursor) {
        List<String> row = new ArrayList<>(columns.size());
        for (Column<T> col : columns) {
            Object raw;
            try {
                raw = col.field.get(dto);
            } catch (IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }

            String formatted = formatForSalesforce(raw, col.format);

            // ★重要：ここで escapeCsv しない（二重エスケープ防止）
            row.add(formatted);
        }

        // ★RotatingCsvWriter が「エスケープ＆カンマ区切り＆改行」まで面倒みる
        w.writeRecord(row);
    }
}




----------------------------



InputStream csvStream = bulkApiClient.getQueryResult(jobId);

RetailStoreRepository repository = ...; // MyBatis mapper取得

RetailStoreSyncExecutor executor =
        new RetailStoreSyncExecutor(repository);

executor.execute(csvStream);




----------------------------



File csvFile = new File("C:/work/result.csv");

try (InputStream csvStream = new FileInputStream(csvFile)) {
    RetailStoreSyncExecutor executor =
            new RetailStoreSyncExecutor(repository);

    executor.execute(csvStream);
}




----------------------------




String rawValue = record.get(sfFieldName);

System.out.println("[DEBUG] sfFieldName=" + sfFieldName
        + ", dtoFieldName=" + dtoFieldName
        + ", rawValue=" + rawValue);

Field field = fieldCache.get(dtoFieldName);





----------------------------