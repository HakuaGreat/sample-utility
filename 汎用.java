public interface BulkInsertRepository<T> {
    void insert(List<T> list);
}

----------------------------

public abstract class AbstractCsvSyncExecutor<T> {

    private static final int CHUNK_SIZE = 1000;

    private final BulkInsertRepository<T> repository;

    protected AbstractCsvSyncExecutor(BulkInsertRepository<T> repository) {
        this.repository = repository;
    }

    public void execute(InputStream csvStream) throws Exception {
        Properties mapping = loadMapping();

        DtoAssembler<T> assembler = createAssembler(mapping);

        try (CsvResultReader reader = new CsvResultReader(
                new InputStreamReader(csvStream, StandardCharsets.UTF_8))) {

            reader.open();

            List<T> buffer = new ArrayList<>(CHUNK_SIZE);

            CsvRecord record;
            while ((record = reader.readRecord()) != null) {
                T dto = assembler.assemble(record);
                buffer.add(dto);

                if (buffer.size() >= CHUNK_SIZE) {
                    repository.insert(buffer);
                    buffer = new ArrayList<>(CHUNK_SIZE);
                }
            }

            if (!buffer.isEmpty()) {
                repository.insert(buffer);
            }
        }
    }

    protected abstract Properties loadMapping() throws Exception;

    protected abstract DtoAssembler<T> createAssembler(Properties mapping);
}

----------------------------

public abstract class AbstractCsvDtoAssembler<T> implements DtoAssembler<T> {

    private final Map<String, String> mapping = new HashMap<>();
    private final Map<String, Field> fieldCache = new HashMap<>();
    private final Class<T> dtoClass;

    protected AbstractCsvDtoAssembler(Properties props, Class<T> dtoClass) {
        this.dtoClass = dtoClass;

        for (String key : props.stringPropertyNames()) {
            mapping.put(normalize(key), props.getProperty(key));
        }

        for (Field field : dtoClass.getDeclaredFields()) {
            field.setAccessible(true);
            fieldCache.put(field.getName(), field);
        }
    }

    @Override
    public T assemble(CsvRecord record) {
        try {
            T dto = dtoClass.getDeclaredConstructor().newInstance();

            for (Map.Entry<String, String> entry : mapping.entrySet()) {
                String sfFieldName = entry.getKey();
                String dtoFieldName = entry.getValue();

                String rawValue = record.get(sfFieldName);
                Field field = fieldCache.get(dtoFieldName);

                if (field == null) {
                    continue;
                }

                Object formatted = formatForDatabase(dtoFieldName, field.getType(), rawValue);
                field.set(dto, formatted);
            }

            applyProgramValues(dto, record);

            return dto;

        } catch (Exception e) {
            throw new RuntimeException("CSVからDTOへの変換に失敗しました", e);
        }
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

    protected void applyProgramValues(T dto, CsvRecord record) {
    }

    protected String emptyToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    protected String normalize(String s) {
        if (s == null) {
            return null;
        }
        return s.replace("\uFEFF", "").trim().toLowerCase();
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
            // 数値でないならそのまま
        }

        return trimmed;
    }
}

----------------------------

import java.util.List;

public interface BulkInsertHandler<T> {
    void insert(List<T> list);
}

----------------------------

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class GenericCsvBridgeService<T> {

    private final DtoAssembler<T> assembler;
    private final BulkInsertHandler<T> insertHandler;
    private final int chunkSize;

    public GenericCsvBridgeService(
            DtoAssembler<T> assembler,
            BulkInsertHandler<T> insertHandler,
            int chunkSize) {
        this.assembler = assembler;
        this.insertHandler = insertHandler;
        this.chunkSize = chunkSize;
    }

    public void execute(InputStream csvStream) throws Exception {
        try (CsvResultReader reader = new CsvResultReader(
                new InputStreamReader(csvStream, StandardCharsets.UTF_8))) {

            reader.open();

            List<T> buffer = new ArrayList<>(chunkSize);

            CsvRecord record;
            while ((record = reader.readRecord()) != null) {
                T dto = assembler.assemble(record);
                buffer.add(dto);

                if (buffer.size() >= chunkSize) {
                    insertHandler.insert(buffer);
                    buffer = new ArrayList<>(chunkSize);
                }
            }

            if (!buffer.isEmpty()) {
                insertHandler.insert(buffer);
            }
        }
    }
}

----------------------------

import java.util.List;

public class RetailStoreInsertHandler implements BulkInsertHandler<RetailStoreDto> {

    private final RetailStoreRepository repository;

    public RetailStoreInsertHandler(RetailStoreRepository repository) {
        this.repository = repository;
    }

    @Override
    public void insert(List<RetailStoreDto> list) {
        repository.insert(list);
    }
}

----------------------------

Properties mapping = new Properties();
try (InputStream in = getClass().getResourceAsStream("/sf-mapping.properties")) {
    if (in == null) {
        throw new IllegalStateException("sf-mapping.properties が見つかりません");
    }
    mapping.load(in);
}

RetailStoreCsvDtoAssembler assembler =
        new RetailStoreCsvDtoAssembler(mapping, batchId);

RetailStoreInsertHandler insertHandler =
        new RetailStoreInsertHandler(repository);

GenericCsvBridgeService<RetailStoreDto> bridgeService =
        new GenericCsvBridgeService<>(assembler, insertHandler, 1000);

bridgeService.execute(csvStream);

----------------------------