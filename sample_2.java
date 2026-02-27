public final class MappedCsvWriter<T> {
    private final List<Col<T>> cols;

    private MappedCsvWriter(List<Col<T>> cols) {
        this.cols = cols;
    }

    public static <T> MappedCsvWriter<T> fromSfToDtoFieldMap(
            Class<T> clazz,
            LinkedHashMap<String, String> sfToDtoField // ★順序が大事なのでLinkedHashMap推奨
    ) {
        List<Col<T>> cols = new ArrayList<>();
        for (var e : sfToDtoField.entrySet()) {
            String sfName = e.getKey();
            String dtoFieldName = e.getValue();
            try {
                Field f = clazz.getDeclaredField(dtoFieldName);
                f.setAccessible(true);
                cols.add(new Col<>(sfName, f));
            } catch (NoSuchFieldException ex) {
                throw new IllegalArgumentException("DTOにフィールドが無い: " + dtoFieldName + " (SF列 " + sfName + ")", ex);
            }
        }
        return new MappedCsvWriter<>(cols);
    }

    public void writeHeader(PrintWriter w) {
        w.println(cols.stream().map(c -> c.sfName).collect(Collectors.joining(",")));
    }

    public void writeRow(PrintWriter w, T dto) {
        String line = cols.stream()
                .map(c -> get(dto, c.field))
                .collect(Collectors.joining(","));
        w.println(line);
    }

    private String get(T dto, Field f) {
        try {
            Object v = f.get(dto);
            return escape(v == null ? "" : String.valueOf(v));
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private String escape(String s) {
        if (s.contains(",") || s.contains("\n") || s.contains("\r") || s.contains("\"")) {
            return "\"" + s.replace("\"", "\"\"") + "\"";
        }
        return s;
    }

    private static final class Col<T> {
        final String sfName;
        final Field field;
        Col(String sfName, Field field) { this.sfName = sfName; this.field = field; }
    }
}