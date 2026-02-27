public final class ReflectiveCsvWriter<T> {
    private final List<Field> fields;

    private ReflectiveCsvWriter(Class<T> clazz) {
        this.fields = Arrays.stream(clazz.getDeclaredFields())
            .filter(f -> !Modifier.isStatic(f.getModifiers()))
            .peek(f -> f.setAccessible(true))
            // ★順序固定：これがズレ防止の芯
            .sorted(Comparator.comparing(Field::getName))
            .toList();
    }

    public static <T> ReflectiveCsvWriter<T> forClass(Class<T> clazz) {
        return new ReflectiveCsvWriter<>(clazz);
    }

    public void writeHeader(PrintWriter w) {
        w.println(fields.stream().map(Field::getName).collect(Collectors.joining(",")));
    }

    public void writeRow(PrintWriter w, T dto) {
        String line = fields.stream()
            .map(f -> get(dto, f))
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
}