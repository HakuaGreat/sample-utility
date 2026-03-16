import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.regex.Pattern;

private static final Pattern SCIENTIFIC_NOTATION_PATTERN =
        Pattern.compile("^[+-]?\\d+(\\.\\d+)?[eE][+-]?\\d+$");

private Object formatForDatabase(String fieldName, Class<?> type, String rawValue) {
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


----------------------------


private String normalizeScientificNotation(String value) {
    if (value == null) {
        return null;
    }

    String trimmed = value.trim();

    if (!SCIENTIFIC_NOTATION_PATTERN.matcher(trimmed).matches()) {
        return trimmed;
    }

    return new BigDecimal(trimmed).toPlainString();
}



----------------------------


private String emptyToNull(String value) {
    if (value == null) {
        return null;
    }

    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
}


----------------------------

Object formatted = formatForDatabase(dtoFieldName, field.getType(), rawValue);
field.set(dto, formatted);