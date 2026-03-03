private static String formatForSalesforce(Object v, String format) {
    if (v == null) return "";

    if (format == null || format.trim().isEmpty() || "raw".equals(format)) {
        return String.valueOf(v);
    }

    switch (format) {

        case "date": { // yyyy-MM-dd
            if (v instanceof java.time.LocalDate) {
                return ((java.time.LocalDate) v)
                        .format(DateTimeFormatter.ISO_LOCAL_DATE);
            }

            if (v instanceof java.sql.Date) {
                return ((java.sql.Date) v)
                        .toLocalDate()
                        .format(DateTimeFormatter.ISO_LOCAL_DATE);
            }

            if (v instanceof java.util.Date) {
                Instant i = ((java.util.Date) v).toInstant();
                return LocalDateTime
                        .ofInstant(i, ZoneOffset.UTC)
                        .toLocalDate()
                        .format(DateTimeFormatter.ISO_LOCAL_DATE);
            }

            return String.valueOf(v);
        }

        case "datetime_utc": { // yyyy-MM-ddTHH:mm:ss.SSSZ
            Instant instant = null;

            if (v instanceof Instant) {
                instant = (Instant) v;
            } else if (v instanceof OffsetDateTime) {
                instant = ((OffsetDateTime) v).toInstant();
            } else if (v instanceof ZonedDateTime) {
                instant = ((ZonedDateTime) v).toInstant();
            } else if (v instanceof LocalDateTime) {
                instant = ((LocalDateTime) v).toInstant(ZoneOffset.UTC);
            } else if (v instanceof java.util.Date) {
                instant = ((java.util.Date) v).toInstant();
            }

            if (instant != null) {
                DateTimeFormatter fmt =
                        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX")
                                .withZone(ZoneOffset.UTC);
                return fmt.format(instant);
            }

            return String.valueOf(v);
        }

        case "bool": {
            if (v instanceof Boolean) {
                return ((Boolean) v) ? "TRUE" : "FALSE";
            }
            return String.valueOf(v);
        }

        case "int": {
            if (v instanceof Number) {
                return String.valueOf(((Number) v).longValue());
            }
            return String.valueOf(v);
        }

        case "decimal": {
            if (v instanceof BigDecimal) {
                return ((BigDecimal) v).toPlainString();
            }
            if (v instanceof Number) {
                return BigDecimal
                        .valueOf(((Number) v).doubleValue())
                        .toPlainString();
            }
            return String.valueOf(v);
        }

        case "decimal2": {
            BigDecimal bd = null;

            if (v instanceof BigDecimal) {
                bd = (BigDecimal) v;
            } else if (v instanceof Number) {
                bd = BigDecimal.valueOf(((Number) v).doubleValue());
            }

            if (bd != null) {
                return bd.setScale(2, BigDecimal.ROUND_HALF_UP)
                         .toPlainString();
            }

            return String.valueOf(v);
        }

        default:
            return String.valueOf(v);
    }
}