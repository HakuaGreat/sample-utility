import java.util.Map;

public class SalesforceResultRowMapper implements CsvRowMapper<SalesforceResultDto> {

    @Override
    public SalesforceResultDto map(Map<String, String> row) {
        SalesforceResultDto dto = new SalesforceResultDto();
        dto.setSfId(get(row, "sf__Id", "id", "Id"));
        dto.setLinkKey(get(row, "LinkKey__c", "linkKey", "LINK_KEY"));
        dto.setCreated(toBoolean(get(row, "created", "Created")));
        dto.setSuccess(toBoolean(get(row, "success", "Success")));
        dto.setError(get(row, "error", "Error"));
        return dto;
    }

    private String get(Map<String, String> row, String... keys) {
        for (String key : keys) {
            if (row.containsKey(key)) {
                return row.get(key);
            }
        }
        return null;
    }

    private Boolean toBoolean(String value) {
        if (value == null) {
            return null;
        }
        return "true".equalsIgnoreCase(value.trim());
    }
}