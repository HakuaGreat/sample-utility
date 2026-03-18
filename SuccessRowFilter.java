import java.util.Map;

public class SuccessRowFilter implements CsvRowFilter {

    @Override
    public boolean test(Map<String, String> row) {
        String success = row.get("success");
        if (success == null) {
            success = row.get("Success");
        }
        return "true".equalsIgnoreCase(success);
    }
}