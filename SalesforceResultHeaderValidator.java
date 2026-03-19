import java.util.List;

public class SalesforceResultHeaderValidator implements CsvHeaderValidator {

    @Override
    public void validate(List<String> headers) {
        if (headers == null || headers.isEmpty()) {
            throw new CsvValidationException("CSVヘッダが存在しません。");
        }

        boolean hasSfId = headers.contains("sf__Id") || headers.contains("id") || headers.contains("Id");
        boolean hasLinkKey = headers.contains("LinkKey__c") || headers.contains("linkKey") || headers.contains("LINK_KEY");

        if (!hasSfId) {
            throw new CsvValidationException("必須カラムがありません: sf__Id / id / Id");
        }

        if (!hasLinkKey) {
            throw new CsvValidationException("必須カラムがありません: LinkKey__c / linkKey / LINK_KEY");
        }
    }
}