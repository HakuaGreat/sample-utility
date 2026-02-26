import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;

public class SalesforceResponseParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    static {
        MAPPER.findAndRegisterModules();
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * SalesforceのJSONレスポンスから必要な値だけ抜き出して返す。
     * - 取りたいキー: "id", "state", "jobId", "contentUrl" など（呼び出し側で指定）
     * - ネストも "result.id" みたいにドット区切りで指定可能
     */
    public static Map<String, String> pick(String json, String... paths) {
        try {
            JsonNode root = MAPPER.readTree(json);

            Map<String, String> out = new LinkedHashMap<>();
            for (String path : paths) {
                JsonNode node = root;
                for (String key : path.split("\\.")) {
                    if (node == null) break;
                    node = node.get(key);
                }

                // 値がない/Nullなら null を入れる（必要ならここで弾く）
                out.put(path, (node == null || node.isNull()) ? null : node.asText());
            }
            return out;

        } catch (JsonProcessingException e) {
            // ここ重要：Bulk2.0はHTMLエラー返すことあるので先頭だけ出すと原因追いやすい
            String head = json == null ? "null" : json.substring(0, Math.min(json.length(), 200));
            throw new RuntimeException("Salesforce response is not valid JSON. head=" + head, e);
        }
    }
}