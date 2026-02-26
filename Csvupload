import java.io.IOException;
import java.net.URI;
import java.net.http.*;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

public class BulkV2Uploader {

    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(20))
            .build();

    /**
     * Bulk API 2.0 CSV upload with retry.
     *
     * Retries on: 429, 500, 502, 503, 504
     * - Respects Retry-After header if present (seconds).
     * - Otherwise exponential backoff + jitter.
     */
    public HttpResponse<String> uploadCsvWithRetry(
            String instanceBaseUrl,   // 例: https://xxxxx.my.salesforce.com
            String apiVersion,        // 例: v60.0
            String accessToken,
            String jobId,
            Path csvPath,
            int maxAttempts           // 例: 8
    ) throws IOException, InterruptedException {

        String url = instanceBaseUrl
                + "/services/data/" + apiVersion
                + "/jobs/ingest/" + jobId
                + "/batches";

        // ファイル送信は再試行しても同じ内容が送れるので、毎回 newBuilder で作る
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofMinutes(2))
                    .header("Authorization", "Bearer " + accessToken)
                    .header("Content-Type", "text/csv; charset=UTF-8")
                    .header("Accept", "application/json")
                    .PUT(HttpRequest.BodyPublishers.ofFile(csvPath))
                    .build();

            HttpResponse<String> resp;
            try {
                resp = client.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (HttpTimeoutException e) {
                // タイムアウトも一時障害扱いでリトライ対象にする
                if (attempt == maxAttempts) throw e;
                sleepBackoff(null, attempt);
                continue;
            }

            int code = resp.statusCode();
            if (code / 100 == 2) {
                return resp; // 成功
            }

            if (!isRetryable(code) || attempt == maxAttempts) {
                // ここでHTMLが返ってきても、そのまま呼び出し側がログできる