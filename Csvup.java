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
                throw new IOException("CSV upload failed. status=" + code + " body=" + truncate(resp.body(), 2000));
            }

            // Retry-After があれば最優先（秒 or HTTP-date だが、SF/ゲートウェイ系は秒が多い）
            Long retryAfterSeconds = parseRetryAfterSeconds(resp);
            sleepBackoff(retryAfterSeconds, attempt);
        }

        // ここには来ないがコンパイラ用
        throw new IOException("CSV upload failed: exhausted retries");
    }

    private static boolean isRetryable(int code) {
        return code == 429 || code == 500 || code == 502 || code == 503 || code == 504;
    }

    private static Long parseRetryAfterSeconds(HttpResponse<?> resp) {
        Optional<String> ra = resp.headers().firstValue("Retry-After");
        if (ra.isEmpty()) return null;
        String v = ra.get().trim();
        // 基本「秒」だけ対応（HTTP-dateは必要になったら追加）
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException ignore) {
            return null;
        }
    }

    /**
     * Backoff strategy:
     * - If retryAfterSeconds != null: sleep that + small jitter
     * - else: exponential backoff (base 1s) capped at 30s + jitter
     */
    private static void sleepBackoff(Long retryAfterSeconds, int attempt) throws InterruptedException {
        long jitterMs = ThreadLocalRandom.current().nextLong(0, 350); // 0..349ms

        if (retryAfterSeconds != null) {
            long ms = retryAfterSeconds * 1000L + jitterMs;
            Thread.sleep(ms);
            return;
        }

        // attempt=1 => 1s, 2 => 2s, 3 => 4s, 4 => 8s ...
        long baseMs = 1000L * (1L << Math.min(attempt - 1, 5)); // 1,2,4,8,16,32
        long cappedMs = Math.min(baseMs, 30_000L);
        Thread.sleep(cappedMs + jitterMs);
    }

    private static String truncate(String s, int max) {
        if (s == null) return "";
        if (s.length() <= max) return s;
        return s.substring(0, max) + "...(truncated)";
    }
}