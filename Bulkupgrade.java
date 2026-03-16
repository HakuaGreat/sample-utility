import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class BulkQueryResultFileProcessor {

    private final HttpClient httpClient;
    private final RetailStoreSyncExecutor executor;

    public BulkQueryResultFileProcessor(HttpClient httpClient,
                                        RetailStoreSyncExecutor executor) {
        this.httpClient = httpClient;
        this.executor = executor;
    }

    public void processAllResults(String instanceUrl,
                                  String apiVersion,
                                  String jobId,
                                  String accessToken,
                                  Path workDir) throws Exception {

        String locator = null;
        int pageNo = 1;

        do {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(buildResultsUri(instanceUrl, apiVersion, jobId, locator))
                    .header("Authorization", "Bearer " + accessToken)
                    .header("Accept", "text/csv")
                    .GET()
                    .build();

            HttpResponse<InputStream> response = httpClient.send(
                    request,
                    HttpResponse.BodyHandlers.ofInputStream()
            );

            int status = response.statusCode();
            if (status < 200 || status >= 300) {
                throw new IllegalStateException(
                        "結果CSV取得失敗 status=" + status
                );
            }

            String nextLocator = response.headers()
                    .firstValue("Sforce-Locator")
                    .orElse(null);

            Path csvFile = workDir.resolve("bulk_result_" + pageNo + ".csv");

            try (InputStream bodyStream = response.body()) {
                Files.copy(bodyStream, csvFile, StandardCopyOption.REPLACE_EXISTING);
            }

            try (InputStream csvStream = Files.newInputStream(csvFile)) {
                executor.execute(csvStream);
            } finally {
                Files.deleteIfExists(csvFile);
            }

            if (nextLocator == null || "null".equalsIgnoreCase(nextLocator.trim())) {
                locator = null;
            } else {
                locator = nextLocator;
            }

            pageNo++;

        } while (locator != null);
    }

    private URI buildResultsUri(String instanceUrl,
                                String apiVersion,
                                String jobId,
                                String locator) {
        String base = instanceUrl
                + "/services/data/" + apiVersion
                + "/jobs/query/" + jobId
                + "/results";

        if (locator == null || locator.isBlank()) {
            return URI.create(base);
        }

        return URI.create(base + "?locator=" + locator);
    }
}