import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public static List<File> listParts(File outDir, String baseName) {
    File[] files = outDir.listFiles((dir, name) ->
            name.matches(java.util.regex.Pattern.quote(baseName) + "_\\d{3}\\.csv"));

    if (files == null) return java.util.Collections.emptyList();

    return Arrays.stream(files)
            .sorted(Comparator.comparing(File::getName)) // payload_001 → payload_002…
            .collect(Collectors.toList());
}



----------------------------
List<File> parts = listParts(outDir, "payload");

for (File f : parts) {
    Bulk.createUpsert("RetailStore", f.getPath(), "LinkKey__c");
}