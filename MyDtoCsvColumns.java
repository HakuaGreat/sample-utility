package your.pkg.csv;

import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

public final class MyDtoCsvColumns {
    private MyDtoCsvColumns() {}

    private static final DateTimeFormatter DT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static final List<CsvCol<MyDto>> COLS = Arrays.asList(
        new CsvCol<>("id", MyDto::getId),
        new CsvCol<>("name", MyDto::getName),
        new CsvCol<>("createdAt", d -> d.getCreatedAt() == null ? "" : DT.format(d.getCreatedAt()))
        // ... 300+列をこの順で並べる（この順＝ヘッダ順＝データ順）
    );
}
