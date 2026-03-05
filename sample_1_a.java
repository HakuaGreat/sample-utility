import java.io.*;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Objects;

/**
 * CSVを「最大バイト数」で自動分割しながら書くWriter。
 * - record単位を守る（レコード途中で分割しない）
 * - 各分割ファイルにヘッダーを書ける
 * - Java11 / 標準のみ
 *
 * 使い方：
 *   try (RotatingCsvWriter w = RotatingCsvWriter.open(outDir, "payload", 9_000_000, UTF_8, true, header)) {
 *       w.writeRecord(row1);
 *       w.writeRecord(row2);
 *   }
 */
public final class RotatingCsvWriter implements Closeable {

    private final File outDir;
    private final String baseName;
    private final long maxBytesPerFile;
    private final Charset charset;
    private final boolean writeHeaderEachPart;
    private final List<String> header;

    private int partNo = 0;
    private OutputStream out;
    private long bytesWritten = 0;

    private final byte[] lfBytes;

    private RotatingCsvWriter(
            File outDir,
            String baseName,
            long maxBytesPerFile,
            Charset charset,
            boolean writeHeaderEachPart,
            List<String> header
    ) {
        this.outDir = Objects.requireNonNull(outDir, "outDir");
        this.baseName = Objects.requireNonNull(baseName, "baseName");
        this.maxBytesPerFile = maxBytesPerFile;
        this.charset = Objects.requireNonNull(charset, "charset");
        this.writeHeaderEachPart = writeHeaderEachPart;
        this.header = header; // headerはnull許容（ヘッダー無しCSVなら）
        this.lfBytes = "\n".getBytes(this.charset);
    }

    public static RotatingCsvWriter open(
            File outDir,
            String baseName,
            long maxBytesPerFile,
            Charset charset,
            boolean writeHeaderEachPart,
            List<String> header
    ) throws IOException {
        if (maxBytesPerFile <= 0) throw new IllegalArgumentException("maxBytesPerFile must be > 0");
        if (!outDir.exists() && !outDir.mkdirs()) {
            throw new IOException("Failed to create dir: " + outDir.getAbsolutePath());
        }

        RotatingCsvWriter w = new RotatingCsvWriter(outDir, baseName, maxBytesPerFile, charset, writeHeaderEachPart, header);
        w.rotate(); // 最初のファイルを開く（必要ならヘッダーも書く）
        return w;
    }

    /** 1レコードを書く（必要ならファイルを分割してから書く） */
    public void writeRecord(List<String> cols) throws IOException {
        String line = toCsvLine(cols);
        byte[] lineBytes = line.getBytes(charset);

        // 1レコードがmaxを超えるなら分割不能（レコード単位を守るため）
        if (lineBytes.length + lfBytes.length > maxBytesPerFile) {
            throw new IllegalStateException("Single record exceeds maxBytesPerFile. bytes=" + (lineBytes.length + lfBytes.length));
        }

        // 追加したら超える？（超えるなら次ファイルへ）
        if (bytesWritten + lineBytes.length + lfBytes.length > maxBytesPerFile) {
            rotate();
        }

        out.write(lineBytes);
        out.write(lfBytes);
        bytesWritten += lineBytes.length + lfBytes.length;
    }

    /** 今の出力ファイル（デバッグ用） */
    public File currentFile() {
        if (partNo <= 0) return null;
        return new File(outDir, String.format("%s_%03d.csv", baseName, partNo));
    }

    private void rotate() throws IOException {
        closeCurrent();

        partNo++;
        File file = new File(outDir, String.format("%s_%03d.csv", baseName, partNo));
        out = new BufferedOutputStream(new FileOutputStream(file));
        bytesWritten = 0;

        if (writeHeaderEachPart && header != null) {
            String headerLine = toCsvLine(header);
            byte[] hb = headerLine.getBytes(charset);

            if (hb.length + lfBytes.length > maxBytesPerFile) {
                throw new IllegalStateException("Header exceeds maxBytesPerFile. bytes=" + (hb.length + lfBytes.length));
            }

            out.write(hb);
            out.write(lfBytes);
            bytesWritten += hb.length + lfBytes.length;
        }
    }

    private void closeCurrent() throws IOException {
        if (out != null) {
            out.flush();
            out.close();
            out = null;
        }
    }

    @Override
    public void close() throws IOException {
        closeCurrent();
    }

    // ===== ここは「昔のCSV書き込み」と同じロジックに寄せてある =====

    private static String toCsvLine(List<String> cols) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append(escapeCsv(cols.get(i)));
        }
        return sb.toString();
    }

    /** Java11向けの堅牢版（カンマ/改行/ダブルクォート/前後空白でクォート） */
    private static String escapeCsv(String s) {
        if (s == null) return "";

        boolean mustQuote = false;

        if (s.indexOf(',') >= 0 || s.indexOf('"') >= 0 || s.indexOf('\n') >= 0 || s.indexOf('\r') >= 0) {
            mustQuote = true;
        }

        if (!mustQuote && !s.isEmpty()) {
            char first = s.charAt(0);
            char last = s.charAt(s.length() - 1);
            if (first == ' ' || first == '\t' || last == ' ' || last == '\t') {
                mustQuote = true;
            }
        }

        if (s.indexOf('"') >= 0) {
            s = s.replace("\"", "\"\"");
            mustQuote = true;
        }

        return mustQuote ? "\"" + s + "\"" : s;
    }
}