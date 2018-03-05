import java.util.concurrent.BlockingQueue;
import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.io.IOException;

public class IngestionWriter implements Runnable {
  private BufferedWriter writer = null;
  protected BlockingQueue<String> queue = null;
  private final Charset UTF_8 = StandardCharsets.UTF_8;

  public IngestionWriter(Path tablePath, BlockingQueue<String> queue) {
    this.queue = queue;
    try {
      writer = Files.newBufferedWriter(tablePath, UTF_8);
    } catch (IOException err) {
      Utils.traceErr(err);
    }
  }

  @Override
  public void run() {
    long startTime = System.currentTimeMillis();
    int recordCount = -2;
    try {
      while (true) {
        String line = queue.take();
        if (line.equals("__EOF__")) {
          break;
        }

        recordCount++;
        writer.write(line);
        writer.newLine();
      }

    } catch (IOException err) {
      Utils.traceErr(err);
    } catch (InterruptedException err) {
      Utils.traceErr(err);
    } finally {
      try {
        writer.close();
      } catch (IOException err) {
        Utils.traceErr(err);
      }
    }

    long endTime = System.currentTimeMillis();
    String runTime = String.valueOf(endTime - startTime);
    String out = String.valueOf(recordCount) + " records written in " + runTime + " ms";
    System.out.println(out);
  }
}