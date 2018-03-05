import java.util.concurrent.BlockingQueue;
import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.io.IOException;

public class IngestionReader implements Runnable {
  private BufferedReader reader = null;
  protected BlockingQueue<String> queue = null;
  private final Charset UTF_8 = StandardCharsets.UTF_8;

  public IngestionReader(Path csvPath, BlockingQueue<String> queue) {
    this.queue = queue;
    try {
      reader = Files.newBufferedReader(csvPath, UTF_8);
    } catch (IOException err) {
      Utils.traceErr(err);
    }
  }

  @Override
  public void run() {
    long startTime = System.currentTimeMillis();
    String colnames;
    String firstLine;
    int recordCount = 1;

    try {
      colnames = reader.readLine();
      firstLine = reader.readLine();
    } catch (IOException err) {
      Utils.traceErr(err);
      try {
        reader.close();
      } catch (IOException err2) {
        Utils.traceErr(err2);
      }

      return;
    }

    String[] lineData = firstLine.split(",");
    StringBuilder typesLine = new StringBuilder();

    String comma = "";
    for (int i = 0; i < lineData.length; i++) {
      typesLine.append(comma);
      comma = ",";

      try {
        Integer.parseInt(lineData[i]);
        typesLine.append("INTEGER");
        continue;
      } catch (NumberFormatException err) {}

      try {
        Float.parseFloat(lineData[i]);
        typesLine.append("FLOAT");
        continue;
      } catch (NumberFormatException err) {}

      typesLine.append("STRING");
    }

    try {
      queue.put(colnames);
      queue.put(typesLine.toString());
      queue.put(firstLine);

      String line;
      while((line = reader.readLine()) != null) {
        recordCount++;
        queue.put(line);
      }

      queue.put("__EOF__");
    } catch (IOException err) {
      Utils.traceErr(err);
    } catch (InterruptedException err) {
      Utils.traceErr(err);
    } finally {
      try {
        reader.close();
      } catch (IOException err) {
        Utils.traceErr(err);
      }
    }

    long endTime = System.currentTimeMillis();
    String runTime = String.valueOf(endTime - startTime);
    String out = String.valueOf(recordCount) + " records read in " + runTime + " ms";
    System.out.println(out);
  }
}