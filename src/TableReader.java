import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class TableReader {
  private BufferedReader reader;
  public int columnCount = 0;
  private Boolean endOfFile = false;
  public String name;

  public static TableReader create(String[] tablename) {
    try {
      return new TableReader(tablename);
    } catch (IOException err) {
      Utils.traceErr(err);
      return null;
    }
  }

  private TableReader(String[] tablename) throws IOException {
    String home = System.getenv("CSVQL_DBHOME");
    String db = tablename[0];
    name = tablename[1];
    String filename = tablename[1] + ".csv";
    String path = home + "/" + db + "/" + filename;
    FileReader fileReader = new FileReader(path);
    reader = new BufferedReader(fileReader);
  }

  private String[] row() throws IOException {
    String[] rowData = null;

    if (endOfFile) {
      return rowData;
    }

    if (columnCount == 0) {
      rowData = reader.readLine().split(",");
      columnCount = rowData.length;
      return rowData;
    }

    rowData = new String[columnCount];
    int colIdx = 0;
    StringBuilder builder = new StringBuilder();
    int comma = (int) ',';
    int newline = (int) '\n';
    while (colIdx < columnCount) {
      int codePoint = reader.read();
      if (codePoint == comma || codePoint == newline || codePoint == -1) {
        rowData[colIdx] = builder.toString();
        endOfFile = codePoint == -1;
        if (codePoint == comma) {
          builder = new StringBuilder();
        }
        colIdx++;
      } else {
        builder.appendCodePoint(codePoint);
      }
    }

    return endOfFile ? null : rowData;
  }

  public String[] readRow() {
    try {
      return row();
    } catch (IOException err) {
      String msg = err.getMessage();
      System.out.println(msg);
      return null;
    }
  }

  public void close() {
    try {
      reader.close();
    } catch (IOException err) {
      System.out.println(err.getMessage());
    }
  }
}