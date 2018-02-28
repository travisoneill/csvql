import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;


public class RunQuery {
  public static void main(String[] args) {
    System.out.println("Hello!");
    while (true) {
      Console console = System.console();
      String query = console.readLine("> ");
      System.out.println(query);
      try {
        Executor.execute(query);
      } catch (Exception err) {
        System.out.println(err.getMessage());
      }
    }
  }
}

class Query {
  public String from;
  public List<String> select = new ArrayList<>();
  public List<String> where = new ArrayList<>();
  public int limit;
}


class QueryParser {
  public static Query parse(String queryStr) {
    Set<String> keywords = new HashSet<String>();
    keywords.add("SELECT");
    keywords.add("FROM");
    keywords.add("WHERE");
    keywords.add("LIMIT");

    Query queryObj = new Query();
    List<String> words = new ArrayList<>();
    StringBuilder word = new StringBuilder();

    int len = queryStr.length();
    for (int i = 0; i < len; i++) {
      char letter = queryStr.charAt(i);
      if (letter == ' ' || letter == ',' || i == len - 1) {
        if (i == len - 1) {
          word.append(letter);
        }

        if (!word.toString().equals("")) {
          words.add(word.toString());
          word = new StringBuilder();
        }
      } else {
        word.append(letter);
      }
    }

    String keyword = words.get(0);
    for (int i = 1; i < words.size(); i++) {
      String currentWord = words.get(i);
      String capital = currentWord.toUpperCase();
      if (keywords.contains(capital)) {
        keyword = capital;
      } else {
        switch (keyword.toUpperCase()) {
          case "SELECT":
            queryObj.select.add(currentWord);
            break;
          case "FROM":
            queryObj.from = currentWord;
            break;
          case "WHERE":
            queryObj.where.add(currentWord);
            break;
          case "LIMIT":
            queryObj.limit = Integer.parseInt(currentWord);
        }
      }
    }

    return queryObj;
  }

  private String[] parseSelect(String statement) {
    String[] tablenames = statement.split(",");
    for (int i = 0; i < tablenames.length; i++) {
      tablenames[i] = tablenames[i].trim();
    }

    return tablenames;
  }
}

class TableReader {
  private BufferedReader reader;
  public int columnCount = 0;
  private Boolean endOfFile = false;

  public static TableReader create(String tablename) {
    String filename = tablename + ".csv";
    try {
      return new TableReader(filename);
    } catch (IOException err) {
      String msg = err.getMessage();
      System.out.println(msg);
      return null;
    }
  }

  private TableReader(String filename) throws IOException {
    String home = System.getenv("CSVQL_DBHOME");
    String db = System.getenv("CSVQL_DBNAME");
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

    return rowData;
  }

  public String[] readRow() {
    try {
      return row();
    } catch (IOException err) {
      String msg = err.getMessage();
      System.out.println(msg);
      return new String[0];
    }
  }
}

class Schema {
  private Column[] columns;
  private HashMap<String, Integer> columnIndex = new HashMap<>();

  public Schema(String[] names, String[] types) {
    columns = new Column[names.length];
    for (int i = 0; i < names.length; i++) {
      columns[i] = ColumnFactory.create(names[i], types[i]);
      columnIndex.put(names[i], i);
    }
  }

  public int length() {
    return columns.length;
  }

  public int getIndex(String colname) {
    // System.out.println(colname);
    if (colname == null) {
      return -1;
    } else {
      return columnIndex.get(colname);
    }
  }

  public Column getColumn(String colname) {
    if (colname == null) {
      return null;
    } else {
      return columns[columnIndex.get(colname)];
    }
  }
}

class Executor {
  private static int[] parseSelect(List<String> colnames, Schema schema) {
    int[] indices;
    if (colnames.get(0).equals("*")) {
      indices = new int[schema.length()];
      for (int i = 0; i < indices.length; i++) {
        indices[i] = i;
      }
    } else {
      indices = new int[colnames.size()];
      for (int i = 0; i < indices.length; i++) {
        indices[i] = schema.getIndex(colnames.get(i));
      }
    }

    return indices;
  }

  public static void execute(String sqlQuery) {
    Query query = QueryParser.parse(sqlQuery);
    TableReader reader = TableReader.create(query.from);
    Schema schema = new Schema(reader.readRow(), reader.readRow());
    int[] selectIdx = parseSelect(query.select, schema);
    String colname = query.where.size() > 0 ? query.where.get(0) : null;
    String oper = query.where.size() > 0 ? query.where.get(1) : null;
    String comp = query.where.size() > 0 ? query.where.get(2) : null;
    Column column = schema.getColumn(colname);
    int idx = schema.getIndex(colname);

    String[] line;
    while ((line = reader.readRow()) != null) {
      if (colname == null || column.where(line[idx], oper, comp)) {
        printRow(line, selectIdx);
      }
    }
  }

  private static void printRow(String[] line, int[] selectIdx) {
    StringBuilder out = new StringBuilder();
    int len = selectIdx.length;
    for (int i = 0; i < len; i++) {
      out.append(line[i]);
      if (i < len - 1) {
        out.append(" | ");
      }
    }

    System.out.println(out.toString());
  }
}

class ColumnFactory {
  private enum ColumnType { STRING, INTEGER, FLOAT }

  public static Column create(String name, String type) {
    ColumnType coltype = ColumnType.valueOf(type);
    switch (coltype) {
      case STRING:
        return new StringColumn(name, type);
      case INTEGER:
        return new IntegerColumn(name, type);
      case FLOAT:
        return new FloatColumn(name, type);
      default:
        return null;
    }
  }
}

class Column {
  private final String name;
  private final String type;

  Column(String colname, String coltype) {
    name = colname;
    type = coltype;
  }

  public boolean where(String val, String oper, String comp) {
    return false;
  }

  public boolean where(String val) {
    return true;
  }
}

class StringColumn extends Column {

  StringColumn(String name, String type) {
    super(name, type);
  }

  public boolean where(String val, String oper, String comp) {
    int compVal = val.compareTo(comp);
    switch (oper) {
      case "=":
        return compVal == 0;
      case ">":
        return compVal == 1;
      case "<":
        return compVal == -1;
      case ">=":
        return compVal != -1;
      case "<=":
        return compVal != 1;
      default:
        return true;
    }
  }
}

class IntegerColumn extends Column {

  IntegerColumn(String name, String type) {
    super(name, type);
  }

  public boolean where(String val, String oper, String comp) {
    switch (oper) {
      case "=":
        return Integer.parseInt(val) == Integer.parseInt(comp);
      case ">":
        return Integer.parseInt(val) > Integer.parseInt(comp);
      case "<":
        return Integer.parseInt(val) < Integer.parseInt(comp);
      default:
        return true;
    }
  }
}

class FloatColumn extends Column {

  FloatColumn(String name, String type) {
    super(name, type);
  }

  public boolean where(String val, String oper, String comp) {
    switch (oper) {
      case "=":
        return Float.parseFloat(val) == Float.parseFloat(comp);
      case ">":
        return Float.parseFloat(val) > Float.parseFloat(comp);
      case "<":
        return Float.parseFloat(val) < Float.parseFloat(comp);
      default:
        return true;
    }
  }
}
