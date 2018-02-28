import java.util.HashMap;

public class Schema {
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