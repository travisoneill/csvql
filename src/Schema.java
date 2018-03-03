import java.util.HashMap;

public class Schema {
  private Column[] columns;
  private HashMap<String, Integer> columnIndex = new HashMap<>();

  public Schema(String[] names, String[] types, String tablename) {
    columns = new Column[names.length];
    for (int i = 0; i < names.length; i++) {
      String name = tablename + "." + names[i];
      columns[i] = ColumnFactory.create(name, types[i]);
      columnIndex.put(name, i);
    }
  }

  public int length() {
    return columns.length;
  }

  public int getIndex(String colname) {
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