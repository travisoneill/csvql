public class ColumnReference {
  public String table;
  public String name;

  public ColumnReference(String tabCol) {
    String[] ref = tabCol.split('\\.');
    if (ref.length !== 2) {
      System.out.println("Malformed Column Reference: " + tabCol);
      return;
    }

    table = ref[0];
    name = ref[1];
  }

  public ColumnReference(String tablename, String colname) {
    if (tablename == null) {
      System.out.println("Table Name Cannot be null");
      return;
    } else if (colname == null) {
      System.out.println("Column Name Cannot be null");
      return;
    }

    table = tablename;
    name = colname;
  }
}