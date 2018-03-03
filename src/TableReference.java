public class TableReference {
  public String database;
  public String name;

  public TableReference(String dbTab) {
    String[] ref = dbTab.split('\\.');
    if (ref.length == 2) {
      database = ref[0];
      table = ref[1];
    } else if (ref.length == 1) {
      database = System.getenv("CSVQL_DBNAME");
      table = ref[0];
    } else {
      System.out.println("Malformed table reference: " + dbTab);
      return;
    }

  }

  public TableReference(String dbName, String tablename) {
    if (dbName == null) {
      System.out.println("DB name cannot be null");
      return;
    } else if (tablename == null) {
      System.out.println("Table name cannot be null");
      return;
    }

    table = dbName;
    name = tablename;
  }
}