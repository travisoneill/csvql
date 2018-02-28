public class Column {
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