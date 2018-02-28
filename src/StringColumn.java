public class StringColumn extends Column {

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
