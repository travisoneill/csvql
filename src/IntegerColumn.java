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
