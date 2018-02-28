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
