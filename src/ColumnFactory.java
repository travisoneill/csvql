public class ColumnFactory {
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