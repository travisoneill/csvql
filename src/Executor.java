import java.util.List;

public class Executor {
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

    int found = 0;
    String[] line;
    while ((line = reader.readRow()) != null) {
      if (found == query.limit) {
        reader.close();
        break;
      }

      if (colname == null || column.where(line[idx], oper, comp)) {
        found++;
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