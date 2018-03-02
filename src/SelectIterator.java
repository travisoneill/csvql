import java.util.List;

public class SelectIterator {
  private TableReader reader;
  private int[] selectCols;
  private Column whereCol = null;
  private int whereColIdx = -1;
  private String whereOper = null;
  private String whereVal = null;

  public SelectIterator(TableReader rdr, Schema sch, Query query) {
    reader = rdr;
    selectCols = parseSelect(query.select, sch);
    String colname = query.where.size() > 0 ? query.where.get(0) : null;
    whereOper = query.where.size() > 0 ? query.where.get(1) : null;
    whereVal = query.where.size() > 0 ? query.where.get(2) : null;
    whereCol = sch.getColumn(colname);
    whereColIdx = sch.getIndex(colname);
  }

  public String[] next() {
    String[] line;
    while ((line = reader.readRow()) != null) {
      if (whereCol == null || whereCol.where(line[whereColIdx], whereOper, whereVal)) {
        return select(line);
      }
    }

    reader.close();
    return null;
  }

  private String[] select(String[] line) {
    String[] sel = new String[selectCols.length];
    for (int i = 0; i < sel.length; i++) {
      int idx = selectCols[i];
      sel[i] = line[idx];
    }

    return sel;
  }

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
}