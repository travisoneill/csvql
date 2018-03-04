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
    int len = 0;
    for (int i = 0; i < colnames.size(); i++) {
      len += colnames.get(i).indexOf("*") != -1 ? schema.length() : 1;
    }

    int[] indices = new int[len];
    int offset = 0;
    for (int i = 0; i < colnames.size(); i++) {
      String colname = colnames.get(i);
      if (colname.indexOf("*") != -1) {
        int j;
        for (j = 0; j < schema.length(); j++) {
          indices[i + offset + j] = j;
        }
        offset += j - 1;
      } else {
        indices[i + offset] = schema.getIndex(colname);
      }
    }

    return indices;
  }
}