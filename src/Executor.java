import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;

public class Executor {

  public static void execute(String sqlQuery) {
    Query query = QueryParser.parse(sqlQuery);
    if (query.isJoin()) {
      executeJoin(query);
    } else {
      executeSimple(query);
    }
  }

  private static void executeJoin(Query query) {
    String leftTable = query.from[1];
    String rightTable = query.join[1];
    TableReader leftReader = TableReader.create(query.from);
    TableReader rightReader = TableReader.create(query.join);
    Schema leftSchema = new Schema(leftReader.readRow(), leftReader.readRow(), leftTable);
    Schema rightSchema = new Schema(rightReader.readRow(), rightReader.readRow(), rightTable);
    String primaryKey = null;
    String foreignKey = null;

    for (int i = 0; i < query.on.size(); i++) {
      String[] tab = query.on.get(i).split("\\.");
      String tablename = tab[0];
      if (tablename == rightTable) {
        foreignKey = tab[1];
      } else if (tablename == leftTable) {
        primaryKey = tab[1];
      }
    }

    List<String> leftSelect = new ArrayList<>();
    leftSelect.add(primaryKey);
    for (int i = 0; i < query.select.size(); i++) {
      String[] tabCol = query.select.get(i).split("\\.");
      if (tabCol.length == 1) {
        leftSelect.add(tabCol[0]);
      } else if (tabCol[0] == leftTable) {
        leftSelect.add(tabCol[1]);
      }
    }


    Query leftQuery = new Query();
    leftQuery.select = leftSelect;
    leftQuery.from = query.from;
    String[] whereTabCol = query.where.get(0).split("\\.");
    if (whereTabCol.length == 1) {
      leftQuery.where = query.where;
    } else if (whereTabCol[0] == leftTable) {
      // leftQuery
    }

    // if (query.where.size() > 0 && query.where.get(0).split("\\.") == leftTable) {
    //   leftQuery.where =
    // }



    Map<String, TableReader> readerMap = new HashMap<>();
    readerMap.put(leftTable, leftReader);
    readerMap.put(rightTable, rightReader);

    Map<String, Schema> schemaMap = new HashMap<>();
    schemaMap.put(leftTable, leftSchema);
    schemaMap.put(rightTable, rightSchema);


  }

  private static void executeSimple(Query query) {
    TableReader reader = TableReader.create(query.from);
    Schema schema = new Schema(reader.readRow(), reader.readRow(), reader.name);
    SelectIterator selector = new SelectIterator(reader, schema, query);

    int found = 0;
    String[] row;
    while (found < query.limit && (row = selector.next()) != null) {
      found++;
      printRow(row);
    }

    reader.close();
  }

  private static void printRow(String[] row) {
    StringBuilder out = new StringBuilder();
    int len = row.length;
    for (int i = 0; i < len; i++) {
      out.append(row[i]);
      if (i < len - 1) {
        out.append(" | ");
      }
    }

    System.out.println(out.toString());
  }
}
