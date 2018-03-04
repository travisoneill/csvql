import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.io.*;

public class Executor {

  public static void execute(String sqlQuery) {
    Query query;
    try {
      query = QueryParser.parse(sqlQuery);
    } catch (Exception err) {
      System.out.println("Query Parsing Error:");
      System.out.println(err.getMessage());
      err.printStackTrace();
      return;
    }

    try {
      if (query.isJoin()) {
        executeJoin(query);
      } else {
        executeSimple(query);
      }
    } catch (Exception err) {
      System.out.println("Query Execution Error:");
      System.out.println(err.getMessage());
      err.printStackTrace();
    }
  }

  private static void executeJoin(Query query) {
    /* Executes join query by the following steps:
       example query:
         SELECT users.name, orders.price
         FROM users
         JOIN orders ON users.id = orders.user_id
         WHERE users.id = 1;

       1) Parse query into subqueries for the left and right tables
          Left: SELECT id, name FROM users WHERE id = 1;
          Right: SELECT user_id, price FROM orders;

       2) Run left table query and store results in a hashmap:
          <primaryKey, array<selected columns>>

       3) Run right table query and on each record returned:
          - pull foreign key from hashmap from 2
          - if found return selected data from hashmap + returned record

       - Assumes left table specifed in "FOR" statement and right table in "JOIN" statement
       - Assumes column refrences without table name are in left table
       - Assumes primary key in left table and foreign key in right table
    */

    String leftTable = query.from[1];
    String rightTable = query.join[1];
    TableReader leftReader = TableReader.create(query.from);
    TableReader rightReader = TableReader.create(query.join);
    Schema leftSchema = new Schema(leftReader.readRow(), leftReader.readRow(), leftTable);
    Schema rightSchema = new Schema(rightReader.readRow(), rightReader.readRow(), rightTable);
    String primaryKey = null;
    String foreignKey = null;

    // handle ordering in on statement and match correct columns with tables in JOIN/FROM
    for (int i = 0; i < query.on.size(); i++) {
      String[] tabCol = query.on.get(i).split("\\.");
      String tablename = tabCol[0];
      String colname = tabCol.length > 1 ? tabCol[1] : null;
      if (tablename.equals(rightTable)) {
        foreignKey = colname;
      } else if (tablename.equals(leftTable)) {
        primaryKey = colname;
      }
    }

    List<String> leftSelect = new ArrayList<>();
    List<String> rightSelect = new ArrayList<>();

    // need to always select primary and foreign key columns
    leftSelect.add(leftTable + "." + primaryKey);
    rightSelect.add(rightTable + "." + foreignKey);

    for (int i = 0; i < query.select.size(); i++) {
      String colname = query.select.get(i);
      if (colname == "*") {
        leftSelect.add(colname);
        rightSelect.add(colname);
        continue;
      }

      String[] tabCol = colname.split("\\.");
      if (tabCol.length == 1 || tabCol[0].equals(leftTable)) {
        // assume left table if not specified
        leftSelect.add(colname);
      } else if (tabCol[0].equals(rightTable)) {
        rightSelect.add(colname);
      }
    }

    Query leftQuery = new Query();
    leftQuery.select = leftSelect;
    leftQuery.from = query.from;

    Query rightQuery = new Query();
    rightQuery.select = rightSelect;
    rightQuery.from = query.join;

    String[] whereTabCol = query.where.size() > 0 ? query.where.get(0).split("\\.") : null;
    if (whereTabCol != null) {
      String whereTable = whereTabCol[0];
      if (whereTabCol.length == 1 || whereTable.equals(leftTable)) {
        // assume left table if not specified
        leftQuery.where = query.where;
      } else if (whereTable.equals(rightTable)) {
        rightQuery.where = query.where;
      }
    }

    SelectIterator leftIterator = new SelectIterator(leftReader, leftSchema, leftQuery);
    SelectIterator rightIterator = new SelectIterator(rightReader, rightSchema, rightQuery);

    Map<String, String[]> joinMap = new HashMap<>();

    // store left table as map: <leftTable.primaryKey, array<leftTable.col>>
    String[] row;
    while ((row = leftIterator.next()) != null) {
      joinMap.put(row[0], Arrays.copyOfRange(row, 1, row.length));
    }

    int recordsFound = 0;
    String[] rightRow;
    String[] leftRow;
    while ((rightRow = rightIterator.next()) != null && recordsFound < query.limit) {
      // for each row returned from right table pull joinMap[foreignKey]
      if ((leftRow = joinMap.get(rightRow[0])) != null) {
        // if found pk == fk. Return record.
        recordsFound++;
        printRow(leftRow, Arrays.copyOfRange(rightRow, 1, rightRow.length));
      }
    }
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

  private static void printRow(String[] ...rows) {
    StringBuilder out = new StringBuilder("| ");
    for (String[] row:rows) {
      int len = row.length;
      for (String s:row) {
        out.append(s);
        out.append(" | ");
      }
    }

    System.out.println(out.toString());
  }
}
