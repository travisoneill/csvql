import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.Arrays;

public class QueryParser {
  public static Query parse(String queryStr) {
    Set<String> keywords = new HashSet<String>();
    keywords.add("SELECT");
    keywords.add("FROM");
    keywords.add("JOIN");
    keywords.add("ON");
    keywords.add("WHERE");
    keywords.add("LIMIT");
    keywords.add("INGEST");
    keywords.add("SCHEMA");

    Query queryObj = new Query();
    List<String> words = new ArrayList<>();
    StringBuilder word = new StringBuilder();

    int len = queryStr.length();
    for (int i = 0; i < len; i++) {
      char letter = queryStr.charAt(i);
      if (letter == ' ' || letter == ',' || i == len - 1) {
        if (i == len - 1) {
          word.append(letter);
        }

        if (!word.toString().equals("")) {
          words.add(word.toString());
          word = new StringBuilder();
        }
      } else {
        word.append(letter);
      }
    }

    String keyword = words.get(0);
    queryObj.type = keyword.toUpperCase();
    for (int i = 1; i < words.size(); i++) {
      String currentWord = words.get(i);
      String capital = currentWord.toUpperCase();
      if (keywords.contains(capital)) {
        keyword = capital;
      } else {
        switch (keyword.toUpperCase()) {
          case "SELECT":
            queryObj.select.add(currentWord);
            break;
          case "FROM":
            queryObj.from = parseTablename(currentWord);
            break;
          case "JOIN":
            queryObj.type = "JOIN";
            queryObj.join = parseTablename(currentWord);
            break;
          case "ON":
            queryObj.on.add(currentWord);
            break;
          case "WHERE":
            queryObj.where.add(currentWord);
            break;
          case "LIMIT":
            queryObj.limit = Integer.parseInt(currentWord);
            break;
          case "INGEST":
            queryObj.ingest.add(currentWord);
            break;
          case "SCHEMA":
            queryObj.type = "SCHEMA";
            queryObj.schema = currentWord;
        }
      }
    }

    if (queryObj.type.equals("SELECT")) {
      String tablename = queryObj.from[1];
      for (int i = 0; i < queryObj.select.size(); i++) {
        String tabCol = parseColumn(queryObj.select.get(i), tablename);
        queryObj.select.set(i, tabCol);
      }

      if (queryObj.where.size() > 0) {
        queryObj.where.set(0, parseColumn(queryObj.where.get(0), tablename));
      }
    }

    return queryObj;
  }

  private static String parseColumn(String col, String defaultTable) {
    if (col.equals("*")) {
      return "*";
    }

    String[] tabCol = col.split("\\.");
    if (tabCol.length == 1) {
      return defaultTable + "." + col;
    } else {
      return col;
    }
  }

  private static String[] parseTablename(String tablename) {
    String[] split = tablename.split("\\.");
    String dbname = split.length == 2 ? split[0] : System.getenv("CSVQL_DBNAME");
    String table = split.length == 2 ? split[1] : split[0];
    String[] ret = { dbname, table };
    return ret;
  }

  private String[] parseSelect(String statement) {
    String[] tablenames = statement.split(",");
    for (int i = 0; i < tablenames.length; i++) {
      tablenames[i] = tablenames[i].trim();
    }

    return tablenames;
  }
}