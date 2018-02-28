import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.List;

public class QueryParser {
  public static Query parse(String queryStr) {
    Set<String> keywords = new HashSet<String>();
    keywords.add("SELECT");
    keywords.add("FROM");
    keywords.add("WHERE");
    keywords.add("LIMIT");

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
          case "WHERE":
            queryObj.where.add(currentWord);
            break;
          case "LIMIT":
            queryObj.limit = Integer.parseInt(currentWord);
        }
      }
    }

    return queryObj;
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