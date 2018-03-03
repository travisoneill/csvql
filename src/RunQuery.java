import java.io.Console;
import java.util.Stack;

public class RunQuery {
  public static void main(String[] args) {
    Stack<String> queryLog = new Stack<>();
    Console console = System.console();
    String query = "";
    while (true) {
      String database = System.getenv("CSVQL_DBNAME");
      String cursor = database + "> ";
      String queryPart = console.readLine(cursor);

      if (queryPart.length() == 0) {
        continue;
      }

      if (queryPart.charAt(queryPart.length() - 1) == ';') {
        query += queryPart.substring(0, queryPart.length() - 1);
        query = query.trim();
      } else {
        query += queryPart.trim() + " ";
        continue;
      }

      System.out.println(query);

      try {
        Executor.execute(query);
      } catch (Exception err) {
        System.out.println(err.getMessage());
      }

      queryLog.push(query.trim());
      query = "";
    }
  }
}