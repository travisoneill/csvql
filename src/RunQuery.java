import java.io.Console;
import java.util.Stack;

public class RunQuery {
  public static void main(String[] args) {
    System.out.println("Enter Query:");
    Stack<String> queryLog = new Stack<>();
    Console console = System.console();
    String query = "";
    while (true) {
      String queryPart = console.readLine("> ");

      // if (queryPart.length() == 0) {
      //   console.printf("  %s", queryLog.peek());
      //   query = console.readLine("\r> ");
      // }

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