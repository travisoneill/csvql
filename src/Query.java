import java.util.ArrayList;
import java.util.List;

public class Query {
  public String type;
  public String[] from;
  public List<String> select = new ArrayList<>();
  public List<String> where = new ArrayList<>();
  public int limit = Integer.MAX_VALUE;
  public String[] join = null;
  public List<String> on = new ArrayList<>();
  public List<String> ingest = new ArrayList<>();

  public void print() {
    Utils.print("SELECT: ", select);
    Utils.print("WHERE: ", where);
    Utils.print("FROM: ", from);
    Utils.print("JOIN: ", join);
    Utils.print("ON: ", on);
    System.out.println(" ");
  }
}