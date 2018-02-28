import java.util.ArrayList;
import java.util.List;

public class Query {
  public String[] from;
  public List<String> select = new ArrayList<>();
  public List<String> where = new ArrayList<>();
  public int limit = Integer.MAX_VALUE;
}