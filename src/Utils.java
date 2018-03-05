import java.util.List;

public class Utils {
  public static void print(List<String> list) {
    StringBuilder out = new StringBuilder();
    for (int i = 0; i < list.size(); i++) {
      out.append(list.get(i) + "  ");
    }

    System.out.println(out.toString());
  }

  public static void print(String[] list) {
    StringBuilder out = new StringBuilder();
    for (int i = 0; i < list.length; i++) {
      out.append(list[i] + "  ");
    }

    System.out.println(out.toString());
  }

  public static void print(int[] list) {
    StringBuilder out = new StringBuilder();
    for (int i = 0; i < list.length; i++) {
      out.append(list[i] + "  ");
    }

    System.out.println(out.toString());
  }

  public static void print(String start, List<String> list) {
    StringBuilder out = new StringBuilder(start);
    for (int i = 0; i < list.size(); i++) {
      out.append(list.get(i) + "  ");
    }

    System.out.println(out.toString());
  }

  public static void print(String start, String[] list) {
    StringBuilder out = new StringBuilder(start);
    for (int i = 0; i < list.length; i++) {
      out.append(list[i] + "  ");
    }

    System.out.println(out.toString());
  }

  public static void print(String start, int[] list) {
    StringBuilder out = new StringBuilder(start);
    for (int i = 0; i < list.length; i++) {
      out.append(list[i] + "  ");
    }

    System.out.println(out.toString());
  }

  public static void print(Object obj) {
    System.out.println(obj);
  }

  public static void traceErr(Exception err, String ...msgs) {
    for(String msg:msgs) {
      System.out.println(msg);
    }

    System.out.println(err.getMessage());
    err.printStackTrace();
  }

  public static void traceErr(Exception err) {
    System.out.println(err.getMessage());
    err.printStackTrace();
  }
}