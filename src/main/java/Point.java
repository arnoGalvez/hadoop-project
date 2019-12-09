import java.util.ArrayList;
import java.util.List;

class Point {
  private List<Double> coords = new ArrayList<Double>();

  public Point(int n) {
    for (int i = 0; i < n; i++) {
      coords.add(0.);
    }
  }

  public int size() {
    return coords.size();
  }

  public List<Double> getCoords() {
    return coords;
  }

  public Point(List<Double> _coords) {
    coords.addAll(_coords);
  }

  public Point(Point pt) {
    this.coords.clear();
    this.coords.addAll(pt.coords);
  }

  private Point addCoords(Point pt) {
    int len = coords.size();
    for(int i = 0; i < len; ++i) {
      double sum = (pt.coords.get(i) + coords.get(i));
      coords.set(i, sum);
    }
    return this;
  }

  public void scale(double a) {
    int len = coords.size();
    for (int i = 0; i < len; i++) {
      coords.set(i, coords.get(i) * a);
    }
  }

  public static Point add(Point lhs, Point rhs) {
    Point res = new Point(lhs);
    return res.addCoords(rhs);
  }

  public static Point sum(List<Point> pts) {
    int ncoords = pts.get(0).coords.size();
    Point res = new Point(ncoords);
    for(Point pt : pts) {
      res = Point.add(res, pt);
    }
    return res;
  }

  public static Point scale(Point point, double a) {
    Point res = new Point(point);
    int len = res.size();
    for (int i = 0; i < len; i++) {
      res.coords.set(i, res.coords.get(i) * a);
    }

    return res;
  }

  public static Point mean(List<Point> pts) {
    Point res = Point.sum(pts);
    res.scale(1. / pts.size());
    return res;
  }
}