import java.util.ArrayList;
import java.util.List;
import java.util.Random;

class Point {
  private List<Double> coords;// = new ArrayList<Double>();
  private static Random r = new Random( 42 );

  public Point(int n) {
    coords = new ArrayList<Double>( n );
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
    coords = new ArrayList<Double>( _coords.size() );
    coords.addAll(_coords);
  }

  public Point(Point pt) {
    coords = new ArrayList<Double>( pt.coords.size() );
    this.coords.addAll(pt.coords);
  }

  @Override
  public String toString()
  {
    return coords.toString();
  }

  static public Point RandomPoint(int n, Double min, Double max)
  {
    Point ret = new Point( n );
    for (int i = 0; i < ret.coords.size(); ++i )
    {
      ret.coords.set( i, min + (max - min) * r.nextDouble() );
    }

    return ret;
  }

  private Point addCoords(Point pt) {
    int len = coords.size();
    for(int i = 0; i < len; ++i) {
      double sum = (pt.coords.get(i) + coords.get(i));
      coords.set(i, sum);
    }
    return this;
  }

  private Point subCoords(Point pt) {
    int len = coords.size();
    for(int i = 0; i < len; ++i) {
      double sub = (coords.get(i) - pt.coords.get(i));
      coords.set(i, sub);
    }
    return this;
  }

  public void scale(double a) {
    int len = coords.size();
    for (int i = 0; i < len; i++) {
      coords.set(i, coords.get(i) * a);
    }
  }

  public double distance(Point p) {
    double res = 0.;
    for (int i = 0; i < coords.size(); i++) {
      double diff = coords.get(i) - p.coords.get(i);
      res += diff * diff;
    }
    return res;
  }

  public double norm() {
    double res = 0.;
    for (int i = 0; i < coords.size(); i++) {
      double x = coords.get(i);
      res += x * x;
    }
    return res;
  }

  public static int getNearest(List<Point> pts, Point pt) {
    int id_nearest = 0;
    Point nearest = pts.get(0);
    double min_dist = nearest.distance(pt);
    for (int i = 0; i < pts.size(); i++)  {
      Point centroid = pts.get(i);
      double dist = centroid.distance(pt);
      if(dist < min_dist) {
        id_nearest = i;
        min_dist = dist;
      }
    }
    return id_nearest;
  }

  public static Point add(Point lhs, Point rhs) {
    Point res = new Point(lhs);
    return res.addCoords(rhs);
  }

  public static Point sub(Point lhs, Point rhs) {
    Point res = new Point(lhs);
    return res.subCoords(rhs);
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