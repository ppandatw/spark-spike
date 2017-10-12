package spark.spike;

import scala.Serializable;
import scala.Tuple2;

import java.util.Comparator;

public class DepartmentComparator implements Comparator<Tuple2<String, Double>>, Serializable {
    private static final long serialVersionUID = -7727845679043804111L;

    public static final DepartmentComparator instance = new DepartmentComparator();

    @Override
    public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
        return Double.compare(o1._2(), o2._2());
    }

}
