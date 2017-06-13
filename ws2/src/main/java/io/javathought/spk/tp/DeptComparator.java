package io.javathought.spk.tp;

import scala.Serializable;
import scala.Tuple2;

/**
 * Created by user on 13/06/17.
 */
public class DeptComparator implements java.util.Comparator<scala.Tuple2<String, Double>>, Serializable {
    @Override
    public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
        return o1._2.compareTo(o2._2);
    }
}
