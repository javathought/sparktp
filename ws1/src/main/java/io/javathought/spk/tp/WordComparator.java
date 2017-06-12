package io.javathought.spk.tp;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

public class WordComparator implements Comparator<Tuple2<String, Integer>>, Serializable {

	@Override
	public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
		
		return o1._2.compareTo(o2._2);
	}

}
