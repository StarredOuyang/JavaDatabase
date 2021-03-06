package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * A class to perform various aggregations, by accepting one tuple at a time
 * @author Doug Shook
 *
 */
public class Aggregator {
	AggregateOperator o;
boolean groupBy;
TupleDesc td;
ArrayList<Tuple> tuples;

	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		this.o = o;
this.groupBy = groupBy;
this.td = td;
tuples = new ArrayList<>();

	}

	/**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 */
	public void merge(Tuple t) {
		tuples.add(t);
		
		
	}
	
	/**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 */
	public ArrayList<Tuple> getResults() {
		//your code here
		return tuples;
	}

}
