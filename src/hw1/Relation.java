package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * This class provides methods to perform relational algebra operations. It will
 * be used to implement SQL queries.
 * 
 * @author Doug Shook
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	private TupleDesc td;

	public Relation(ArrayList<Tuple> l, TupleDesc td) {
		// your code here
		tuples = l;
		this.td = td;

	}

	/**
	 * This method performs a select operation on a relation
	 * 
	 * @param field
	 *            number (refer to TupleDesc) of the field to be compared, left side
	 *            of comparison
	 * @param op
	 *            the comparison operator
	 * @param operand
	 *            a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		// think this is correct

		ArrayList<Tuple> newtuples = new ArrayList<>();

		for (int i = 0; i < tuples.size(); i++) {
			Tuple x = tuples.get(i);
			// for type int
			if (operand.getType() == Type.INT) {
				IntField intfield = new IntField(x.getField(field));
				if (intfield.compare(op, operand)) {
					newtuples.add(x); // if operand is greater then add to tuple
				}
			} else {
				StringField stringfield = new StringField(x.getField(i));
				if (stringfield.compare(op, operand)) {
					newtuples.add(x);
				}

			}

		}
		return new Relation(newtuples, td);
	}

	/**
	 * This method performs a rename operation on a relation
	 * 
	 * @param fields
	 *            the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names
	 *            a list of new names. The order of these names is the same as the
	 *            order of field numbers in the field list
	 * @return
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names) {
		// your code here

		Type[] x = new Type[td.numFields()];
		String[] newnames = new String[td.numFields()];
		for (int i = 0; i < td.numFields(); i++) {
			x[i] = td.getType(i);
			if (fields.contains(i)) {
				newnames[i] = names.get(i);
			} else {
				newnames[i] = td.getFieldName(i);
			}

		}

		TupleDesc newtupleDesc = new TupleDesc(x, newnames);
		return new Relation(tuples, newtupleDesc);
	}

	/**
	 * This method performs a project operation on a relation
	 * 
	 * @param fields
	 *            a list of field numbers (refer to TupleDesc) that should be in the
	 *            result
	 * @return
	 */
	public Relation project(ArrayList<Integer> fields) {
		Type[] x = new Type[fields.size()];
		String[] newnames = new String[fields.size()];
		int count = 0;

		for (int i : fields) {
			newnames[count] = td.getFieldName(i);
			x[count++] = td.getType(i);
		}

		TupleDesc newtupleDesc = new TupleDesc(x, newnames);
		ArrayList<Tuple> newtuples = new ArrayList<>();
		for (int i = 0; i < tuples.size(); i++) {
			Tuple newt = new Tuple(newtupleDesc);
			count = 0;
			for (int j : fields) {
				newt.setField(count++, new IntField(tuples.get(i).getField(j)).toByteArray());
			}

			newtuples.add(newt);
		}

		return new Relation(newtuples, newtupleDesc);

	}

	/**
	 * This method performs a join between this relation and a second relation. The
	 * resulting relation will contain all of the columns from both of the given
	 * relations, joined using the equality operator (=)
	 * 
	 * @param other
	 *            the relation to be joined
	 * @param field1
	 *            the field number (refer to TupleDesc) from this relation to be
	 *            used in the join condition
	 * @param field2
	 *            the field number (refer to TupleDesc) from other to be used in the
	 *            join condition
	 * @return
	 */

	public Relation join(Relation other, int field1, int field2) {
		TupleDesc td2 = other.getDesc();
		TupleDesc td1 = this.getDesc();

		ArrayList<Tuple> theseTuples = this.getTuples();

		ArrayList<Tuple> otherTuple = other.getTuples();
		// need new tuple arraylist

		int newNum = td1.numFields() + td2.numFields();

		ArrayList<Tuple> newtuples = new ArrayList<>();
		Type[] newtypes = new Type[newNum];
		String[] newnames = new String[newNum];
		for (int i = 0; i < td1.numFields(); i++) {
			newtypes[i] = td.getType(i);
			newnames[i] = td.getFieldName(i);
		}

		for (int i = 0; i < td2.numFields(); i++) {
			newtypes[td1.numFields() + i] = td2.getType(i);
			newnames[td1.numFields() + i] = td2.getFieldName(i);

		}

		TupleDesc newtupleDesc = new TupleDesc(newtypes, newnames);

		// select all rows so long as columns match
		Tuple newt;

		for (int i = 0; i < theseTuples.size(); i++) {
			for (int j = 0; j < otherTuple.size(); j++) {
				// while(i<theseTuples.size() && j<otherTuple.size()){
				if (compareFields(theseTuples.get(i).getField(field1), otherTuple.get(j).getField(field2))) {
					newt = new Tuple(newtupleDesc);
					for (int k = 0; k < td1.numFields(); k++) {

						newt.setField(k, this.tuples.get(i).getField(k));

					}
					for (int k = 0; k < td1.numFields(); k++) {

						newt.setField(td1.numFields() + k, otherTuple.get(i).getField(k));

					}

					System.out.println();
					newtuples.add(newt);

				}
			}
		}

		return new Relation(newtuples, newtupleDesc);

		// return null;
	}

	public boolean compareFields(byte[] arr1, byte[] arr2) {
		if (arr1.length != arr2.length)
			return false;
		for (int i = 0; i < arr1.length; i++) {
			if (arr1[i] != arr2[i])
				return false;
		}
		return true;
	}

	/**
	 * Performs an aggregation operation on a relation. See the lab write up for
	 * details.
	 * 
	 * @param op
	 *            the aggregation operation to be performed
	 * @param groupBy
	 *            whether or not a grouping should be performed
	 * @return
	 */

	// *** MAKE SURE WE DO CHECK FOR STRINGS AND ALTERNATE process if need be,
	// for max/min/
	public Relation aggregate(AggregateOperator op, boolean groupBy) {
		Aggregator x = new Aggregator(op, groupBy, td);
		ArrayList<Tuple> newtuples = new ArrayList<>();
		TupleDesc newtupleDesc = null;
		switch (op) {

		case MAX:
			if (!groupBy) {

				Type[] newtypes = new Type[1];
				String[] newnames = new String[1];
				newtypes[0] = td.getType(0);
				newnames[0] = td.getFieldName(0);
				newtupleDesc = new TupleDesc(newtypes, newnames);
				Tuple maxtuple = new Tuple(newtupleDesc);

				// Type int***
				if (td.getType(0) == Type.INT) {

					int max = Integer.MIN_VALUE;
					IntField temp = null;
					for (int i = 0; i < tuples.size(); i++) {
						temp = new IntField(tuples.get(i).getField(0));
						if (temp.getValue() > max) {
							max = temp.getValue();
						}

					}
					IntField finalmax = new IntField(max);
					maxtuple.setField(0, finalmax.toByteArray());
					x.merge(maxtuple);
				}
				// strings to be compared lexographically
				else if (td.getType(0) == Type.STRING) {
					String max = "";// empty string is lexographically lowest
					StringField temp = null;
					for (int i = 0; i < tuples.size(); i++) {
						temp = new StringField(tuples.get(i).getField(0));
						if (temp.getValue().compareTo(max) > 0) {
							max = temp.getValue();
						}

					}
					StringField finalmax = new StringField(max);
					maxtuple.setField(0, finalmax.toByteArray());
					x.merge(maxtuple);

				}

			} else {
				// **** group by
				Type[] newtypes = new Type[2];
				String[] newnames = new String[2];
				newtypes[0] = td.getType(0);
				newnames[0] = td.getFieldName(0);
				newtypes[1] = td.getType(1);
				newnames[1] = td.getFieldName(1);

				newtupleDesc = new TupleDesc(newtypes, newnames);
				if (td.getType(1) == Type.INT && td.getType(0) == Type.INT) { // int type group name
					Map<Integer, Integer> map = new HashMap<Integer, Integer>();
					for (int i = 0; i < tuples.size(); i++) {
						IntField curr = new IntField(tuples.get(i).getField(0));
						if (map.containsKey(curr.getValue())) {
							int oldValue = map.get(curr.getValue());
							int newValue = new IntField(tuples.get(i).getField(1)).getValue();
							if (oldValue < newValue) {
								oldValue = newValue;
							}
							map.put(curr.getValue(), oldValue);
						} else {

							map.put(curr.getValue(), new IntField(tuples.get(i).getField(1)).getValue());
						}
					}

					for (Integer a1 : map.keySet()) {
						Tuple newt = new Tuple(newtupleDesc);
						newt.setField(0, new IntField(a1).toByteArray());
						newt.setField(1, new IntField(map.get(a1)).toByteArray());
						x.merge(newt);
					}
				} else if (td.getType(1) == Type.INT && td.getType(0) == Type.STRING) { // string type group name
					Map<String, Integer> map = new HashMap<String, Integer>();
					for (int i = 0; i < tuples.size(); i++) {
						StringField curr = new StringField(tuples.get(i).getField(0));
						if (map.containsKey(curr.getValue())) {
							int oldValue = map.get(curr.getValue());
							int newValue = new IntField(tuples.get(i).getField(1)).getValue();
							if (oldValue < newValue) {
								oldValue = newValue;
							}
							map.put(curr.getValue(), oldValue);
						} else {

							map.put(curr.getValue(), new IntField(tuples.get(i).getField(1)).getValue());
						}
					}

					for (String a1 : map.keySet()) {
						Tuple newt = new Tuple(newtupleDesc);
						newt.setField(0, new StringField(a1).toByteArray());
						newt.setField(1, new IntField(map.get(a1)).toByteArray());
						x.merge(newt);
					}
				} else if (td.getType(1) == Type.STRING && td.getType(0) == Type.STRING) { // string type MAX group by
					Map<String, String> map = new HashMap<String, String>();
					for (int i = 0; i < tuples.size(); i++) {
						StringField curr = new StringField(tuples.get(i).getField(0));
						if (map.containsKey(curr.getValue())) {
							String oldValue = map.get(curr.getValue());
							String newValue = new StringField(tuples.get(i).getField(1)).getValue();
							if (newValue.compareTo(oldValue) > 0) {
								oldValue = newValue;
							}
							map.put(curr.getValue(), oldValue);
						} else {

							map.put(curr.getValue(), new StringField(tuples.get(i).getField(1)).getValue());
						}
					}

					for (String a1 : map.keySet()) {
						Tuple newt = new Tuple(newtupleDesc);
						newt.setField(0, new StringField(a1).toByteArray());
						newt.setField(1, new StringField(map.get(a1)).toByteArray());
						x.merge(newt);
					}
				} else {// integer type group name
					Map<Integer, String> map = new HashMap<Integer, String>();
					for (int i = 0; i < tuples.size(); i++) {
						IntField curr = new IntField(tuples.get(i).getField(0));
						if (map.containsKey(curr.getValue())) {
							String oldValue = map.get(curr.getValue());
							String newValue = new StringField(tuples.get(i).getField(1)).getValue();
							if (newValue.compareTo(oldValue) > 0) {
								oldValue = newValue;
							}
							map.put(curr.getValue(), oldValue);
						} else {

							map.put(curr.getValue(), new StringField(tuples.get(i).getField(1)).getValue());
						}
					}

					for (Integer a1 : map.keySet()) {
						Tuple newt = new Tuple(newtupleDesc);
						newt.setField(0, new IntField(a1).toByteArray());
						newt.setField(1, new StringField(map.get(a1)).toByteArray());
						x.merge(newt);
					}
				}
			}

			break;

		case MIN:
			if (!groupBy) {// without groupby
				Type[] newtypes = new Type[1];
				String[] newnames = new String[1];
				newtypes[0] = td.getType(0);
				newnames[0] = td.getFieldName(0);
				newtupleDesc = new TupleDesc(newtypes, newnames);
				Tuple mintuple = new Tuple(newtupleDesc);
				if (td.getType(0) == Type.INT) { // type int for min.
					int min = Integer.MAX_VALUE;
					IntField temp = null;
					for (int i = 0; i < tuples.size(); i++) {
						temp = new IntField(tuples.get(i).getField(0));
						if (temp.getValue() < min) {
							min = temp.getValue();
						}
					}
					IntField finalmax = new IntField(min);
					mintuple.setField(0, finalmax.toByteArray());
					x.merge(mintuple);
				} else if (td.getType(0) == Type.STRING) { // type string for

					String min = "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ";
					// empty string is lexographically lowest
					StringField temp = null;
					if (tuples.size() != 0)
						min = new StringField(tuples.get(0).getField(0)).getValue();

					for (int i = 0; i < tuples.size(); i++) {
						temp = new StringField(tuples.get(i).getField(0));
						if (temp.getValue().compareTo(min) < 0) {
							min = temp.getValue();
						}

					}
					StringField finalmin = new StringField(min);
					mintuple.setField(0, finalmin.toByteArray());
					x.merge(mintuple);

				}

			} else { // Groupby == true
				Type[] newtypes = new Type[2];
				String[] newnames = new String[2];
				newtypes[0] = td.getType(0);
				newnames[0] = td.getFieldName(0);
				newtypes[1] = td.getType(1);
				newnames[1] = td.getFieldName(1);
				//
				newtupleDesc = new TupleDesc(newtypes, newnames);
				if (td.getType(1) == Type.INT && td.getType(0) == Type.INT) {
					Map<Integer, Integer> map = new HashMap<Integer, Integer>(); // a1 key 1, a2 value
					for (int i = 0; i < tuples.size(); i++) {

						IntField curr = new IntField(tuples.get(i).getField(0));

						if (map.containsKey(curr.getValue())) {
							int oldValue = map.get(curr.getValue());
							int newValue = new IntField(tuples.get(i).getField(1)).getValue();
							if (oldValue > newValue) {
								oldValue = newValue;
							}
							map.put(curr.getValue(), oldValue);
						} else {

							map.put(curr.getValue(), new IntField(tuples.get(i).getField(1)).getValue());
						}
					}

					for (Integer a1 : map.keySet()) {
						Tuple newt = new Tuple(newtupleDesc);
						newt.setField(0, new IntField(a1).toByteArray());
						newt.setField(1, new IntField(map.get(a1)).toByteArray());
						x.merge(newt);
					}

				} else if (td.getType(1) == Type.INT && td.getType(0) == Type.STRING) {
					Map<String, Integer> map = new HashMap<String, Integer>(); // a1 key 1, a2 value
					for (int i = 0; i < tuples.size(); i++) {

						StringField curr = new StringField(tuples.get(i).getField(0));

						if (map.containsKey(curr.getValue())) {
							int oldValue = map.get(curr.getValue());
							int newValue = new IntField(tuples.get(i).getField(1)).getValue();
							if (oldValue > newValue) {
								oldValue = newValue;
							}
							map.put(curr.getValue(), oldValue);
						} else {

							map.put(curr.getValue(), new IntField(tuples.get(i).getField(1)).getValue());
						}
					}

					for (String a1 : map.keySet()) {
						Tuple newt = new Tuple(newtupleDesc);
						newt.setField(0, new StringField(a1).toByteArray());
						newt.setField(1, new IntField(map.get(a1)).toByteArray());
						x.merge(newt);
					}

				} else if (td.getType(1) == Type.STRING && td.getType(0) == Type.STRING) {// string type MIN group by
					Map<String, String> map = new HashMap<String, String>(); // a1 key 1, a2 value
					for (int i = 0; i < tuples.size(); i++) {
						System.out.println(tuples.get(i).getField(0));
						StringField curr = new StringField(tuples.get(i).getField(0));

						if (map.containsKey(curr.getValue())) {
							String oldValue = map.get(curr.getValue());
							String newValue = new StringField(tuples.get(i).getField(1)).getValue();
							if (newValue.compareTo(oldValue) < 0) {
								oldValue = newValue;
							}
							map.put(curr.getValue(), oldValue);
						} else {

							map.put(curr.getValue(), new StringField(tuples.get(i).getField(1)).getValue());
						}
					}

					for (String a1 : map.keySet()) {
						Tuple newt = new Tuple(newtupleDesc);
						newt.setField(0, new StringField(a1).toByteArray());
						newt.setField(1, new StringField(map.get(a1)).toByteArray());
						x.merge(newt);
					}
				} else {
					Map<Integer, String> map = new HashMap<Integer, String>(); // a1 key 1, a2 value
					for (int i = 0; i < tuples.size(); i++) {
						IntField curr = new IntField(tuples.get(i).getField(0));
						if (map.containsKey(curr.getValue())) {
							String oldValue = map.get(curr.getValue());
							String newValue = new StringField(tuples.get(i).getField(1)).getValue();
							if (newValue.compareTo(oldValue) < 0) {
								oldValue = newValue;
							}
							map.put(curr.getValue(), oldValue);
						} else {

							map.put(curr.getValue(), new StringField(tuples.get(i).getField(1)).getValue());
						}
					}

					for (Integer a1 : map.keySet()) {
						Tuple newt = new Tuple(newtupleDesc);
						newt.setField(0, new IntField(a1).toByteArray());
						newt.setField(1, new StringField(map.get(a1)).toByteArray());
						x.merge(newt);
					}
				}
			}

			break;
		case AVG:
			if (!groupBy) {
				Type[] newtypes = new Type[1]; // single column? no string for

				String[] newnames = new String[1];
				newtypes[0] = td.getType(0);
				newnames[0] = td.getFieldName(0);
				newtupleDesc = new TupleDesc(newtypes, newnames);
				Tuple avgtuple = new Tuple(newtupleDesc);
				int avg = 0;
				IntField temp = null;
				for (int i = 0; i < tuples.size(); i++) {
					temp = new IntField(tuples.get(i).getField(0));

					avg += temp.getValue();
				}
				IntField finalsum = new IntField(avg / tuples.size());
				avgtuple.setField(0, finalsum.toByteArray());
				x.merge(avgtuple);
			} else {
				// do avg groupBy
				Type[] newtypes = new Type[2]; // single column? no string for
				String[] newnames = new String[2];
				newtypes[0] = td.getType(0);
				newnames[0] = td.getFieldName(0);
				newtypes[1] = td.getType(1);
				newnames[1] = td.getFieldName(1);
				if (td.getType(0) == Type.INT) {// int type group name
					newtupleDesc = new TupleDesc(newtypes, newnames);
					int count = 0;
					Map<Integer, Integer> map = new HashMap<Integer, Integer>(); // a1 key 1, a2 value
					for (int i = 0; i < tuples.size(); i++) {
						IntField curr = new IntField(tuples.get(i).getField(0));
						if (map.containsKey(curr.getValue())) {
							int currsum = map.get(curr.getValue()) * count;
							count += 1;
							currsum += (new IntField(tuples.get(i).getField(1)).getValue());
							map.put(curr.getValue(), currsum / count);

						} else {
							map.put(curr.getValue(), new IntField(tuples.get(i).getField(1)).getValue());
						}
					}

					for (Integer a1 : map.keySet()) {
						Tuple newt = new Tuple(newtupleDesc);
						newt.setField(0, new IntField(a1).toByteArray());
						newt.setField(1, new IntField(map.get(a1)).toByteArray());
						x.merge(newt);
					}
				} else {// string type group name
					newtupleDesc = new TupleDesc(newtypes, newnames);
					int count = 0;
					Map<String, Integer> map = new HashMap<String, Integer>(); // a1 key 1, a2 value
					for (int i = 0; i < tuples.size(); i++) {
						StringField curr = new StringField(tuples.get(i).getField(0));
						if (map.containsKey(curr.getValue())) {
							int currsum = map.get(curr.getValue()) * count;
							count += 1;
							currsum += (new IntField(tuples.get(i).getField(1)).getValue());
							map.put(curr.getValue(), currsum / count);

						} else {
							map.put(curr.getValue(), new IntField(tuples.get(i).getField(1)).getValue());
						}
					}

					for (String a1 : map.keySet()) {
						Tuple newt = new Tuple(newtupleDesc);
						newt.setField(0, new StringField(a1).toByteArray());
						newt.setField(1, new IntField(map.get(a1)).toByteArray());
						x.merge(newt);
					}
				}
			}
			break;

		case SUM:
			if (!groupBy) {
				Type[] newtypes = new Type[1]; // single column no string for

				String[] newnames = new String[1];
				newtypes[0] = td.getType(0);
				newnames[0] = td.getFieldName(0);
				newtupleDesc = new TupleDesc(newtypes, newnames);
				Tuple sumtuple = new Tuple(newtupleDesc);
				int sum = 0;
				IntField temp = null;
				for (int i = 0; i < tuples.size(); i++) {
					temp = new IntField(tuples.get(i).getField(0));

					sum += temp.getValue();
				}
				IntField finalsum = new IntField(sum);
				sumtuple.setField(0, finalsum.toByteArray());
				x.merge(sumtuple);
			} else {
				// do sum groupBy

				Type[] newtypes = new Type[2];

				String[] newnames = new String[2];
				newtypes[0] = td.getType(0);
				newnames[0] = td.getFieldName(0);
				newtypes[1] = td.getType(1);
				newnames[1] = td.getFieldName(1);
				if (td.getType(0) == Type.INT) {// int type group name

					newtupleDesc = new TupleDesc(newtypes, newnames);

					Map<Integer, Integer> map = new HashMap<Integer, Integer>(); // a1 key 1, a2 value

					for (int i = 0; i < tuples.size(); i++) {
						IntField curr = new IntField(tuples.get(i).getField(0));
						if (map.containsKey(curr.getValue())) {
							int currsum = map.get(curr.getValue());
							currsum += (new IntField(tuples.get(i).getField(1)).getValue());
							map.put(curr.getValue(), currsum);
						} else {

							map.put(curr.getValue(), new IntField(tuples.get(i).getField(1)).getValue());
						}
					}
					for (Integer a1 : map.keySet()) {
						Tuple newt = new Tuple(newtupleDesc);
						newt.setField(0, new IntField(a1).toByteArray());
						newt.setField(1, new IntField(map.get(a1)).toByteArray());
						x.merge(newt);
					}
				} else {// string type group name
					newtupleDesc = new TupleDesc(newtypes, newnames);

					Map<String, Integer> map = new HashMap<String, Integer>(); // a1 key 1, a2 value

					for (int i = 0; i < tuples.size(); i++) {
						StringField curr = new StringField(tuples.get(i).getField(0));
						if (map.containsKey(curr.getValue())) {
							int currsum = map.get(curr.getValue());
							currsum += (new IntField(tuples.get(i).getField(1)).getValue());
							map.put(curr.getValue(), currsum);
						} else {

							map.put(curr.getValue(), new IntField(tuples.get(i).getField(1)).getValue());
						}
					}
					for (String a1 : map.keySet()) {
						Tuple newt = new Tuple(newtupleDesc);
						newt.setField(0, new StringField(a1).toByteArray());
						newt.setField(1, new IntField(map.get(a1)).toByteArray());
						x.merge(newt);
					}
				}

			}
			break;

		case COUNT: // type int or string does not matter.
			if (!groupBy) {
				Type[] newtypes = new Type[1];
				String[] newnames = new String[1];
				newtypes[0] = td.getType(0);
				newnames[0] = td.getFieldName(0);
				newtupleDesc = new TupleDesc(newtypes, newnames);
				Tuple counttuple = new Tuple(newtupleDesc);

				IntField finalsum = new IntField(tuples.size());
				counttuple.setField(0, finalsum.toByteArray());
				x.merge(counttuple);
			} else {
				// do count groupBy
				Type[] newtypes = new Type[2];
				String[] newnames = new String[2];
				newtypes[0] = td.getType(0);
				newnames[0] = td.getFieldName(0);
				newtypes[1] = td.getType(1);
				newnames[1] = td.getFieldName(1);

				newtupleDesc = new TupleDesc(newtypes, newnames);
				if (td.getType(1) == Type.INT && td.getType(0) == Type.INT) { // int type group name
					Map<Integer, Integer> map = new HashMap<Integer, Integer>();
					for (int i = 0; i < tuples.size(); i++) {
						int count = 1;
						IntField curr = new IntField(tuples.get(i).getField(0));
						if (map.containsKey(curr.getValue())) {

							count = map.get(curr.getValue());
							count += 1;
							map.put(curr.getValue(), count);

						} else {
							map.put(curr.getValue(), count);
						}
					}
					for (Integer a1 : map.keySet()) {
						Tuple newt = new Tuple(newtupleDesc);
						newt.setField(0, new IntField(a1).toByteArray());
						newt.setField(1, new IntField(map.get(a1)).toByteArray());
						x.merge(newt);
					}
				} else if (td.getType(1) == Type.INT && td.getType(0) == Type.STRING) { // STRING type group name
					Map<String, Integer> map = new HashMap<String, Integer>();
					for (int i = 0; i < tuples.size(); i++) {
						int count = 1;
						StringField curr = new StringField(tuples.get(i).getField(0));
						if (map.containsKey(curr.getValue())) {

							count = map.get(curr.getValue());
							count += 1;
							map.put(curr.getValue(), count);

						} else {
							map.put(curr.getValue(), count);
						}
					}
					for (String a1 : map.keySet()) {
						Tuple newt = new Tuple(newtupleDesc);
						newt.setField(0, new StringField(a1).toByteArray());
						newt.setField(1, new IntField(map.get(a1)).toByteArray());
						x.merge(newt);
					}
				}

			}
			break;

		}

		// x.merge(t);

		return new Relation(x.getResults(), newtupleDesc);

	}

	public TupleDesc getDesc() {
		// your code here
		return td;
	}

	public ArrayList<Tuple> getTuples() {

		return tuples;
	}

	/**
	 * Returns a string representation of this relation. The string representation
	 * should first contain the TupleDesc, followed by each of the tuples in this
	 * relation
	 */
	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append(td.toString());
		Iterator<Tuple> it = tuples.iterator();
		while (it.hasNext()) {
			result.append(it.next().toString());
		}
		return result.toString();
	}
}