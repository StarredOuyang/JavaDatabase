package hw1;

import java.util.HashMap;
import java.util.Map;

/**
 * This class represents a tuple that will contain a single row's worth of
 * information from a table. It also includes information about where it is
 * stored
 * 
 * @author Sam Madden modified by Doug Shook
 *
 */

public class Tuple {

	/**
	 * Creates a new tuple with the given description
	 * 
	 * @param t
	 *            the schema for this tuple
	 */
	TupleDesc td;
	private Map<String, byte[]> TupleMap;
	private int pid;
	private int id;

	public Tuple(TupleDesc t) {
		TupleMap = new <String, byte[]>HashMap();
		td = t;

	}

	public TupleDesc getDesc() {

		return td;
	}

	/**
	 * retrieves the page id where this tuple is stored
	 * 
	 * @return the page id of this tuple
	 */
	public int getPid() {
		// your code here
		return pid;
	}

	public void setPid(int pid) {
		this.pid = pid;
	}

	/**
	 * retrieves the tuple (slot) id of this tuple
	 * 
	 * @return the slot where this tuple is stored
	 */
	public int getId() {
		// your code here
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public void setDesc(TupleDesc td) {
		this.td = td;
	}

	/**
	 * Stores the given data at the i-th field
	 * 
	 * @param i
	 *            the field number to store the data
	 * @param v
	 *            the data
	 */
	public void setField(int i, byte[] v) {
		TupleMap.put(td.getFieldName(i), v);
	}

	public byte[] getField(int i) {

		return TupleMap.get(td.getFieldName(i));
	}

	/**
	 * Creates a string representation of this tuple that displays its contents. You
	 * should convert the binary data into a readable format (i.e. display the ints
	 * in base-10 and convert the String columns to readable text).
	 */
	public String toString() {
		String result = "";
		for (int i = 0; i < td.numFields(); i++) {

			result += "Field - " + td.getFieldName(i);
			if (td.getType(i) == Type.INT) {
				result += "  Value: " + java.nio.ByteBuffer.wrap(TupleMap.get(td.getFieldName(i))).getInt() + "\n";

			} else {// String:

				result += "  Value: " + new String(TupleMap.get(td.getFieldName(i))) + "\n ";

			}
		}

		return result;
	}
}
