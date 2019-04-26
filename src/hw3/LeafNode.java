package hw3;

import java.util.ArrayList;

import hw1.RelationalOperator;

public class LeafNode implements Node {
	int degree;
	ArrayList<Entry> entries;
	LeafNode left_sibling, right_sibling;// sibling nodes
	private InnerNode parent;

	public LeafNode(int degree) {
		this.degree = degree;
		left_sibling = null;

		right_sibling = null;
		this.parent = parent;

		entries = new ArrayList<>();
	}

	public InnerNode getParent() {
		return parent;
	}

	public void setParent(InnerNode parent) {
		this.parent = parent;
	}

	public ArrayList<Entry> getEntries() {
		// your code here
		return entries;
	}

	public int getDegree() {
		// your code here
		return degree;
	}

	// inserting in sorted order
	public void insert(Entry e) {

		// for empty list
		if (entries.isEmpty()) {
			entries.add(e);
		} else {

			// field1.compare(LT,field2);
			for (int i = 0; i < entries.size(); i++) {
				if (e.getField().compare(RelationalOperator.LT, entries.get(i).getField())) {
					entries.add(i, e);
					return;

				}
			}
//** add to end for overflow, might not work
			entries.add(e);
		}

	}

	public void delete(Entry e) {
		for (int i = 0; i < entries.size(); i++) {
			if (e.equals(entries.get(i))) {
				entries.remove(i);
			}
		}
	}

	public boolean isLeafNode() {
		return true;
	}

}