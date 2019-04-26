package hw3;

import java.util.ArrayList;
import java.util.Collections;

import hw1.Field;
import hw1.RelationalOperator;

public class InnerNode implements Node {
	int degree;
	
	ArrayList<Field> keys;
	private ArrayList<Node> children; // constraint of degree+1 *********
InnerNode parent;
	public InnerNode(int degree) {
	parent = null;
		this.degree = degree;
		keys = new ArrayList<>();
		children = new ArrayList<>();
	}

	public void insertKey(Field key) { // will have to split inner node if keys.size()==degree
		// no constarint for size of keys?
		if (keys.isEmpty()) {
			keys.add(key);
		} else {

			for (int i = 0; i < keys.size(); i++) {
				if (key.compare(RelationalOperator.LT, keys.get(i))) {
					keys.add(i, key);
					break;
				}
			}
			
		}

	}

	public void setChildren(ArrayList<Node> children) {
		//sort these children by the first value of their entry
	 if(children.get(0).isLeafNode()){
		 for(int i= 0;i<children.size();i++){
			 for(int j = i;j<children.size();j++){
				 LeafNode a = (LeafNode) children.get(i);
				 LeafNode b = (LeafNode) children.get(j);
				 if(a.entries.get(0).getField().compare(RelationalOperator.GT, b.entries.get(0).getField())){
					 Collections.swap(children, i, j);
				 }
			 }
			 
			 
		 }
	 }
		
		
		
		this.children = children;
		//sort children based on first value of each 
		
	
		
			
		
	}

	public ArrayList<Field> getKeys() {
		// your code here
		return keys;
	}

	public ArrayList<Node> getChildren() {
		// your code here
		return children;
	}

	public int getDegree() {
		// your code here
		return degree;
	}

	public boolean isLeafNode() {
		return false;
	}

}