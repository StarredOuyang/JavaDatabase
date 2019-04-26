package hw3;

import java.util.ArrayList;

import hw1.Field;
import hw1.RelationalOperator;

public class BPlusTree {
	Node root; // ? shoud just be Node to cast to either once children added?
	Node leftchild;
	Node rightchild;
	int degree;

	public BPlusTree(int degree) {
		this.degree = degree;
		root = new LeafNode(degree);
		leftchild = null;
		rightchild = null;
	}

	public LeafNode search(Field f) {
		Node curr = root;
		while (!curr.isLeafNode()) {
			// bin search on current innernode key value
			ArrayList<Field> keys = ((InnerNode) curr).getKeys();
			int l = 0, h = keys.size() - 1;

			// modified bin search b/c returns index where node would go\
			// Algo:
			/*
			 * If search key < key_0, then repeat the search process on the first branch of
			 * current node. If search key >= key_last, then repeat the search process on
			 * the last branch of current node. Otherwise, find the first key_i >= key, and
			 * repeat the search process on the (i+1) branch of current node.
			 */
			int mid = (l + h) / 2;
			while (h > l) {// ******* h>=l??
				mid = (l + h) / 2;
				if (f.compare(RelationalOperator.LTE, keys.get(0))) {
					curr = ((InnerNode) curr).getChildren().get(0);
					if (curr.isLeafNode())
						break; // if leaf node then break, else keep searching
								// for leaf
				}
				if (f.compare(RelationalOperator.GT, keys.get(keys.size() - 1))) {
					curr = ((InnerNode) curr).getChildren().get(keys.size() - 1);
					if (curr.isLeafNode())
						break; // if leaf node then break, else keep searching
								// for leaf
				}

			}

			curr = ((InnerNode) curr).getChildren().get(mid);

		}
		// proper binary search, finds treenode or returns null
		ArrayList<Entry> entries = ((LeafNode) curr).getEntries();
		int l = 0, h = entries.size() - 1;

		// modified bin search b/c returns index where node would go
		int mid = (l + h) / 2;
		while (h >= l) {
			mid = (l + h) / 2;
			// boundary hit
			// pseudocode: search for entry I such that K(i-1)<k<=k(i)
			// key(mid-1)< field f <= key(mid)
			if (entries.get(mid).getField().compare(RelationalOperator.EQ, f))
				return (LeafNode) curr;

			// now check to go left or right
			else if (f.compare(RelationalOperator.GT, entries.get(mid).getField())) {
				l = mid + 1;
			} else {
				h = mid - 1;
			}
		}

		if (entries.get(mid).getField().compare(RelationalOperator.EQ, f))
			return (LeafNode) curr;

		return null; // return null if not found
	}

	// rather than returning leafnode if exists, find leafnode where it would go.
	// same code as
	// above search but second run through leafNode entries not performed
	
	
	
	public LeafNode modifiedsearch(Field f) {
		Node curr = root;
		Node prev = curr;
		while (!curr.isLeafNode()) {
			 prev = curr;
			// bin search on current innernode key value
			ArrayList<Field> keys = ((InnerNode) curr).getKeys();
			int l = 0, h = keys.size() - 1;

			// modified bin search b/c returns index where node would go\
			// Algo:
			/*
			 * If search key < key_0, then repeat the search process on the first branch of
			 * current node. If search key >= key_last, then repeat the search process on
			 * the last branch of current node. Otherwise, find the first key_i >= key, and
			 * repeat the search process on the (i+1) branch of current node.
			 */
			int mid = (l + h) / 2;
			while (h >= l) {// ******* h>=l??
				mid = (l + h) / 2;
				if(curr.isLeafNode()) return (LeafNode) curr;
				if (f.compare(RelationalOperator.LTE, ((InnerNode)curr).keys.get(0))) {
					curr = ((InnerNode) curr).getChildren().get(0);
					
					//seee?
//					Node min = ((InnerNode) curr).getChildren().get(0);
//					for(Node n : ((InnerNode) curr).getChildren()){
//						if(((InnerNode)n).keys.get(0).compare(RelationalOperator.LT, ((InnerNode)min).keys.get(0))){
//							min = n;
//						}
//						
//					}
					
					
					if (curr.isLeafNode())
						break; // if leaf node then break, else keep searching
								// for leaf
				}
			
				
				if(!curr.isLeafNode()){ 
					if (f.compare(RelationalOperator.GT, ((InnerNode)curr).keys.get(keys.size() - 1))) {
						curr = ((InnerNode) curr).getChildren().get((1));
						if (curr.isLeafNode())
							break; // if leaf node then break, else keep searching
									// for leaf
					}	
				}
				if (f.compare(RelationalOperator.GT, ((InnerNode)curr).keys.get(keys.size() - 1))) {
					curr = ((InnerNode) curr).getChildren().get(keys.size() - 1);
					if (curr.isLeafNode())
						break; // if leaf node then break, else keep searching
								// for leaf
				}
				curr = ((InnerNode) curr).getChildren().get(mid);
			}

			

		}
		//((LeafNode)curr).setParent((InnerNode)prev);
		return (LeafNode) curr;
	}
	
//	public void sort_inner_children(InnerNode n){
//		ArrayList<Node> list = n.getChildren();
//		
//		
//	}

	public void insert(Entry e) {
		//do search, if not null then we can insert, else already exists***************************
		
		
		if (root.isLeafNode()) {
			// this is specifically for special root case first split
			if (((LeafNode) root).entries.size() >= degree) {//
				// take middle value of entry, make that innernode,split
				// leafnode into 2 set root to innernode and 2 children to 2
				// split leafnodes
				
				((LeafNode)root).insert(e);//see hoe behaves, intentional overflow

				// get middle value add to new InnerNode key
				int low = 0, high = ((LeafNode) root).getEntries().size() - 1;
				int mid = (low + high) / 2;
				// new parent InnerNode
				InnerNode temp = new InnerNode(degree);
				// get middle entry value, get key from that, add to new Key
				// list of parent
				temp.insertKey(((LeafNode) root).getEntries().get(mid).getField());

				// now make two child leaf nodes.
				// Left:
				LeafNode left = new LeafNode(degree); // holds 0 - (mid-1)
				Entry y = ((LeafNode) root).getEntries().get(0);
				for (int i = 0; i <= mid; i++)
					left.insert(((LeafNode) root).getEntries().get(i));
				// Right:
				LeafNode right = new LeafNode(degree);
				for (int i = mid+1; i < ((LeafNode) root).getEntries().size(); i++)
					right.insert(((LeafNode) root).getEntries().get(i));

				root = temp;
				left.right_sibling = right;
				left.setParent((InnerNode) root);
				right.setParent((InnerNode) root);
				ArrayList<Node> children = new ArrayList<>();
				children.add(left);
				children.add(right);
				((InnerNode) root).setChildren(children);

			} else { // then root is leafnode and not yet full
				((LeafNode) root).insert(e);
			}

		} else {// Root is NOT leafnode, i.e has children
			LeafNode leaf = modifiedsearch(e.getField());

			if (leaf.getEntries().size() < leaf.getDegree()) {// can insert. leafNode insert handles sorting
				leaf.insert(e);
			}
			/*
			 * Otherwise, split the leaf node. 1)Allocate a new leaf node and move half keys
			 * to the new node. 2)Insert the new leaf's smallest key into the parent node.
			 * 3)If the parent is full, split it too, repeat the split process above until a
			 * parent is found that need not split. 4)If the root splits, create a new root
			 * which has one key and two children.
			 */
			else if (leaf.getEntries().size() == leaf.getDegree()) {
				// largest val ofnwe leafnode(middle of total entries before split) passed to
				// InnerNode

				// splitLeaf should not be recursive, might call call split InnerNode(which will
				// be recursive. base).
				// inner node: base rasse if root fulland if has no parent ceate new root
				// this will be recursive
				leaf.insert(e);
				ArrayList<Entry> entries = leaf.getEntries();
				ArrayList<Entry> entry1 = new ArrayList<>();
				ArrayList<Entry> entry2 = new ArrayList<>();
				int mid = (entries.size() % 2 == 0 ? entries.size() / 2 : entries.size() / 2 + 1);
				// split entries - if odd add extra val to left
				for (int i = 0; i < mid; i++) {
					entry1.add(entries.get(i));
				}
				for (int i = mid; i < entries.size(); i++) {
					entry2.add(entries.get(i));
				}
				
				InnerNode parent = leaf.getParent();
			//parent.parent = (InnerNode) root;
				
				LeafNode newLeaf = new LeafNode(degree);
				newLeaf.entries = entry1;
				
				
				leaf.entries = entry2;
				
				newLeaf.right_sibling = leaf;
				ArrayList<Node> newchildren = parent.getChildren();
				newchildren.add(newLeaf);//**
				parent.setChildren(newchildren);
				
				
				   newLeaf.setParent(parent);
	                int size = newLeaf.getEntries().size();
	                Field key = newLeaf.getEntries().get(size - 1).getField();
	                if (parent.getKeys().size() < parent.degree) {// no split on innernode needed
	                	leaf.setParent(parent);
	                    parent.insertKey(key);
	                } else {
	                	
	                	
	                	if(parent==root){
	                		
	                		
	                	splitInner(parent, key); //root may or may not be split below
	                    
	                	//parent.keys = ((InnerNode) root).getKeys();//**?
	                    if(((InnerNode)root).getKeys().size()>degree){ //case where root must be split
	                    
	                    int len = ((InnerNode)root).getKeys().size()-1;
	        			InnerNode newroot = new InnerNode(degree);
	        			newroot.insertKey((((InnerNode) root).getKeys().get(len/2)));
	        			
	        			ArrayList<Node> totalchildren = ((InnerNode)root).getChildren();
	        			ArrayList<Node> leftchildren = new ArrayList<>();
	        			ArrayList<Node> rightchildren = new ArrayList<>();
	        			//every leaf node less less than or equal to root goes to left innernode.
	        			//else goes to right innernode
	        			
	        			for(Node n : totalchildren){ //think they will be leafnodes in this case
	        				if(((LeafNode)n).entries.get(0).getField().compare(RelationalOperator.LTE, newroot.keys.get(0))){
	        					leftchildren.add(n);
	        				}else{
	        					rightchildren.add(n);
	        				}
	        			}
	        			
	        			
	        	
	        		//	leaf.setParent((InnerNode) root);
	    				
	    				//   newLeaf.setParent((InnerNode) root);
	        		InnerNode leftchild = new InnerNode(degree);
	        		InnerNode rightchild = new InnerNode(degree);
	        		leftchild.insertKey(((InnerNode) root).getKeys().get(0));
	        		leftchild.setChildren(leftchildren);
	        		rightchild.insertKey(((InnerNode) parent).getKeys().get(len));
	        	
	        	
	        		rightchild.setChildren(rightchildren);
	        		
	        	
	        		
	        	
//	        		
	        		ArrayList<Node> children = new ArrayList<>();
	        		children.add(leftchild);
//	        		
	        		//parent.keys = newroot.getKeys();
	        		
	        		children.add(rightchild);
	        		newroot.setChildren(children);
	        		root = newroot;
	        		leftchild.parent = (InnerNode) root;
	        		rightchild.parent = (InnerNode) root;
	        		newLeaf.setParent(leftchild);
	        		//leaf.setParent(rightchild);
	        		
	        		//root still has access to leafnodes keep in mind
        		
	        		
	                    }
	                }else{
	                	inserthelper(parent,key);
	                	 
	                	
		        		
	                }
	                }

            }

            // work on InnerNode and Leaf Node split (both cases may occur).

            // case where InnerNode needs to be split

        }
    }

	// recursive innernode call. base case if hit root or parent node does not need
	// to be split
	// try to add to2
	/*
	 * Insert the new leaf's smallest key into the parent node.
If the parent is full, split it too, repeat the split process above until a parent is found that need not split.
If the root splits, create a new root which has one key and two children.
	 */
	private void inserthelper(InnerNode node, Field key){
		
		if(node==root) return;
		if(node.keys.size()<degree){
			node.insertKey(key);
			return;
		}
		inserthelper( node.parent,  key);
	}

	
	public void splitInner(InnerNode node, Field key) {
		
	//	if(parent==root && parent.getKeys().size() >= parent.degree ){
		//	InnerNode newroot = new InnerNode(degree);
			
		//}
		
		
		int splitIndex;
		InnerNode newNode = new InnerNode(degree);
		ArrayList<Field> temp = new ArrayList<>();
		InnerNode newRoot = new InnerNode(degree);// do we need to create a new root as innerNode?
		node.insertKey(key);// first add then split, num of keys > degree at this point
		temp.addAll(node.getKeys());
		if (node.getKeys().size() % 2 == 0){
			splitIndex = node.getKeys().size() / 2;
		}
		else{
			splitIndex = (node.getKeys().size() + 1) / 2;
		}
		for (int i = splitIndex; i < degree; i++) {
			newNode.insertKey(node.getKeys().get(i));// set right parent keys
			temp.remove(i);
		}
		node.keys = temp; // set left parent keys
		int mid = splitIndex - 1;// mid key need to pop up to root
		newRoot.insertKey(temp.get(mid)); // set new root
		ArrayList<Node> children = new ArrayList<>();
		children.add(node);
		children.add(newNode);
		newRoot.setChildren(children);// set children for new root
		// newNode.setChildren(); dont know how to set children for new right parent
	node.parent = newRoot; //***? 
	newNode.parent = newRoot;
	}

	public void delete(Entry e) {
		// your code here
		if (root.isLeafNode()) {
			// if the leaf node is at least half-full, done!
			if (((LeafNode) root).entries.size() == degree || ((LeafNode) root).entries.size() >= degree / 2) {
				((LeafNode) root).delete(e);
			} else {

			}
		}

	}

	public Node getRoot() {
		// your code here
		return root;
	}

}