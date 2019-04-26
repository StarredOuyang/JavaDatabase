package hw1;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A heap file stores a collection of tuples. It is also responsible for
 * managing pages. It needs to be able to manage page creation as well as
 * correctly manipulating pages when tuples are added or deleted.
 * 
 * @author Sam Madden modified by Doug Shook
 *
 */
public class HeapFile {
	public static final int PAGE_SIZE = 4096;
	private TupleDesc tupleDesc;
	private File file;
	

	long file_pos;
	
	int page_open;
	RandomAccessFile x;
	

	/**
	 * Creates a new heap file in the given location that can accept tuples of
	 * the given type
	 * 
	 * @param f
	 *            location of the heap file
	 * @param types
	 *            type of tuples contained in the file
	 */
	public HeapFile(File f, TupleDesc type) {
		// your code here

	
		page_open = 0;
		this.tupleDesc = type;
		this.file = f;
		try {
			
			file_pos = 0;
		} catch (Exception e) {

		}
	}

	public File getFile() {
		// your code here
		return file;
	}

	public TupleDesc getTupleDesc() {
		// your code here
		return tupleDesc;
	}

	/**
	 * Creates a HeapPage object representing the page at the given page number.
	 * Because it will be necessary to arbitrarily move around the file, a
	 * RandomAccessFile object should be used here.
	 * 
	 * @param id
	 *            the page number to be retrieved
	 * @return a HeapPage at the given page number
	 * @throws IOException
	 */
	public HeapPage readPage(int id) throws IOException {
		x = new RandomAccessFile(getFile(), "rws");
		//each page is 4096 bytes. 4096 * page num will give us the byte location in the file.
		x.seek(PAGE_SIZE * id);

		byte[] b = new byte[PAGE_SIZE];

		x.read(b);

		HeapPage result = new HeapPage(id, b, this.getId());
		x.close();
		return result;

	}

	/**
	 * Returns a unique id number for this heap file. Consider using the hash of
	 * the File itself.
	 * 
	 * @return
	 */
	public int getId() {
		// your code here
		return file.hashCode(); // 
	}

	/**
	 * Writes the given HeapPage to disk. Because of the need to seek through
	 * the file, a RandomAccessFile object should be used in this method.
	 * 
	 * @param p
	 *            the page to write to disk
	 * @throws IOException
	 */
	public void writePage(HeapPage p) throws IOException {
		x = new RandomAccessFile(getFile(), "rws");
		x.seek(PAGE_SIZE * page_open); //finds the open page(global var) to write to. for example if there are multiple pages but an empty slot before the last page

		byte[] b = p.getPageData();
		x.write(b);

		file_pos = x.getFilePointer();
x.close();
	}

	/**
	 * Adds a tuple. This method must first find a page with an open slot,
	 * creating a new page if all others are full. It then passes the tuple to
	 * this page to be stored. It then writes the page to disk (see writePage)
	 * 
	 * @param t
	 *            The tuple to be stored
	 * @return The HeapPage that contains the tuple
	 * @throws Exception
	 */
	public HeapPage addTuple(Tuple t) throws Exception {
		// your code here
		ArrayList<HeapPage> allpages = new ArrayList<>();

		for (int i = 0; i < this.getNumPages(); i++) {
			HeapPage current = this.readPage(i);

			if (current.emptySlots() != 0) {
				allpages.add(current);
				page_open = i;
			}
		}

		HeapPage newpage;
		// create new page if no empty pages.
		if (allpages.isEmpty()) {
			newpage = new HeapPage(this.getNumPages() + 1, new byte[PAGE_SIZE], this.getId());
			newpage.addTuple(t);
//increment the page number with the most recent open slot(only if not set in above for loop)
			page_open++;
		} else {
			newpage = allpages.get(0);
			newpage.addTuple(t);
		}

		this.writePage(newpage);

		return newpage;
	}
	//modified from above - does not write just returns page
	public HeapPage add_not_write_tuple(Tuple t) throws Exception {
		// your code here
		ArrayList<HeapPage> allpages = new ArrayList<>();

		for (int i = 0; i < this.getNumPages(); i++) {
			HeapPage current = this.readPage(i);

			if (current.emptySlots() != 0) {
				allpages.add(current);
				page_open = i;
			}
		}

		HeapPage newpage;
		// create new page if no empty pages.
		if (allpages.isEmpty()) {
			newpage = new HeapPage(this.getNumPages() + 1, new byte[PAGE_SIZE], this.getId());
			newpage.addTuple(t);
//increment the page number with the most recent open slot(only if not set in above for loop)
			page_open++;
		} else {
			newpage = allpages.get(0);
			newpage.addTuple(t);
		}

	

		return newpage;
	}

	/**
	 * This method will examine the tuple to find out where it is stored, then
	 * delete it from the proper HeapPage. It then writes the modified page to
	 * disk.
	 * 
	 * @param t
	 *            the Tuple to be deleted
	 * @throws Exception
	 */
	public HeapPage deleteTuple(Tuple t) throws Exception {
		// your code here

		// tuple id and page id
		int page_id = t.getPid(), slot_number = t.getId();
		HeapPage newpage = this.readPage(0); // start at first

		for (int i = 0; i < this.getNumPages(); i++) {
			HeapPage current = this.readPage(i);
			Iterator<Tuple> it = current.iterator();
			while (it.hasNext()) {
				if (it.next().equals(t)) {
					// remove and update slot info
					newpage = current;
					break;
				}

			}

		}
		newpage.deleteTuple(t);
	
		this.writePage(newpage);
		
		return newpage;

	}

	/**
	 * Returns an ArrayList containing all of the tuples in this HeapFile. It
	 * must access each HeapPage to do this (see iterator() in HeapPage)
	 * 
	 * @return
	 * @throws IOException
	 */
	public ArrayList<Tuple> getAllTuples() throws IOException {
		ArrayList<Tuple> alltuples = new ArrayList<>();
//go to each page number. get the iterator for that heappage and list all the elements in the iterator. append every tuple to an ArrayList 
		for (int i = 0; i < this.getNumPages(); i++) {
			HeapPage curr = this.readPage(i);
			Iterator<Tuple> it = curr.iterator();

			while (it.hasNext()) {
				Tuple x = it.next();
				alltuples.add(x);

			}

		}

		return alltuples;

	}

	/**
	 * Computes and returns the total number of pages contained in this HeapFile
	 * 
	 * @return the number of pages
	 */
	public int getNumPages() {
// each page is 4096 bytes. take total # of bytes in file and divide by 4096.
		int numPages = (int) Math.ceil(file.length() / PAGE_SIZE);

		return numPages;

	}
}
