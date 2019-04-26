package hw1;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class HeapPage {

	private int id;
	private byte[] header;
	private Tuple[] tuples;
	private TupleDesc td;
	private int numSlots;
	private int tableId;
//boolean read_locked and boolean write_locked??
	public HeapPage(int id, byte[] data, int tableId) throws IOException {
		this.id = id;
		this.tableId = tableId;

		this.td = Database.getCatalog().getTupleDesc(this.tableId);
		this.numSlots = getNumSlots();
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

		// allocate and read the header slots of this page
		header = new byte[getHeaderSize()];
		for (int i = 0; i < header.length; i++)
			header[i] = dis.readByte();

		try {
			// allocate and read the actual records of this page
			tuples = new Tuple[numSlots];
			for (int i = 0; i < tuples.length; i++)
				tuples[i] = readNextTuple(dis, i);
		} catch (NoSuchElementException e) {
			e.printStackTrace();
		}
		dis.close();
	}

	public int getId() {
		// your code here
		return id;
	}

	/**
	 * Computes and returns the total number of slots that are on this page
	 * (occupied or not). Must take the header into account!
	 * 
	 * @return number of slots on this page
	 */
	public int getNumSlots() {
		
		int slotSize = td.getSize();
		int slotNum = (int) Math.floor((8 * HeapFile.PAGE_SIZE) / (8 * slotSize + 1));
		// total bits / slot size + 1 bit in header

		return slotNum;
	}

	/**
	 * Computes the size of the header. Headers must be a whole number of bytes
	 * (no partial bytes)
	 * 
	 * @return size of header in bytes
	 */
	private int getHeaderSize() {
		// your code here
		return (int) Math.ceil((double) numSlots / 8);
	}

	/**
	 * Checks to see if a slot is occupied or not by checking the header
	 * 
	 * @param s
	 *            the slot to test
	 * @return true if occupied
	 */
	public boolean slotOccupied(int s) {
		// your code here
		int numByte = s / 8; // number of bytes
		int numBit = s % 8; // number of bits
		if (numByte > header.length - 1) {
			return false;
		}
		//seeing if that bit is 'taken' meaning it is populated with a 1
		int location = header[numByte] >> numBit; // slot location in header
		if ((location & 1) == 1) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Sets the occupied status of a slot by modifying the header
	 * 
	 * @param s
	 *            the slot to modify
	 * @param value
	 *            its occupied status
	 */
	public void setSlotOccupied(int s, boolean value) {
		// your code here
		int numByte = s / 8; // number of bytes
		int numBit = s % 8; // number of bits
		//shifting the value by adding a 1 bit, representing taken slot
		if (slotOccupied(s) == false && value == true) {
			header[numByte] ^= (1 << numBit); // modify if write to the slot
		
		}
	
		if (slotOccupied(s) == true && value == false) {
			header[numByte] ^= (1 << numBit); // modify if delete from the slot
		
		}
	}

	/**
	 * Adds the given tuple in the next available slot. Throws an exception if
	 * no empty slots are available. Also throws an exception if the given tuple
	 * does not have the same structure as the tuples within the page.
	 * 
	 * @param t
	 *            the tuple to be added.
	 * @throws Exception
	 */
	public void addTuple(Tuple t) throws Exception {
		// your code here
		if (getNumSlots() == 0) {
			throw new Exception("number of slots is 0");
		}
		if (!t.getDesc().equals(this.td)) {
			throw new Exception("schema doesn't match");
		}
		//checking to see if empty slots in pages that are not the last page.
		for (int i = 0; i < tuples.length; i++) {
			if (!slotOccupied(i)) {
				t.setId(i);
				t.setPid(this.id);
				tuples[i] = t;
				setSlotOccupied(i, true);
				break;
			}
		}
	}

	public int emptySlots() {
		int count = 0;
		
		for (int i = 0; i < this.getNumSlots(); i++) {
			if (!this.slotOccupied(i))
				count++;
		}
		return count;
	}

	/**
	 * Removes the given Tuple from the page. If the page id from the tuple does
	 * not match this page, throw an exception. If the tuple slot is already
	 * empty, throw an exception
	 * 
	 * @param t
	 *            the tuple to be deleted
	 * @throws Exception
	 */
	public void deleteTuple(Tuple t) throws Exception {
		// your code here
		int tupleID = t.getId();
		int tuplePid = t.getPid();
		if (this.id != tuplePid) {
			throw new Exception("Page id doesn't match");
		}
		if (slotOccupied(tupleID) == false) {
			throw new Exception("Empty slot");
		}
		setSlotOccupied(tupleID, false);
		tuples[tupleID] = null;
	}

	/**
	 * Suck up tuples from the source file.
	 */
	private Tuple readNextTuple(DataInputStream dis, int slotId) {
		// if associated bit is not set, read forward to the next tuple, and
		// return null.
		if (!slotOccupied(slotId)) {
			for (int i = 0; i < td.getSize(); i++) {
				try {
					dis.readByte();
				} catch (IOException e) {
					throw new NoSuchElementException("error reading empty tuple");
				}
			}
			return null;
		}
		//
		// read fields in the tuple
		Tuple t = new Tuple(td);
		t.setPid(this.id);
		t.setId(slotId);

		for (int j = 0; j < td.numFields(); j++) {
			if (td.getType(j) == Type.INT) {
				byte[] field = new byte[4];
				try {
					dis.read(field);
					t.setField(j, field);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				// byte[] field = new byte[132];
				byte[] field = new byte[129];
				try {
					dis.read(field);
					t.setField(j, field);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		return t;
	}

	/**
	 * Generates a byte array representing the contents of this page. Used to
	 * serialize this page to disk.
	 *
	 * The invariant here is that it should be possible to pass the byte array
	 * generated by getPageData to the HeapPage constructor and have it produce
	 * an identical HeapPage object.
	 *
	 * @return A byte array correspond to the bytes of this page.
	 */
	public byte[] getPageData() {
		int len = HeapFile.PAGE_SIZE;
		ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
		DataOutputStream dos = new DataOutputStream(baos);

		// create the header of the page
		for (int i = 0; i < header.length; i++) {
			try {
				dos.writeByte(header[i]);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// create the tuples
		for (int i = 0; i < tuples.length; i++) {

			// empty slot
			if (!slotOccupied(i)) {
				for (int j = 0; j < td.getSize(); j++) {
					try {
						dos.writeByte(0);
					} catch (IOException e) {
						e.printStackTrace();
					}

				}
				continue;
			}

			// non-empty slot
			for (int j = 0; j < td.numFields(); j++) {
				byte[] f = tuples[i].getField(j);
				try {
					dos.write(f);

				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// padding
		int zerolen = HeapFile.PAGE_SIZE - (header.length + td.getSize() * tuples.length); // -
																							// numSlots
																							// *
																							// td.getSize();
		byte[] zeroes = new byte[zerolen];
		try {
			dos.write(zeroes, 0, zerolen);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			dos.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return baos.toByteArray();
	}

	/**
	 * Returns an iterator that can be used to access all tuples on this page.
	 * 
	 * @return
	 */
	public Iterator<Tuple> iterator() {

		List<Tuple> x = new ArrayList();

		for (Tuple p : tuples) {
			if (p != null)
				x.add(p);
		}
		return x.iterator();
	}
}
