package test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import hw1.BufferPool;
import hw1.Catalog;
import hw1.Database;
import hw1.HeapFile;
import hw1.HeapPage;
import hw1.Permissions;
import hw1.Tuple;
import hw1.TupleDesc;


public class TransactionTest {
	
	private Catalog c;
	private BufferPool bp;
	private HeapFile hf;
	private TupleDesc td;
	private int tid;
	
	@Before
	public void setup() {
		
		try {
			Files.copy(new File("testfiles/test.dat.bak").toPath(), new File("testfiles/test.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			System.out.println("unable to copy files");
			e.printStackTrace();
		}
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/test.txt");
		
		int tableId = c.getTableId("test");
		td = c.getTupleDesc(tableId);
		hf = c.getDbFile(tableId);
		
		bp = Database.getBufferPool();
		
		tid = c.getTableId("test");
	}
	@Test
	public void testReleaseLocks() throws Exception {
		bp.getPage(0, tid, 0, Permissions.READ_ONLY);
	    bp.getPage(0, tid, 0, Permissions.READ_WRITE);
	    bp.transactionComplete(0, true);

	    //check if tid is same before blocking?
	    bp.getPage(1, tid, 0, Permissions.READ_WRITE);
	    bp.getPage(1, tid, 0, Permissions.READ_WRITE);
	    
	    
	    
	    
	    bp.transactionComplete(1, true);
	    assertTrue(true); //will only reach this point if locks are properly released
	}
	
	/*
	 * Transaction A acquires a share lock on row 1.
Transaction B acquires a share lock on row 2.
Transaction A now requests an exclusive lock on row 2, 
and is blocked until transaction B finishes and releases the 
share lock it has on row 2.
Transaction B now requests an exclusive lock on row 1, and is 
blocked until transaction A finishes and releases the share lock 
it has on row 1.
	 */
	@Test
	public void deadlocktest() throws Exception{
		bp.getPage(0, tid, 0, Permissions.READ_ONLY);
		
	 HeapPage curr =  bp.getPage(1, tid, 0, Permissions.READ_WRITE);
	    bp.releasePage(0, tid, 0);
	    
	 HeapPage next = bp.getPage(2, tid, 0, Permissions.READ_WRITE); //this transaction will abort...
	    
			 
	 assertTrue(true);   
	 
	}
	
	@Test
	public void testCommit() throws Exception {
		Tuple t = new Tuple(td);
		t.setField(0, new byte[] {0, 0, 0, (byte)131});
		byte[] s = new byte[129];
		s[0] = 2;
		s[1] = 98;
		s[2] = 121;
		t.setField(1, s);
		
		bp.getPage(0, tid, 0, Permissions.READ_WRITE); //acquire lock for the page
		bp.insertTuple(0, tid, t); //insert the tuple into the page
		bp.transactionComplete(0, true); //should flush the modified page
		
		//reset the buffer pool, get the page again, make sure data is there
		Database.resetBufferPool(BufferPool.DEFAULT_PAGES);
		HeapPage hp = bp.getPage(1, tid, 0, Permissions.READ_ONLY);
		Iterator<Tuple> it = hp.iterator();
		assertTrue(it.hasNext());
		it.next();
		assertTrue(it.hasNext());
		it.next();
		assertFalse(it.hasNext());
	}
	
	@Test
	public void testAbort() throws Exception {
		Tuple t = new Tuple(td);
		t.setField(0, new byte[] {0, 0, 0, (byte)131});
		byte[] s = new byte[129];
		s[0] = 2;
		s[1] = 98;
		s[2] = 121;
		t.setField(1, s);
		
		bp.getPage(0, tid, 0, Permissions.READ_WRITE); //acquire lock for the page
		bp.insertTuple(0, tid, t); //insert the tuple into the page
		bp.transactionComplete(0, false); //should abort, discard changes
		
		//reset the buffer pool, get the page again, make sure data is there
		Database.resetBufferPool(BufferPool.DEFAULT_PAGES);
		HeapPage hp = bp.getPage(1, tid, 0, Permissions.READ_ONLY);
		Iterator<Tuple> it = hp.iterator();
		assertTrue(it.hasNext());
		it.next();
		assertFalse(it.hasNext());
	}

}
