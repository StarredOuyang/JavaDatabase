package hw1;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;





/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool which check that the transaction has the appropriate locks
 * to read/write the page.
 */
public class BufferPool {
	/** Bytes per page, including header. */
	public static final int PAGE_SIZE = 4096;

	// Our cache will be a Map<HeapFile, Boolean>
	// boolean will hold status isDirty, if it has been modified(default false)
	// we will pull
	// heapfile out of map, modify it, put back into map mark isDirty = true

	Map<Integer, HeapPage> cache; // cache itself maps pageId(in heapfile) to
									// cached page which may be modified while
									// in BufferPool
	Map<Integer, Boolean> isDirty; // maps pageid to boolena isdirty

	// this will hold the transaction id, and each tid's corresponding list of
	// pageid's?
	
	
	
	//take this out
	//Map<Integer, List<Integer>> transaction_pages;
//iterate through or second map page id table id. 
	//for partial information
	//plug in partial value with id's as tranasction id or whatever and Entry as value.

	// arraylist of transactid ??

	// still need to moidify heappage to reflect concept of lock

	// how to access heapfile from here?

	/**
	 * Default number of pages passed to the constructor. This is used by other
	 * classes. BufferPool should use the numPages argument to the constructor
	 * instead.
	 */

	public static final int DEFAULT_PAGES = 50;
	private int numPages;
private boolean deadlock;
	/**
	 * Creates a BufferPool that caches up to numPages pages.
	 *
	 * @param numPages
	 *            maximum number of pages in this buffer pool.
	 */
	public BufferPool(int numPages) {
		// your code here
deadlock = false;
		this.numPages = numPages;
		cache = new HashMap<>();
		isDirty = new HashMap<>();
		//transaction_pages = new HashMap<>();

	}

	/*
	 * DEFINTION: STRICT 2PL - guarantees strict schedules (see Section 21.4).
	 * In this variation, a transaction T does not release any of 22.1 Two-Phase
	 * Locking Techniques for Concurrency Control 785 its exclusive (write)
	 * locks until after it commits or aborts. Hence, no other transaction can
	 * read or write an item that is written by T unless T has committed,
	 * leading to a strict schedule for recoverability. Strict 2PL is not
	 * deadlock-free.
	 * 
	 */

	// how should transactions be "conceptualized" in BufferPool?
	// map<Integer, List<something>> transactions = new HashMap<>();
	// map keys are "transaction id's, which return list of locks held by
	// transaction?
	// dont know if list is best way

	/**
	 * Retrieve the specified page with the associated permissions. Will acquire
	 * a lock and may block if that lock is held by another transaction.
	 * <p>
	 * The retrieved page should be looked up in the buffer pool. If it is
	 * present, it should be returned. If it is not present, it should be added
	 * to the buffer pool and returned. If there is insufficient space in the
	 * buffer pool, an page should be evicted and the new page should be added
	 * in its place.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the page
	 * @param tableId
	 *            the ID of the table with the requested page
	 * @param pid
	 *            the ID of the requested page
	 * @param perm
	 *            the requested permissions on the page
	 */

	// need to block on write-write, read-write(try to write when read on lock
	// table)
	// write-read (transaction tries to read a page that has a write lock on it)

	// write lock - no page can read or write until lock released
	// read lock - any page can read, no page can write until lock released

	// perm check if 0 then locktable gets R for tha tpge, if 1 then gets W for
	// that page

	// transaction_pages update this method

	public HeapPage getPage(int tid, int tableId, int pid, Permissions perm) throws Exception {
		// ****throw exception or return null if lock associated with
		// transaction id or
		// page is dirty?? *****
		HeapFile hf = Database.getCatalog().getDbFile(tableId);//
		HeapPage page = hf.readPage(pid);
		
Map<Entries,Character> lock_table = Database.getCatalog().lock_table;

		// case 1(simplest): not yet in cache. can give it whatever lock it
		// wants, put in cache
		// mark as clean
Entries new_entry = new Entries(pid,tableId,tid);

		if (cache.size() < numPages && !cache.containsKey(pid)) {

			// put page in cache, mark as clean
			cache.put(pid, page);
			isDirty.put(pid, false);
			// put requested lock on page:
			
			//make new entry:page_id, table_id, tansaction_id
			
			lock_table.put(new_entry, (perm.permLevel == 0 ? 'R' : 'W'));

			// add page to transaction mapping.
			
			Database.getCatalog().lock_table = lock_table;
			return cache.get(pid); // return heapfile page or use that page id
									// to return cache? in case modified?

		}

		// case 2: if  it is present in cache.
		//block if W exists on current page.
		// or if this tid requests W and Read on current page.
		//else can put W on page with no lock
		//or add another read lock entry in lock_table

		if (cache.size() < numPages && cache.containsKey(pid)) {
 //if holds lock do nothing. else iterate through lock table entries.
			//if W lock on entry with same page id and table id, then block, else add W or R
			

			//find out if lock exists on current page
			Character current_perm = null;// will stay null if not found in map, means no lock
			Entries curr = null;
	//if W: , if R: if null n lock on page	
			for(Entries e : lock_table.keySet()){
				if(e.get_page_id()==pid && e.get_table_id()==tableId){
					if(lock_table.get(e)=='W'){
						curr = e; 
						current_perm = 'W';
						break; //if W, know write lock on page, rest irrelevant
					}else{ //its a R
						curr = e;
						current_perm = 'R';
						
					}
					
				}
				
			}
			
			if(curr==null){//no lock on page
				lock_table.put(new_entry, (perm.permLevel == 0 ? 'R' : 'W'));
				Database.getCatalog().lock_table = lock_table;
				cache.put(pid, page);
				isDirty.put(pid, false);
				Database.getCatalog().lock_table = lock_table;
				return cache.get(pid);
			} else if(lock_table.get(curr)=='W'){
				//block**
				TimeUnit.SECONDS.sleep(1); //bool set false initiallt, set trye after this
				if(deadlock){ //abort transaction
					deadlock = false; //reset for next check
					transactionComplete( tid,  false);
					}else{
				deadlock = true;
				return getPage( tid,  tableId,  pid,  perm);
					}
				
			}else if(lock_table.get(curr) =='R'){
				if(perm.permLevel==1){ //request W on page that has R, 
					//Block ****
					//bool set false initiallt, set trye after this
					if(deadlock){ //abort transaction
						deadlock = false; //reset for next check
						
						//aborint transaction, but still return the cache
						transactionComplete( tid,  false);
						return cache.get(pid);
						
						}else{
							TimeUnit.SECONDS.sleep(1); 
					deadlock = true;
					return getPage( tid,  tableId,  pid,  perm);
						}
					
				}else{ //request R on page that has R, ok.
					lock_table.put(new_entry, (perm.permLevel == 0 ? 'R' : 'W'));
					cache.put(pid, page);
					isDirty.put(pid, false);
					Database.getCatalog().lock_table = lock_table;
					return cache.get(pid);
				}
			}
			
			
			
			Database.getCatalog().lock_table = lock_table;
			return cache.get(pid);
		}

		// ** flush/release??*
		// case 3: evict one page, insert like above, let transaction do
		// whatever it wants
		if (cache.size() == numPages) {
			evictPage();
			cache.put(pid, page);
			isDirty.put(pid, false);
			// put requested lock on page:
			lock_table.put(new_entry, (perm.permLevel == 0 ? 'R' : 'W'));

			
			Database.getCatalog().lock_table = lock_table;
			return cache.get(pid);

		}
		
		//reset lock table: changes made above are only local until updated here
		Database.getCatalog().lock_table = lock_table;

		return null;
	}

	/**
	 * Releases the lock on a page. Calling this is very risky, and may result
	 * in wrong behavior. Think hard about who needs to call this and why, and
	 * why they can run the risk of calling it.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @param tableID
	 *            the ID of the table containing the page to unlock
	 * @param pid
	 *            the ID of the page to unlock
	 */
	public void releasePage(int tid, int tableId, int pid) {
		// your code here
		HeapFile hf = Database.getCatalog().getDbFile(tableId);
		Map<Entries, Character> lock_table =  Database.getCatalog().lock_table;
		// think this is all?
		// page_id, int table_id, int transaction_id
		lock_table.remove(new Entries(pid,tableId,tid)); // remove lock associated w/ page
		
		Database.getCatalog().lock_table = lock_table;
		
	}

	// map<int, list<int>> transaction id mapped to list of page id's?
	// transaction id mapped to series of locks?
	/**
	 * Return true if the specified transaction has a lock on the specified page
	 */
	public boolean holdsLock(int tid, int tableId, int pid) {
		// your code here

		HeapFile hf = Database.getCatalog().getDbFile(tableId);// Why would we
		Map<Entries,Character> lock_table =  Database.getCatalog().lock_table;
		
		for(Entries e : lock_table.keySet()){
			if(tid==e.get_transaction_id() && tableId==e.get_table_id() && pid == e.get_page_id()) return true;
		}
		
		

		return false;
	}

	/**
	 * Commit or abort a given transaction; release all locks associated to the
	 * transaction. If the transaction wishes to commit, write
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @param commit
	 *            a flag indicating whether we should commit or abort
	 */
	public void transactionComplete(int tid, boolean commit) throws IOException {
		Map<Entries, Character> lock_table = Database.getCatalog().lock_table;

	
		// write to disk or abort:
		if (commit) {
//iterate through entries, if tid==tid of entry, flush that page.
			Iterator it = lock_table.entrySet().iterator();

			while(it.hasNext()){
				Entry item = (Entry) it.next();
				Entries e = (Entries) item.getKey();
				char permissions = (char) item.getValue();
		if(e.get_transaction_id()==tid)
					flushPage(e.get_table_id(), e.get_page_id());
			}

		}
		

		// now remove locks associated with tid
		Iterator it = lock_table.entrySet().iterator();
		
		while(it.hasNext()){
			Entry item = (Entry) it.next();
			Entries e = (Entries) item.getKey();
			char permissions = (char) item.getValue();
	if(e.get_transaction_id()==tid)
				it.remove();
	cache.remove(e.get_page_id());
	isDirty.remove(e.get_page_id());
		}
		
		

		
		
//update lock_table
		Database.getCatalog().lock_table = lock_table;
		

	}

	/**
	 * Add a tuple to the specified table behalf of transaction tid. Will
	 * acquire a write lock on the page the tuple is added to. May block if the
	 * lock cannot be acquired.
	 * 
	 * Marks any pages that were dirtied by the operation as dirty
	 *
	 * @param tid
	 *            the transaction adding the tuple
	 * @param tableId
	 *            the table to add the tuple to
	 * @param t
	 *            the tuple to add
	 */
	public void insertTuple(int tid, int tableId, Tuple t) throws Exception {
		// no checks for permissions or anything... should we?

		// just get associated page with new tuple, add it to cache, mark as
		// dirty.

		HeapFile hf = Database.getCatalog().getDbFile(tableId);
		// throw exception if tid does not have page id in its list, and if page
		// id is not
		// associated with write lock

		// *** should we be writing to heapfile? isnt that writing to disk?*
		// should we rather just modify heappage method or find the page
		// ourselves,
		// add the tuple manually and then only write pages to heapfile in
		// flush?

		HeapPage page = hf.add_not_write_tuple(t);
		cache.put(page.getId(), page);
		isDirty.put(page.getId(), true);

	}

	/**
	 * Remove the specified tuple from the buffer pool. Will acquire a write
	 * lock on the page the tuple is removed from. May block if the lock cannot
	 * be acquired.
	 *
	 * Marks any pages that were dirtied by the operation as dirty.
	 *
	 * @param tid
	 *            the transaction adding the tuple.
	 * @param tableId
	 *            the ID of the table that contains the tuple to be deleted
	 * @param t
	 *            the tuple to add
	 */
	public void deleteTuple(int tid, int tableId, Tuple t) throws Exception {
		// all blocking done in getPage?? no need for checks in insert/delete?
		HeapFile hf = Database.getCatalog().getDbFile(tableId);
		// how to mark page dirty?

		//iterate thru  all heappages in heapfile?
	int pid = t.getPid();
	HeapPage mod_page = hf.readPage(pid);
	mod_page.deleteTuple(t);
		
	
		// put new page in cache, mark as dirty.
		cache.put(mod_page.getId(), mod_page);
		isDirty.put(mod_page.getId(), true);

	}

	/*
	 * Flushes the page to disk to ensure dirty pages are updated on disk.
	 */
	private synchronized void flushPage(int tableId, int pid) throws IOException {
		// your code here
		HeapFile hf = Database.getCatalog().getDbFile(tableId);
		if (isDirty.get(pid)) {// page has been modified,
			hf.writePage(cache.get(pid));
			isDirty.put(pid, false);// mark as clean
		}
	}

	/**
	 * Discards a page from the buffer pool.
	 */
	private synchronized void evictPage() throws Exception {
		// find first page marked 'false'

		// this is unordered is that ok??
		for (Integer pid : cache.keySet()) {
			if (isDirty.get(pid) == false) {
				// call flush page before this?
				cache.remove(pid);
				isDirty.remove(pid);
				return; // removes first clean page

			}
		}

	}

}
