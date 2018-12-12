package simpledb;

import java.io.*;

import java.nio.Buffer;
import java.util.*;

import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
	/**
	 * Bytes per page, including header.
	 */
	private static final int PAGE_SIZE = 4096;
	
	private static int pageSize = PAGE_SIZE;

	private static int TIMEOUT_THRESHOLD = 20200;
	static boolean DEBUG_ON = false;
	static boolean DETECT_DEADLOCK = false;
	
	/**
	 * Default number of pages passed to the constructor. This is used by
	 * other classes. BufferPool should use the numPages argument to the
	 * constructor instead.
	 */
	public static final int DEFAULT_PAGES = 50;
	
	final int numPages;   // number of pages -- currently, not enforced
	final ConcurrentHashMap<PageId, Page> pages; // hash table storing current pages in memory
	private LockManager lm;

	private ConcurrentHashMap<PageId, Integer> lruCache;
	
	/**
	 * Creates a BufferPool that caches up to numPages pages.
	 *
	 * @param numPages maximum number of pages in this buffer pool.
	 */
	public BufferPool(int numPages) {
		this.numPages = numPages;
		this.pages = new ConcurrentHashMap<PageId, Page>();
		lm = new LockManager();
	}
	
	public static int getPageSize() {
		return pageSize;
	}
	
	// THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
	public static void setPageSize(int pageSize) {
		BufferPool.pageSize = pageSize;
	}
	
	// THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
	public static void resetPageSize() {
		BufferPool.pageSize = PAGE_SIZE;
	}
	
	/**
	 * Retrieve the specified page with the associated permissions.
	 * Will acquire a lock and may block if that lock is held by another
	 * transaction.
	 * <p>
	 * The retrieved page should be looked up in the buffer pool.  If it
	 * is present, it should be returned.  If it is not present, it should
	 * be added to the buffer pool and returned.  If there is insufficient
	 * space in the buffer pool, an page should be evicted and the new page
	 * should be added in its place.
	 *
	 * @param tid  the ID of the transaction requesting the page
	 * @param pid  the ID of the requested page
	 * @param perm the requested permissions on the page
	 */
	public Page getPage(TransactionId tid, PageId pid, Permissions perm)
			throws TransactionAbortedException, DbException {
		// XXX Yuan points out that HashMap is not synchronized, so this is buggy.
		// XXX TODO(ghuo): do we really know enough to implement NO STEAL here?
		//     won't we still evict pages?

		//some code goes here
		// Acquire the proper lock first
		long start = System.currentTimeMillis();
		boolean success = lm.acquireLock(tid, pid, perm);
		Random r = new Random();
		while (!success) {
			try {
				long end = System.currentTimeMillis();
				if (end - start > (TIMEOUT_THRESHOLD + r.nextInt(100))) {
					System.out.println("Abort: waiting for Tid = " + tid.getId() + ", Pid = " + pid.toString() + ", Perm = " + perm.toString());
					throw new TransactionAbortedException();
				}
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			success = lm.acquireLock(tid, pid, perm);
		}
		// Now we have the proper lock
		if (!pages.containsKey(pid)) {
			if (pages.size() == numPages)
				evictPage();
			pages.put(pid, Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid));
			pages.get(pid).setBeforeImage();
		}

		if (perm == Permissions.READ_WRITE)
			pages.get(pid).markDirty(true, tid);

		return pages.get(pid);
//		Page p = this.pages.get(pid);
//
//		if (p != null) {
//			// Do LRU Cache update
//			for (PageId piddd: this.lruCache.keySet()) {
//				int times = this.lruCache.get(piddd);
//				this.lruCache.replace(piddd, times + 1);
//			}
//			this.lruCache.replace(pid, 1);
//			return p;
//		} else {
//			p = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
//			if (this.pages.size() == this.numPages) {
//				this.evictPage();
//			}
//			this.lruCache.put(pid, 1);
//			this.pages.put(pid, p);
//			return p;
//		}

//		Page p;
//		synchronized (this) {
//			p = pages.get(pid);
//			if (p == null) {
//				if (pages.size() >= numPages) {
//					evictPage();
//				}
//
//				p = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
//				pages.put(pid, p);
//			}
//		}
//
//		return p;
	}
	
	/**
	 * Releases the lock on a page.
	 * Calling this is very risky, and may result in wrong behavior. Think hard
	 * about who needs to call this and why, and why they can run the risk of
	 * calling it.
	 *
	 * @param tid the ID of the transaction requesting the unlock
	 * @param pid the ID of the page to unlock
	 */
	public void releasePage(TransactionId tid, PageId pid) {
		// some code goes here
		// not necessary for lab1|lab2|lab3
		lm.releaseLock(tid, pid);
	}
	
	/**
	 * Release all locks associated with a given transaction.
	 *
	 * @param tid the ID of the transaction requesting the unlock
	 */
	public void transactionComplete(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2|lab3
		transactionComplete(tid, true);
	}
	
	/**
	 * Return true if the specified transaction has a lock on the specified page
	 */
	public boolean holdsLock(TransactionId tid, PageId p) {
		// some code goes here
		// not necessary for lab1|lab2|lab3
		return lm.holdsLock(tid, p);
	}
	
	/**
	 * Commit or abort a given transaction; release all locks associated to
	 * the transaction.
	 *
	 * @param tid    the ID of the transaction requesting the unlock
	 * @param commit a flag indicating whether we should commit or abort
	 */
	public void transactionComplete(TransactionId tid, boolean commit)
			throws IOException {
		// some code goes here
		// not necessary for lab1|lab2|lab3
		if (BufferPool.DEBUG_ON)
			System.out.println("Tx" + tid.getId() + (commit ? "commit" : "abort"));
		try {
			for (PageId pid : pages.keySet()) {
				if (pages.get(pid).isDirty() != null && pages.get(pid).isDirty().equals(tid)) {
					if (commit)
						flushPage(pid);
					else
						pages.put(pid, pages.get(pid).getBeforeImage());
				}
			}
		}
		catch (NullPointerException e) {
			e.printStackTrace();
			System.exit(0);
		}

		lm.releaseAllLocks(tid);
	}
	
	/**
	 * Add a tuple to the specified table on behalf of transaction tid.  Will
	 * acquire a write lock on the page the tuple is added to and any other
	 * pages that are updated (Lock acquisition is not needed for lab2).
	 * May block if the lock(s) cannot be acquired.
	 * <p>
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markDirty bit, and adds versions of any pages that have
	 * been dirtied to the cache (replacing any existing versions of those pages) so
	 * that future requests see up-to-date pages.
	 *
	 * @param tid     the transaction adding the tuple
	 * @param tableId the table to add the tuple to
	 * @param t       the tuple to add
	 */
	public void insertTuple(TransactionId tid, int tableId, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		ArrayList<Page> pages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
		
		for (Page page : pages) {
			page.markDirty(true, tid);
			this.pages.put(page.getId(), page);
		}
	}
	
	/**
	 * Remove the specified tuple from the buffer pool.
	 * Will acquire a write lock on the page the tuple is removed from and any
	 * other pages that are updated. May block if the lock(s) cannot be acquired.
	 * <p>
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markDirty bit, and adds versions of any pages that have
	 * been dirtied to the cache (replacing any existing versions of those pages) so
	 * that future requests see up-to-date pages.
	 *
	 * @param tid the transaction deleting the tuple.
	 * @param t   the tuple to delete
	 */
	public void deleteTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		ArrayList<Page> pages = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);
		
		for (Page page : pages) {
			page.markDirty(true, tid);
			this.pages.put(page.getId(), page);
		}
	}
	
	/**
	 * Flush all dirty pages to disk.
	 * NB: Be careful using this routine -- it writes dirty data to disk so will
	 * break simpledb if running in NO STEAL mode.
	 */
	public synchronized void flushAllPages() throws IOException {
		// some code goes here
		for (PageId pid : pages.keySet()) {
			flushPage(pid);
		}
	}
	
	/**
	 * Remove the specific page id from the buffer pool.
	 * Needed by the recovery manager to ensure that the
	 * buffer pool doesn't keep a rolled back page in its
	 * cache.
	 * <p>
	 * Also used by B+ tree files to ensure that deleted pages
	 * are removed from the cache so they can be reused safely
	 */
	public synchronized void discardPage(PageId pid) {
		// some code goes here
		pages.remove(pid);
	}
	
	/**
	 * Flushes a certain page to disk
	 *
	 * @param pid an ID indicating the page to flush
	 */
	private synchronized void flushPage(PageId pid) throws IOException {
		// some code goes here
		Page page = pages.get(pid);
		if (page != null) {
			TransactionId tid = page.isDirty();
			if (tid != null) {
				Database.getLogFile().logWrite(tid, page.getBeforeImage(), page);
				Database.getLogFile().force();
			}
			
			Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(pages.get(pid));
			
			page.markDirty(false, tid);
		}
		
	}
	
	/**
	 * Write all pages of the specified transaction to disk.
	 */
	public synchronized void flushPages(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2|lab3
		for (PageId pid : pages.keySet()) {
			Page p = pages.get(pid);
			if (p.isDirty() != null && p.isDirty().equals(tid))
				flushPage(pid);
		}
	}
	
	/**
	 * Discards a page from the buffer pool.
	 * Flushes the page to disk to ensure dirty pages are updated on disk.
	 * <p>
	 * eviction policy:
	 * get a list of keys in the hashmap, retrieve the first key in the list,
	 * and remove it from the hashmap.
	 */
	private synchronized void evictPage() throws DbException {
		// some code goes here
		
//		PageId evict = pages.keys().nextElement();
//		Iterator<PageId> it = pages.keySet().iterator();
//		while (it.hasNext() && (pages.get(evict).isDirty() != null)) {
//			evict = it.next();
//		}
//
//		this.discardPage(evict);

		ArrayList<PageId> cleanPages = new ArrayList<>();
		for (PageId pid : pages.keySet()) {
			if (pages.get(pid).isDirty() == null) {
				cleanPages.add(pid);
			}
		}

		if (cleanPages.size() == 0) {
//			throw new DbException("No clean pages to evict!");
			try {
				flushAllPages();

				for (PageId pid : pages.keySet()) {
					if (pages.get(pid).isDirty() == null) {
						cleanPages.add(pid);
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		PageId evictionVictim = cleanPages.get((int) Math.floor(Math.random() * cleanPages.size()));
		try {
			assert pages.get(evictionVictim).isDirty() == null : "Can't evict a dirty page!";
			flushPage(evictionVictim);
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		pages.remove(evictionVictim);
	}

	private class LockManager {
		private HashMap<PageId, Set<TransactionId>> sharers;
		private HashMap<PageId, TransactionId> owners;
		private HashMap<TransactionId, Set<PageId>> sharedPages;
		private HashMap<TransactionId, Set<PageId>> ownedPages;

		private HashMap<PageId, Set<TransactionId>> waiters;
		private HashMap<TransactionId, PageId> waitedPages;

		private HashMap<TransactionId, Set<TransactionId>> waitingTx;

		public LockManager() {
			sharers = new HashMap<>();
			owners = new HashMap<>();
			sharedPages = new HashMap<>();
			ownedPages = new HashMap<>();
			waiters = new HashMap<>();
			waitedPages = new HashMap<>();
			waitingTx = new HashMap<>();
		}
		public synchronized boolean acquireLock(TransactionId tid, PageId pid, Permissions perm)
				throws TransactionAbortedException {
			checkConsistency();
			//System.out.println("************************************************************************************************************************Want: Tid = " + tid.toString() + ", Pid = " + pid.toString() + ", Perm = " + perm.toString());
			boolean success = false;
			if (perm.equals(Permissions.READ_WRITE)) {
				success = acquireExclusiveLock(tid, pid);
			} else if (perm.equals(Permissions.READ_ONLY)) {
				success = acquireSharedLock(tid, pid);
			} else if (perm.equals(Permissions.NO_LOCK)) {
				if (BufferPool.DEBUG_ON) System.out.println("Tx "+tid.getId() +" Nol "+pid);
				success = true;
			} else {
				assert false : "What?!";
			}
			if (success) {

				//System.out.println("Success: Tid = " + tid.toString() + ", Pid = " + pid.toString() + ", Perm = " + perm.toString());
				//removeWaiter(tid, pid);
				waitingTx.remove(tid);
				return true;
			} else {
				if (waitingTx.get(tid) == null ) waitingTx.put(tid, new HashSet<TransactionId>());
				if (perm.equals(Permissions.READ_WRITE)){
					// add edges for every Tx that has a read lock on this page
					Set<TransactionId> sharer = sharers.get(pid);
					if (sharer != null) {
						for( TransactionId t: sharer){
							waitingTx.get(tid).add(t);
							//dep.add(new Dep(tid, t, pid, perm));
						}
					}
					TransactionId owner = owners.get(pid);
					if (owner != null){
						waitingTx.get(tid).add(owner);
						//dep.add(new Dep(tid, owner, pid, perm));
					}
				} else if (perm.equals(Permissions.READ_ONLY)){
					TransactionId owner = owners.get(pid);
					if (owner != null){
						waitingTx.get(tid).add(owner);
						//dep.add(new Dep(tid, owner, pid, perm));
					}
				}
				//addWaiter(tid, pid);
				visited = new HashSet<>();
				if (DETECT_DEADLOCK && detectDeadlock(tid,tid)) {
					if (BufferPool.DEBUG_ON){
						System.out.println("Tx "+tid.getId() +" Deadlock Detected while trying to "+ pid.pageNumber() +" "+perm);
//						PrintDeadlockTree();
					}
					waitingTx.remove(tid);
					// There is a deadlock
					//removeWaiter(tid, pid);

					throw new TransactionAbortedException();
				}
				return false;
			}
		}
		HashSet<TransactionId> visited;
		private boolean detectDeadlock(TransactionId start, TransactionId cur) {
			if (visited.contains(cur)){
				if (start == cur) return true;
			} else {
				visited.add(cur);
				boolean ret = false;
				Set<TransactionId> s =waitingTx.get(cur);
				if (s==null) return ret;
				for (TransactionId t: s){
					ret |= detectDeadlock(start, t);
					if (ret) return true;
				}
				return ret;
			}


			return false;
		}

//		private void PrintDeadlockTree(){
//
//			for (TransactionId s : waitingTx.keySet()){
//				String st = " "+s.getId()+" : " + Arrays.toString(waitingTx.get(s).toArray()) ;
//				System.out.println(st);
//				for (TransactionId t : waitingTx.get(s)){
//					//st += t.getId() +", ";
//				}
//				//System.out.println(st);
//			}
//		}

		private void checkConsistency() {
			for (PageId pid : sharers.keySet()) {
				assert !owners.keySet().contains(pid) : "Page in sharers is also in owners.";
				assert sharers.get(pid).size() > 0 : "sharer is actually empty.";
				for (TransactionId tid : sharers.get(pid)) {
					assert sharedPages.get(tid).contains(pid) : "Pair in sharers does not exist in sharedPages.";
				}
			}
			for (PageId pid : owners.keySet()) {
				assert !sharers.keySet().contains(pid) : "Page in owners is also in sharers.";
				TransactionId tid = owners.get(pid);
				assert owners.get(pid) != null : "owner is actually empty.";
				assert ownedPages.get(tid).contains(pid) : "Pair in owners does not exist in ownedPages.";
			}
			for (TransactionId tid : sharedPages.keySet()) {
				assert sharedPages.get(tid).size() > 0 : "SharedPages is actually empty.";
				for (PageId pid : sharedPages.get(tid)) {
					assert sharers.get(pid).contains(tid) : "Pair in sharedPages does not exist in sharers.";
				}
			}
			for (TransactionId tid : ownedPages.keySet()) {
				assert ownedPages.get(tid).size() > 0 : "OwnedPages is actually empty.";
				for (PageId pid : ownedPages.get(tid)) {
					assert owners.get(pid).equals(tid) : "Pair in ownedPages does not exist in owners.";
				}
			}
		}
		public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
			checkConsistency();
			TransactionId owner = owners.get(pid);
			if (owner != null && owner.equals(tid)) return true;
			Set<TransactionId> sharer = sharers.get(pid);
			if (sharer != null && sharer.contains(tid)) return true;
			return false;
		}
		private void addWaiter(TransactionId tid, PageId pid) {
			Set<TransactionId> waiter = waiters.get(pid);
			if (waiter == null) waiter = new HashSet<TransactionId>();
			waiter.add(tid);
			waiters.put(pid, waiter);
			assert waitedPages.get(tid) == null || waitedPages.get(tid).equals(pid) : "This transaction is waiting for another page!";
			waitedPages.put(tid, pid);
		}
		private void removeWaiter(TransactionId tid, PageId pid) {
			Set<TransactionId> waiter = waiters.get(pid);
			if (waiter==null) return ;
			waiter.remove(tid);
			if (waiter.size() == 0) waiters.remove(pid);
			else waiters.put(pid, waiter);
			waitedPages.remove(tid);
		}
		private void addSharer(TransactionId tid, PageId pid) {
			Set<TransactionId> sharer = sharers.get(pid);
			if (sharer == null) sharer = new HashSet<TransactionId>();
			sharer.add(tid);
			sharers.put(pid, sharer);
			Set<PageId> sharedPage = sharedPages.get(tid);
			if (sharedPage == null) sharedPage = new HashSet<PageId>();
			sharedPage.add(pid);
			sharedPages.put(tid, sharedPage);
		}
		private void addOwner(TransactionId tid, PageId pid) {
			owners.put(pid, tid);
			Set<PageId> ownedPage = ownedPages.get(tid);
			if (ownedPage == null) ownedPage = new HashSet<PageId>();
			ownedPage.add(pid);
			ownedPages.put(tid, ownedPage);
		}
		private void removeSharer(TransactionId tid, PageId pid) {
			Set<TransactionId> sharer = sharers.get(pid);
			sharer.remove(tid);
			if (sharer.size() == 0) sharers.remove(pid);
			else sharers.put(pid, sharer);
			Set<PageId> sharedPage = sharedPages.get(tid);
			sharedPage.remove(pid);
			if (sharedPage.size() == 0) sharedPages.remove(tid);
			else sharedPages.put(tid, sharedPage);
		}
		private void removeOwner(TransactionId tid, PageId pid) {
			owners.remove(pid);
			Set<PageId> ownedPage = ownedPages.get(tid);
			ownedPage.remove(pid);
			if (ownedPage.size() == 0) ownedPages.remove(tid);
			else ownedPages.put(tid, ownedPage);
		}
		private boolean acquireExclusiveLock(TransactionId tid, PageId pid) {
			checkConsistency();
			Set<TransactionId> sharer = sharers.get(pid);
			TransactionId owner = owners.get(pid);
			assert owner == null || sharer == null : "You cannot acquire an exclusive lock on a page that is currently being shared by other transaction, or is already owned by another transaction.!";
			// There is an existing owner other than itself
			if (owner != null && !owner.equals(tid)) return false;
			// There is an existing sharer other than itself
			if (sharer != null && ( (sharer.size() > 1) || (sharer.size() == 1 && !sharer.contains(tid)) )) return false;
			if (sharer != null) {
				// It holds the shared permission already
				assert (sharer.size() == 1 && sharer.contains(tid)) : "Consistency issue.";
				removeSharer(tid, pid);
			}
			addOwner(tid, pid);
			checkConsistency();
			if (BufferPool.DEBUG_ON) System.out.println("Tx "+ tid.getId() + " Xlock Acq "+ pid);
			return true;
		}
		private boolean acquireSharedLock(TransactionId tid, PageId pid) {
			checkConsistency();
			Set<TransactionId> sharer = sharers.get(pid);
			TransactionId owner = owners.get(pid);
			if (BufferPool.DEBUG_ON) System.out.println("Tx "+ tid.getId() + " Slock Acq "+ pid);
			assert owner == null || sharer == null : "owner and sharer are not null at the same time!";
			if (owner != null && !owner.equals(tid)) {
				// There is an existing owner other than itself
				return false;
			} else {
				// (owner != null) means it holds the exclusive permission, thus does nothing
				if (owner == null) addSharer(tid, pid);
				checkConsistency();
				return true;
			}
		}
		public synchronized void releaseLock(TransactionId tid, PageId pid) {
			checkConsistency();
			//System.out.println("ReleaseLocks: Tid = " + tid.toString() + ", Pid = " + pid.toString());
			Set<PageId> sharedPage = sharedPages.get(tid);
			if (sharedPage != null && sharedPage.contains(pid)) {
				removeSharer(tid, pid);
			}
			Set<PageId> ownedPage = ownedPages.get(tid);
			if (ownedPage != null && ownedPage.contains(pid)) {
				removeOwner(tid, pid);
			}
			if (BufferPool.DEBUG_ON) System.out.println("Tx "+ tid.getId() + " Xlock Rel "+ pid);
		}
		public synchronized void releaseAllLocks(TransactionId tid) {
			checkConsistency();
			//System.out.println("ReleaseAllLocks: Tid = " + tid.toString());
			if (sharedPages.get(tid) != null) {
				for (PageId pid : sharedPages.get(tid)) {
					Set<TransactionId> sharer = sharers.get(pid);
					sharer.remove(tid);
					if (sharer.size() == 0) sharers.remove(pid);
					else sharers.put(pid, sharer);
				}
				sharedPages.remove(tid);
			}
			if (ownedPages.get(tid) != null) {
				for (PageId pid : ownedPages.get(tid)) {
					owners.remove(pid);
				}
				ownedPages.remove(tid);
			}
			waitingTx.remove(tid);
		}
	}
}
