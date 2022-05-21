package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.List;
import java.util.Random;
import java.util.Vector;
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
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */

    private final int numPages;
    private final ConcurrentHashMap<PageId, Node> pageCache;

    class Node {
        PageId pageId;
        Page page;
        Node left, right;
        public Node() {}
        public Node(PageId _pageId, Page _page) {
            pageId = _pageId;
            page = _page;
        }
    }
    Node L, R;

    private void remove(Node node) {
//        System.out.println("node.left " + node.left);
//        System.out.println("node.left.right " + node.left.right);
//        System.out.println("node.right " + node.right);
        node.left.right = node.right;
        node.right.left = node.left;
    }

    private void insert(Node node) {
        node.left = L;
        node.right = L.right;
        L.right.left = node;
        L.right = node;
    }

    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.pageCache = new ConcurrentHashMap<>();
        L = new Node();
        R = new Node();
        L.right = R;
        R.left = L;
        lockManager = new LockManager();
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
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }


    class Lock {
        private TransactionId tid;
        private Permissions type;

        public Lock(TransactionId tid, Permissions type) {
            this.tid = tid;
            this.type = type;
        }

        public TransactionId getTid() {
            return tid;
        }

        public Permissions getType() {
            return type;
        }

        public void setTid(TransactionId tid) {
            this.tid = tid;
        }

        public void setType(Permissions type) {
            this.type = type;
        }
    }

    class LockManager {
        ConcurrentHashMap<PageId, Vector<Lock>> locksMap;

        public LockManager() {
            locksMap = new ConcurrentHashMap<>();
        }

        public synchronized boolean lock(TransactionId tid, PageId pid, Permissions type) {
            Vector<Lock> locks = locksMap.get(pid);
            Lock lock = new Lock(tid, type);
            if(locks == null) {
                //no locks on this page
                locks = new Vector<>();
                locks.add(lock);
                locksMap.put(pid, locks);
                return true;
            }

            if(locks.size() == 1) {
                Lock firstLock = locks.get(0);
                if(firstLock.tid.equals(tid)) {
                    // 是当前事务的锁
                    if(firstLock.type.equals(Permissions.READ_ONLY) && type.equals(Permissions.READ_WRITE)) {
                        // 锁升级
                        firstLock.setType(type);
                    }
                    return true;
                } else {
                    // 同为共享锁
                    if(firstLock.type.equals(Permissions.READ_ONLY) && type.equals(Permissions.READ_ONLY)) {
                        locks.add(lock);
                        return true;
                    }
                    return false;
                }
            }

            // 存在多个事务锁，说明全是共享锁
            if(type.equals(Permissions.READ_WRITE)) {
                return false;
            }

            for(Lock lock1 : locks) {
                // 重复请求共享锁
                if(lock.tid.equals(tid)) {
                    return true;
                }
            }

            locks.add(lock);
            return true;
        }

        public synchronized void releaseLock(TransactionId tid, PageId pid) {
            Vector<Lock> locks = locksMap.get(pid);
            for(int i = 0;i < locks.size();i ++) {
                Lock lock = locks.get(i);
                if(lock.tid.equals(tid)) {
                    locks.remove(lock);
                    if(locks.isEmpty()) {
                        locksMap.remove(pid);
                    }
                    return ;
                }
            }
        }

        public synchronized void releaseAllLocks(TransactionId tid) {
            for(PageId pageId : locksMap.keySet()) {
                Vector<Lock> locks = locksMap.get(pageId);
                for(int i = 0;i < locks.size();i ++) {
                    Lock lock = locks.get(i);
                    if(lock.tid.equals(tid)) {
                        locks.remove(lock);
                        if(locks.isEmpty()) {
                            locksMap.remove(pageId);
                        }
                    }
                }
            }
        }

        public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
            Vector<Lock> locks = locksMap.get(pid);
            for(Lock lock : locks) {
                if(lock.tid.equals(tid)) {
                    return true;
                }
            }
            return false;
        }
    }

    private LockManager lockManager;

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        boolean hasLock = false;
        long st = System.currentTimeMillis();
        long timeout = new Random().nextInt(200);
        while(!hasLock) {
            long ed = System.currentTimeMillis();
            if(ed - st > timeout) {
                throw new TransactionAbortedException();
            }
            hasLock = lockManager.lock(tid, pid, perm);
        }

        if(!pageCache.containsKey(pid)) {
            Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            Node node = new Node(pid, page);
            if(pageCache.size() == this.numPages) {
                // evict
                evictPage();
            }
            insert(node);
            pageCache.put(pid, node);
        } else {
            Node node = pageCache.get(pid);
//            System.out.println(node.page.toString());
            remove(node);
            insert(node);
        }
        return pageCache.get(pid).page;
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
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if(commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            restorePages(tid);
        }
        lockManager.releaseAllLocks(tid);
    }

    public synchronized void restorePages(TransactionId tid) {
        for(Node node : pageCache.values()) {
            if(tid.equals(node.page.isDirty())) {
                Page oriPage = Database.getCatalog().getDatabaseFile(node.pageId.getTableId()).readPage(node.pageId);
                remove(node);
                node.page = oriPage;
                insert(node);
                pageCache.put(node.pageId, node);
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = dbFile.insertTuple(tid, t);
        for(Page page:pages) {
            page.markDirty(true, tid);
            Node node = new Node(t.getRecordId().getPageId(), page);
            insert(node);
            pageCache.put(page.getId(), node);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> pages = dbFile.deleteTuple(tid, t);
        for(Page page:pages) {
            page.markDirty(true, tid);
            Node node = new Node(t.getRecordId().getPageId(), page);
            insert(node);
            pageCache.put(page.getId(), node);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for(Node node : pageCache.values()) {
            flushPage(node.page.getId());
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pageCache.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = pageCache.get(pid).page;
        if(page.isDirty() != null) {
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for(Node node : pageCache.values()) {
            if(node.page.isDirty() != null && node.page.isDirty().equals(tid)) {
                flushPage(node.pageId);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1

//        // lab2 exe4
//        Node node = R.left;
//        remove(node);
//        try {
//            flushPage(node.pageId);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        discardPage(node.pageId);

        // lab4 exe3
        for(Node node = R.left; node != L; node = node.left) {
            if(node.page.isDirty() == null) {
                // 不是脏页, evict
                remove(node);
                discardPage(node.pageId);
                return ;
            }
        }
        throw new DbException("all pages are dirty");
    }

}
