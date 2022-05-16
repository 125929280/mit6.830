package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;

    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pageNumber = pid.getPageNumber();
        int pageSize = BufferPool.getPageSize();
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "r");
            byte[] data = new byte[pageSize];
            randomAccessFile.seek(pageNumber * pageSize);
            randomAccessFile.read(data);
            HeapPage page = new HeapPage((HeapPageId) pid, data);
            randomAccessFile.close();
            return page;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new IllegalArgumentException();
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
//        System.out.println(page.getId().getPageNumber() + " " + numPages());
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        randomAccessFile.seek(page.getId().getPageNumber() * BufferPool.getPageSize());
        randomAccessFile.write(page.getPageData());
        randomAccessFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        long len = file.length();
        return (int) Math.floor(len * 1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        List<Page> ans = new ArrayList<>();
        for(int i = 0;i < numPages();i ++) {
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            if(heapPage.getNumEmptySlots() == 0) continue;
            heapPage.insertTuple(t);
            ans.add(heapPage);
            return ans;
        }

//        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file, true));
//        byte[] emptyPage = HeapPage.createEmptyPageData();
//        bos.write(emptyPage);
//        bos.close();

        HeapPage newPage = new HeapPage(new HeapPageId(getId(), numPages()), HeapPage.createEmptyPageData());
        newPage.insertTuple(t);
        writePage(newPage);
        ans.add(newPage);
        return ans;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> ans = new ArrayList<>();
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        heapPage.deleteTuple(t);
        ans.add(heapPage);
        return ans;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

    private class HeapFileIterator implements DbFileIterator {

        private Iterator<Tuple> it;
        private int nowPage;

        private final HeapFile heapFile;
        private final TransactionId transactionId;
        private final int numPages;

        public HeapFileIterator(HeapFile heapFile, TransactionId transactionId) {
            this.heapFile = heapFile;
            this.transactionId = transactionId;
            this.numPages = numPages();
        }

        private Iterator<Tuple> getIter(int pageNo) throws TransactionAbortedException, DbException {
            HeapPageId pid = new HeapPageId(getId(), pageNo);
            BufferPool bufferPool = Database.getBufferPool();
            HeapPage heapPage = (HeapPage) bufferPool.getPage(transactionId, pid, Permissions.READ_ONLY);
            return heapPage.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            nowPage = 0;
            it = getIter(nowPage);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            while(nowPage < numPages) {
                if(it == null) {
                    return false;
                }
                if(it.hasNext()) {
                    return true;
                }
                nowPage ++;
                if(nowPage >= numPages) {
                    return false;
                }
                it = getIter(nowPage);
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(it == null || !it.hasNext()) {
                throw new NoSuchElementException();
            }
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            it = null;
        }
    }

}

