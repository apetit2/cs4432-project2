package simpledb.buffer;

import simpledb.file.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
   private Buffer[] bufferpool;
   private int numAvailable;
   //hashmap of all free buffer spots left
   private Map<Integer, Integer> availableSlots;
   //hashmap of block filenames associated with a buffer spot in the buffer pool
   private Map<String, Integer> buffers;
   //array of all filled frame spots in the buffer pool
   ArrayList<Integer> usedFrames;
   //what replacement policy that we are using
   private String replacementPolicy;
   
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    * @param policy the type of replacement policy that we will use when buffer gets filled
    */
   BasicBufferMgr(int numbuffs, String policy) {
      //set up the entire bufferpool based upon a pre-specified number of buffers
      bufferpool = new Buffer[numbuffs];
      //the number of free buffers in the buffer pool
      numAvailable = numbuffs;
      //which frames are free in the buffer pool
      availableSlots = new HashMap<>();
      //block names associated with buffer frame in the buffer pool
      buffers = new HashMap<>();
      //what replacement policy we should be using
      replacementPolicy = policy;
      //what buffers/frames are already associated with a particular block
      usedFrames = new ArrayList<>();

      //populate the buffer pool and available slots
      for (int i=0; i<numbuffs; i++) {
         bufferpool[i] = new Buffer();
         bufferpool[i].setFrameNumber(i);
         availableSlots.put(i, 0);
      }
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
      for (Buffer buff : bufferpool)
         if (buff.isModifiedBy(txnum))
         buff.flush();
   }
   
   /**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         buff.assignToBlock(blk);
      }
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();
      //associate the pinned block with the buffer's frame number in the buffer pool
      buffers.put(blk.fileName(), buff.getFrameNumber());
      return buff;
   }
   
   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      buff.assignToNew(filename, fmtr);
      numAvailable--;
      buff.pin();
      //associate the pinned block with the buffer's frame number in the buffer pool
      buffers.put(buff.block().fileName(), buff.getFrameNumber());
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      String filename = buff.block().fileName();
      buff.unpin();
      if (!buff.isPinned()) {
         //since we unpinned the buffer with a block, we can free it up by adding it to the hashmap
         availableSlots.put(buff.getFrameNumber(), 0);
         //since we unpinned the block from this buffer, we can remove the association
         buffers.remove(filename);
         //up the number available
         numAvailable++;
      }
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }
   
   private Buffer findExistingBuffer(Block blk) {
      //check to see if our hashmap has the block in it
      if (buffers.get(blk.fileName()) != null){
         //if it does return the buffer frame number associated with that block
         int frameNumber = buffers.get(blk.fileName());
         return bufferpool[frameNumber];
      }
      //otherwise return null because we could not locate it
      return null;
   }

   private Buffer chooseUnpinnedBuffer() {
      //look up free slots from the list of free frames in the availableslots hashmap
      if (availableSlots.keySet().toArray().length != 0){
         //get the first free frame found and remove it from the hashmap
         int firstOpenSlot = (int) availableSlots.keySet().toArray()[0];
         availableSlots.remove(firstOpenSlot);
         //return the buffer at that frame
         return bufferpool[firstOpenSlot];
      }
      //this needs to be modified -- but for now return null if there are no open frames
      return null;
   }
}
