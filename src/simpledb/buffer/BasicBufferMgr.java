package simpledb.buffer;

import simpledb.file.*;

import java.util.*;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
//made public to unit test -- only reason, otherwise things get a little bit annoying
public class BasicBufferMgr {
   //=====================================CS4432-Project1=====================
   //the buffer pool -- contains all buffers
   private Buffer[] bufferpool;
   //number of buffers that can be used for replacement (free and unpinned)
   private int numAvailable;
   //linked list of all the empty frames left in the buffer pool
   private List<Integer> avaliableFrames;
   //hashmap of blocks to frame in the buffer pool
   private Map<Block, Integer> buffers;
   //what replacement policy we should be using
   private String replacementPolicy;
   //for the least recently used policy, the first element in the array should be
   //the least recently used buffer
   private List<Integer> leastRecentlyUsedArray;
   //for clock policy, what frame the clock pointer is pointing to now -- initially
   //starts at zero because we assume we don't use this policy until all buffers filled
   private int clockFrame;
   //what is the max number the clock pointer can get to
   private int maxClockFrame;

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
    * @param policy the replacement policy we should be using
    */
   //made public to unit test -- only reason, otherwise things get a little bit annoying
   public BasicBufferMgr(int numbuffs, String policy) {
      //=====================================CS4432-Project1=====================
      //initialize all values found above
      bufferpool = new Buffer[numbuffs];
      maxClockFrame = numbuffs - 1;
      numAvailable = numbuffs;
      avaliableFrames = new LinkedList<>();
      buffers = new HashMap<>();
      replacementPolicy = policy;
      leastRecentlyUsedArray = new LinkedList<>();
      //assuming we fill up the buffer, we will want to start removing buffers by looking at the first buffer again
      clockFrame = 0;
      //put a new buffer into each spot in the bufferpool
      //also set the buffer ID (frame number) of that buffer
      for (int i=0; i<numbuffs; i++) {
         bufferpool[i] = new Buffer();
         bufferpool[i].setFrameNumber(i);
         //since no blocks start out associated with a buffer, all should be empty at first
         //add each of the frame spots to the linked list of empty frames
         avaliableFrames.add(i);
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
   public synchronized Buffer pin(Block blk) {
      //=====================================CS4432-Project1=====================
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         buff.assignToBlock(blk);
      }
      if (!buff.isPinned()) {
         //because we have the buffer pinned, that buffer is no longer available
         numAvailable--;

         //if we have a replacement policy of LRU, we know that this buffer is no longer valid for
         //replacement, so we can remove it from the array of frame numbers
         if (replacementPolicy.equalsIgnoreCase("LRU")){
            //if we had previously added this buffer to the LRU array, we should remove
            int location = leastRecentlyUsedArray.indexOf(buff.getFrameNumber());
            if (location != -1){
               //remove it
               leastRecentlyUsedArray.remove(location);
            }
         }
      }
      //pin the block to the buffer
      buff.pin();
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
   public synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      //=====================================CS4432-Project1=====================
      //get an empty buffer or buffer that is okay for replacement
      Buffer buff = chooseUnpinnedBuffer();
      //if we returned null, there was no available buffers, so just return null
      if (buff == null)
         return null;
      //add block to that buffer and pin it
      buff.assignToNew(filename, fmtr);
      numAvailable--;
      buff.pin();
      //associate the block with the frame number for easy access
      buffers.put(buff.block(), buff.getFrameNumber());
      return buff;
   }

   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   public synchronized void unpin(Buffer buff) {
      //=====================================CS4432-Project1=====================
      buff.unpin();
      if (!buff.isPinned()) {
         //if the buffer is now unpinned, we should re-add the buffer to the least recently
         //used linked list so that we can use it if need be
         if (replacementPolicy.equalsIgnoreCase("LRU")){
            leastRecentlyUsedArray.add(buff.getFrameNumber());
         }
         //increment the number of available
         numAvailable++;
      }
   }

   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   public int available() {
      return numAvailable;
   }

   /**
    * Finds an existing buffer if it does indeed exist
    * @param blk
    * @return existing buffer based upon given block
    */
   private Buffer findExistingBuffer(Block blk) {
      //=====================================CS4432-Project1=====================
      //lookup the block from the list of all buffers, and if we can find a frame number
      //we should return that buffer
      if (buffers.get(blk) != null){
         //get the frame number
         int frame = buffers.get(blk);

         //since we were looking up the buffer, we need to reorder our LRU array if
         //the block searched for is unpinned
         if (!bufferpool[frame].isPinned()){
            if (replacementPolicy.equalsIgnoreCase("LRU")) {
               int location = leastRecentlyUsedArray.indexOf(frame);
               if (location != -1){
                  //remove and re-add at the end of the array
                  leastRecentlyUsedArray.remove(location);
                  leastRecentlyUsedArray.add(frame);
               }
            }
         }

         //return the existing buffer
         return bufferpool[frame];
      }
      //we could not find the frame number (ie. it does not exist) so return null
      return null;
   }

   /**
    * returns the next available buffer based on certain criteria
    * @return Buffer
    */
   private Buffer chooseUnpinnedBuffer() {
      //=====================================CS4432-Project1=====================
      //if we still have unused buffers, we can use these first
      if (avaliableFrames.size() != 0){
         //look up the first unused buffer, and return that
         int frame = avaliableFrames.get(0);
         avaliableFrames.remove(0);

         return bufferpool[frame];
      } else { //all of our buffers have been used, so we need to use a replacement policy
         //LRU replacement policy
         if (replacementPolicy.equalsIgnoreCase("LRU")){
            //if we actually have frames that we can replace than use those
            if (leastRecentlyUsedArray.size() != 0){
               //get the least recently used frame
               int frame = leastRecentlyUsedArray.get(0);

               //need to remove the association
               Buffer buff = bufferpool[frame];
               buffers.put(buff.block(), null);
               leastRecentlyUsedArray.remove(0);

               //return the buffer in this frame
               return bufferpool[frame];
            }
            //CLOCK replacement policy
         } else if (replacementPolicy.equalsIgnoreCase("CLOCK")) {
            //if we have unpinned buffers is the only time we should run the replacement policy
            if (numAvailable != 0){
               while (true){ //infinite loop
                  //get the current frame that clock policy is pointing to
                  int frame = clockFrame;
                  //increment the pointer
                  if (clockFrame == maxClockFrame){
                     clockFrame = 0;
                  } else {
                     clockFrame += 1;
                  }

                  //if the buffer at this frame is unpinned, we can use it
                  if (!bufferpool[frame].isPinned()){
                     //need to remove the association
                     Buffer buff = bufferpool[frame];
                     buffers.put(buff.block(), null);

                     return bufferpool[frame];
                  }
                  //otherwise we keep going, until we find an available buffer (since we know there is at least one)
               }
            }
         } else { //we are using neither LRU nor Clock, so we'll just do a inefficient lookup to avoid returning null
            for (Buffer buff : bufferpool)
               if (!buff.isPinned()) {
                  buffers.put(buff.block(), null);
                  return buff;
               }
         }
      }

      return null;
   }

   //=====================================CS4432-Project1=====================

   /**
    * Prints out all the buffers in the bufferpool
    * @return all the buffers in the bufferpool in String format
    */
   @Override
   public String  toString() {
      return Arrays.toString(bufferpool);
   }
}