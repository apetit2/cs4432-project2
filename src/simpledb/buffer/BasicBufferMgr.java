package simpledb.buffer;

import simpledb.file.*;

import java.util.*;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
   private Buffer[] bufferpool;
   private int numAvailable;
   private List<Integer> avaliableFrames;
   private Map<Block, Integer> buffers;
   private String replacementPolicy;
   private List<Integer> leastRecentlyUsedArray;
   private int clockFrame;
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
    */
   BasicBufferMgr(int numbuffs, String policy) {
      bufferpool = new Buffer[numbuffs];
      maxClockFrame = numbuffs - 1;
      numAvailable = numbuffs;
      avaliableFrames = new LinkedList<>();
      buffers = new HashMap<>();
      replacementPolicy = policy;
      leastRecentlyUsedArray = new LinkedList<>();
      //assuming we fill up the buffer, we will want to start removing buffers by looking at the first buffer again
      clockFrame = 0;
      for (int i=0; i<numbuffs; i++) {
         bufferpool[i] = new Buffer();
         bufferpool[i].setFrameNumber(i);
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
   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         buff.assignToBlock(blk);
      }
      if (!buff.isPinned()) {
         numAvailable--;

         if (replacementPolicy.equalsIgnoreCase("LRU")){
            //if we had previously added this buffer to the LRU array, we should remove
            int location = leastRecentlyUsedArray.indexOf(buff.getFrameNumber());
            if (location != -1){
               //remove it
               leastRecentlyUsedArray.remove(location);
            }
         }
      }
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
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      buff.assignToNew(filename, fmtr);
      numAvailable--;
      buff.pin();
      buffers.put(buff.block(), buff.getFrameNumber());
      return buff;
   }

   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned()) {
         if (replacementPolicy.equalsIgnoreCase("LRU")){
            leastRecentlyUsedArray.add(buff.getFrameNumber());
         }
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
      if (buffers.get(blk) != null){
         //get the frame number
         int frame = buffers.get(blk);

         //since we were looking up the buffer, we need to reorder our LRU array if
         //the block searched for is unpinned
         if (!bufferpool[frame].isPinned()){
            if (replacementPolicy.equalsIgnoreCase("LRU")) {
               int location = leastRecentlyUsedArray.indexOf(frame);
               if (location != -1){
                  //remove and readd at the end of the array
                  leastRecentlyUsedArray.remove(location);
                  leastRecentlyUsedArray.add(frame);
               }
            }
         }

         //return the existing buffer
         return bufferpool[frame];
      }
      return null;
   }

   private Buffer chooseUnpinnedBuffer() {
      if (avaliableFrames.size() != 0){
         int frame = avaliableFrames.get(0);
         avaliableFrames.remove(0);

         return bufferpool[frame];
      } else {
         if (replacementPolicy.equalsIgnoreCase("LRU")){
            if (leastRecentlyUsedArray.size() != 0){
               int frame = leastRecentlyUsedArray.get(0);

               //need to remove the association
               Buffer buff = bufferpool[frame];
               buffers.put(buff.block(), null);
               leastRecentlyUsedArray.remove(0);

               return bufferpool[frame];
            }

         } else if (replacementPolicy.equalsIgnoreCase("CLOCK")) {
            int frame = clockFrame;
            if (clockFrame == maxClockFrame){
               clockFrame = 0;
            } else {
               clockFrame += 1;
            }

            if (!bufferpool[frame].isPinned()){
               //need to remove the association
               Buffer buff = bufferpool[frame];
               buffers.put(buff.block(), null);

               return bufferpool[frame];
            } else { //if the frame we just looked at is pinned, we need to keep looking for an unpinned frame
               //to avoid an infinite loop, we ONLY want to loop up to the max-number of frames, and if we get to there, then we can return null because no frames are unpinned
               int i = 0;
               while (i != (maxClockFrame - 1)) {
                  if (!bufferpool[clockFrame].isPinned()) {
                     //need to remove the association
                     Buffer buff = bufferpool[frame];
                     buffers.put(buff.block(), null);

                     return bufferpool[clockFrame];
                  }

                  if (clockFrame == maxClockFrame) {
                     clockFrame = 0;
                  } else {
                     clockFrame += 1;
                  }

                  i++;
               }
            }
         } else { //we are using neither LRU nor Clock, so we'll just do a inefficient lookup
            for (Buffer buff : bufferpool)
               if (!buff.isPinned()) {
                  buffers.put(buff.block(), null);
                  return buff;
               }
         }
      }

      return null;
   }

   @Override
   public String  toString() {
      return Arrays.toString(bufferpool);
   }
}