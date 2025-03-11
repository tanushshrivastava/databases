// G25 
// Tanush Shrivastava, Sashenka Gamage, Bani Gulati, Nicholas Vander Woude

#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}


/*
 * Function: allocBuf
 * ------------------
 * Allocates a buffer frame using the clock replacement policy.
 * 
 * Parameters:
 *  - frame: reference to an integer where the allocated frame number will be stored.
 * 
 * Returns:
 *  - OK on success.
 *  - BUFFEREXCEEDED if no frame is available after scanning all frames twice.
 */
const Status BufMgr::allocBuf(int & frame) 
{
    int numScans = 0;
    // Scan until a candidate frame is found
    while (true) {
        advanceClock();  // Move clock hand
        BufDesc* desc = &bufTable[clockHand];

        if (desc->pinCnt > 0) {
            // Cannot replace a pinned page
        } else if (desc->refbit) {
            // Clear reference bit and continue scanning
            desc->refbit = false;
        } else {
            // Found a replaceable candidate
            if (desc->valid) {
                // Write back to disk if dirty
                if (desc->dirty) {
                    Status s = desc->file->writePage(desc->pageNo, &(bufPool[clockHand]));
                    if (s != OK) return s;
                    bufStats.diskwrites++;
                    desc->dirty = false;
                }
                // Remove mapping from hash table
                Status s = hashTable->remove(desc->file, desc->pageNo);
                if (s != OK) return s;
            }
            // Clear descriptor and allocate frame
            desc->Clear();
            frame = clockHand;
            return OK;
        }
        numScans++;

        // If all frames have been scanned twice, return BUFFEREXCEEDED
        if (numScans >= 2 * numBufs)
            return BUFFEREXCEEDED;
    }
}

/*
 * Function: readPage
 * -------------------
 * Reads a page from disk into the buffer pool.
 * 
 * Parameters:
 *  - file: pointer to the file containing the page.
 *  - pageNo: the page number to read.
 *  - page: reference to a Page pointer that will point to the buffer location.
 * 
 * Returns:
 *  - OK on success.
 *  - Error status if any operation fails.
 */
const Status BufMgr::readPage(File* file, const int pageNo, Page*& page)
{
    int frame;
    // Check if the page is already in the buffer pool
    Status s = hashTable->lookup(file, pageNo, frame);
    if (s == OK) {
        // Update pin count and reference bit
        BufDesc* desc = &bufTable[frame];
        desc->pinCnt++;
        desc->refbit = true;
        page = &bufPool[frame];
        return OK;
    }
    else if (s == HASHNOTFOUND) {
        // Page is not in buffer, allocate a new buffer frame
        int frameNum;
        s = allocBuf(frameNum);
        if (s != OK) return s;
        // Read page from disk into allocated frame
        s = file->readPage(pageNo, &bufPool[frameNum]);
        if (s != OK) return s;
        // Insert mapping into hash table
        s = hashTable->insert(file, pageNo, frameNum);
        if (s != OK) return s;
        // Set buffer descriptor
        bufTable[frameNum].Set(file, pageNo);
        page = &bufPool[frameNum];
        bufStats.diskreads++;
        return OK;
    }
    return s;
}

/*
 * Function: unPinPage
 * --------------------
 * Unpins a page, reducing its pin count and marking it as dirty if specified.
 * 
 * Parameters:
 *  - file: pointer to the file containing the page.
 *  - pageNo: the page number to unpin.
 *  - dirty: boolean flag indicating whether the page has been modified.
 * 
 * Returns:
 *  - OK on success.
 *  - PAGENOTPINNED if the page is not pinned.
 */
const Status BufMgr::unPinPage(File* file, const int pageNo, const bool dirty)
{
    int frame;
    Status s = hashTable->lookup(file, pageNo, frame);
    if (s != OK) return s;  // Page not found in buffer pool
    
    BufDesc* desc = &bufTable[frame];
    if (desc->pinCnt <= 0)
        return PAGENOTPINNED;
    
    desc->pinCnt--;
    if (dirty)
        desc->dirty = true;
    
    return OK;
}

/*
 * Function: allocPage
 * --------------------
 * Allocates a new page in the file and assigns a buffer frame for it.
 * 
 * Parameters:
 *  - file: pointer to the file where the page will be allocated.
 *  - pageNo: reference to an integer where the new page number will be stored.
 *  - page: reference to a Page pointer that will point to the buffer location.
 * 
 * Returns:
 *  - OK on success.
 *  - Error status if any operation fails.
 */
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page)
{
    // Allocate a new page in the file
    Status s = file->allocatePage(pageNo);
    if (s != OK) return s;
    
    // Allocate a new buffer frame
    int frame;
    s = allocBuf(frame);
    if (s != OK) return s;
    
    // Insert the new page into the hash table
    s = hashTable->insert(file, pageNo, frame);
    if (s != OK) return s;
    
    // Set up the buffer descriptor
    bufTable[frame].Set(file, pageNo);
    
    // Initialize the new page with zero values
    memset(&bufPool[frame], 0, sizeof(Page));
    
    page = &bufPool[frame];
    bufStats.diskreads++;
    return OK;
}


const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


