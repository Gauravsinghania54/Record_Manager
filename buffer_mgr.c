#include<stdio.h>
#include<stddef.h>
#include<stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <math.h>

// This structure represents one page frame in buffer pool.
typedef struct pageInfo
{
    SM_PageHandle Data;
    PageNumber pageNumber;
    int dirty; //show that the client thread has changed the contents
    int fixcount; // Show no. of threads who are using the page at a particular time
    int leastLru;   // leastLRU taken to identify the least used bit
    
} Frame;


int bufferSize = 0;


int lastIndex = 0;


int writeCount = 0;


int pageHit = 0;


int clockPointer = 0;




// Defining FIFO Function
 void FIFO(BM_BufferPool *const bm, Frame *page)
{
    
    Frame *frame = (Frame *) bm->mgmtData;
    
    int index=0, firstIndex;
    firstIndex = lastIndex % bufferSize;
    
    while(index < bufferSize)
    {
        if(frame[firstIndex].fixcount == 0)
        {
            if(frame[firstIndex].dirty == 1)
            {
                SM_FileHandle fileHandle;
                openPageFile(bm->pageFile, &fileHandle);
                writeBlock(frame[firstIndex].pageNumber, &fileHandle, frame[firstIndex].Data);
                writeCount++;
            }
            
            frame[firstIndex].pageNumber = page->pageNumber;
            frame[firstIndex].Data = page->Data;
            frame[firstIndex].fixcount = page->fixcount;
            frame[firstIndex].dirty = page->dirty;
            
            break;
        }
        else
        {
            firstIndex++;
            if((firstIndex % bufferSize) == 0)
                firstIndex = 0;
        }
        index++;
    }
}


// Defining LRU  function
 void LRU(BM_BufferPool *const bm, Frame *page)
{
    Frame *frame = (Frame *) bm->mgmtData;
    int i, leastIndex=0, least=0;
    
    // Interating through all the page frames in the buffer pool.
    for(i = 0; i < bufferSize; i++)
    {
        // Finding page frame whose fixCount = 0 i.e. no client is using that page frame.
        if(frame[i].fixcount == 0)
        {
            leastIndex = i;
            least = frame[i].leastLru;
            break;
        }
    }
    
    // Finding the page frame having minimum hitNum (i.e. it is the least recently used) page frame
    for(i = leastIndex + 1; i < bufferSize; i++)
    {
        if(frame[i].leastLru < least)
        {
            leastIndex = i;
            least = frame[i].leastLru;
        }
    }
    
    //if the page has been modified on the Least bit index then set dirty=1
    if(frame[leastIndex].dirty == 1)
    {
        SM_FileHandle fh;
        openPageFile(bm->pageFile, &fh);
        writeBlock(frame[leastIndex].pageNumber, &fh, frame[leastIndex].Data);
        
        // Increase the writeCount which records the number of writes done by the buffer manager.
        writeCount++;
    }
    // Setting page frame's content to new page's content
    frame[leastIndex].Data = page->Data;
    frame[leastIndex].pageNumber = page->pageNumber;
    frame[leastIndex].dirty = page->dirty;
    frame[leastIndex].fixcount = page->fixcount;
    frame[leastIndex].leastLru = page->leastLru;
}

// Defining CLOCK function
 void CLOCK(BM_BufferPool *const bm, Frame *page)
{
    
    Frame *frame = (Frame *) bm->mgmtData;
    while(1)
    {
        clockPointer = (clockPointer % bufferSize == 0) ? 0 : clockPointer;
        
        if(frame[clockPointer].leastLru == 0)
        {
            // If page in memory has been modified (dirtyBit = 1), then write page to disk
            if(frame[clockPointer].dirty == 1)
            {
                SM_FileHandle fh;
                openPageFile(bm->pageFile, &fh);
                writeBlock(frame[clockPointer].pageNumber, &fh, frame[clockPointer].Data);
                
                // Increase the writeCount which records the number of writes done by the buffer manager.
                writeCount++;
            }
            
            // Setting page frame's content to new page's content
            frame[clockPointer].Data = page->Data;
            frame[clockPointer].pageNumber = page->pageNumber;
            frame[clockPointer].dirty = page->dirty;
            frame[clockPointer].fixcount = page->fixcount;
            frame[clockPointer].leastLru = page->leastLru;
            clockPointer++;
            break;
        }
        else
        {
            // Incrementing clockPointer so that we can check the next page frame location.
            // We set hitNum = 0 so that this loop doesn't go into an infinite loop.
            frame[clockPointer++].leastLru = 0;
        }
    }
}

// ***** BUFFER POOL FUNCTIONS ***** //

 RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                         const int numPages, ReplacementStrategy strategy,
                         void *stratData)
{
    bm->pageFile = (char *)pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;
    
    
    Frame *page = malloc(sizeof(Frame) * numPages);
    
    // //BufferSize will give the Total pages in Buffer Pool
    bufferSize = numPages;
    int i;
    
    // Intilalizing all pages in buffer pool. The values of fields (variables) in the page is either NULL or 0
    for(i = 0; i < bufferSize; i++)
    {
        page[i].Data = NULL;
        page[i].pageNumber = -1;
        page[i].dirty = 0;
        page[i].fixcount = 0;
        page[i].leastLru = 0;
    }
    
    bm->mgmtData = page;
    writeCount = clockPointer =0;
    return RC_OK;
    
}


 RC shutdownBufferPool(BM_BufferPool *const bm)
{
    Frame *frame = (Frame *)bm->mgmtData;//storing the data into the frame
    
    forceFlushPool(bm); //Before shutting we need to Write all dirty pages back to disk
    
    int i;
    for(i = 0; i < bufferSize; i++)
    {
        //If fixcount!=0  then data has been modified and havent written to the disk
        if(frame[i].fixcount != 0)
        {
            return RC_PINPAGES;
        }
    }
    
    // Freeing the space occupied by Frame
    free(frame);
    bm->mgmtData = NULL;
    return RC_OK;
}	

// This function writes all the dirty pages (having fixCount = 0) to disk
 RC forceFlushPool(BM_BufferPool *const bm)
{
    Frame *frame = (Frame *)bm->mgmtData;
    
    int i;
    //// Storing all dirty pages in memory to page file on disk
    for(i = 0; i < bufferSize; i++)
    {
        if(frame[i].fixcount == 0 && frame[i].dirty == 1) //checking that no threshold is using the pageframe and also if any page is modified.
        {
            SM_FileHandle fh;
            // Opening page file available on disk
            openPageFile(bm->pageFile, &fh);
            // Writing dirty pages to disk
            writeBlock(frame[i].pageNumber, &fh, frame[i].Data);
            // after write block dirty should be 0
            frame[i].dirty = 0;
            // This will show how many times we have written in the disk.
            writeCount++;
        }
    }
    return RC_OK;
}


// ***** PAGE MANAGEMENT FUNCTIONS ***** //

// This function marks the page as dirty indicating that the data of the page has been modified by the client
 RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    Frame *frame = (Frame *)bm->mgmtData;
    
    int i;
    //Checking all the pages in the buffer pool
    for(i = 0; i < bufferSize; i++)
    {
        //If we need mark a page to dirty page then compare and set the dirty bit to 1
        if(frame[i].pageNumber == page->pageNum)
        {
            frame[i].dirty = 1;
            return RC_OK;
        }
    }
    return RC_ERROR;
}

// This function unpins a page from the memory i.e. removes a page from the memory
 RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    Frame *frame = (Frame *)bm->mgmtData;
    
    int i;
    //Checking all the pages in the buffer pool
    for(i = 0; i < bufferSize; i++)
    {
        // If the current page is the page to be unpinned, then decrease fixCount (which means client has completed work on that page) and exit loop
        if(frame[i].pageNumber== page->pageNum)
        {
            frame[i].fixcount--;
            break;
        }
    }
    return RC_OK;
}

// This function writes the contents of the modified pages back to the page file on disk
 RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    Frame *frame = (Frame *)bm->mgmtData;
    
    int i;
    // Iterating through all the pages in the buffer pool
    for(i = 0; i < bufferSize; i++)
    {
        // If the page is to be written to disk, then right the page to the disk
        if(frame[i].pageNumber == page->pageNum)
        {
            SM_FileHandle fh;
            openPageFile(bm->pageFile, &fh);
            writeBlock(frame[i].pageNumber, &fh, frame[i].Data);
            
            //after the content is written to the disk set dirty = 0;
            frame[i].dirty = 0;
            
            // Increase the writeCount which records the number of writes done by the buffer manager.
            writeCount++;
        }
    }
    return RC_OK;
}

 RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    Frame *frame = (Frame *)bm->mgmtData;
    
    if(pageNum < 0)
    {
        return RC_READ_NON_EXISTING_PAGE;
    }
    if (!bm || bm->numPages <= 0)
    {
        return RC_INVALID_BM;
    }
    
    // Checking if buffer pool is empty and then pinning the first page
//    int isBufferEmpty = isBufferPoolEmptyForPinning(frame, bm, page, pageNum);
//    if(isBufferEmpty == 0)
//        return RC_OK;
    if(frame[0].pageNumber == -1)
    {
        // Reading page from disk and initializing page frame's content in the buffer pool
        SM_FileHandle fileHandle;
        openPageFile(bm->pageFile, &fileHandle);
        frame[0].Data = (SM_PageHandle) malloc(PAGE_SIZE);
        ensureCapacity(pageNum, &fileHandle);
        readBlock(pageNum, &fileHandle, frame[0].Data);
        frame[0].pageNumber = pageNum;
        frame[0].fixcount++;
        lastIndex = pageHit = 0;
        frame[0].leastLru = pageHit;
        page->pageNum = pageNum;
        page->data = frame[0].Data;
        
        return RC_OK;
    }
    else
    {
        int index = 0;
        bool isBufferPoolFull = true;
        
        while(index < bufferSize)
        {
            if(frame[index].pageNumber != -1)
            {
                if(frame[index].pageNumber == pageNum)
                {
                    isBufferPoolFull = false;
                    pageHit++;
                    frame[index].fixcount++;
                    
                    if(bm->strategy == RS_CLOCK)
                        frame[index].leastLru = 1;
                    else if(bm->strategy == RS_LRU)
                        frame[index].leastLru = pageHit;
                    
                    
                    page->pageNum = pageNum;
                    page->data = frame[index].Data;
                    
                    clockPointer++;
                    break;
                }
            }
            else {
                SM_FileHandle fileHandle;
                openPageFile(bm->pageFile, &fileHandle);
                frame[index].Data = (SM_PageHandle) malloc(PAGE_SIZE);
                readBlock(pageNum, &fileHandle, frame[index].Data);
                frame[index].pageNumber = pageNum;
                frame[index].fixcount = 1;
                lastIndex++;
                pageHit++;
                
                if(bm->strategy == RS_CLOCK)
                    frame[index].leastLru = 1;
                else if(bm->strategy == RS_LRU)
                    frame[index].leastLru = pageHit;
                
                
                page->pageNum = pageNum;
                page->data = frame[index].Data;
                
                isBufferPoolFull = false;
                break;
            }
            index++;
        }
        
        if(isBufferPoolFull == true)
        {
            Frame *newPageFrame = (Frame *) malloc(sizeof(Frame));
            
            SM_FileHandle fileHandle;
            openPageFile(bm->pageFile, &fileHandle);
            newPageFrame->Data = (SM_PageHandle) malloc(PAGE_SIZE);
            readBlock(pageNum, &fileHandle, newPageFrame->Data);
            newPageFrame->pageNumber = pageNum;
            newPageFrame->dirty = 0;
            newPageFrame->fixcount = 1;
            
            pageHit++;
            lastIndex++;
            
            if(bm->strategy == RS_CLOCK)
                newPageFrame->leastLru = 1;
            else if(bm->strategy == RS_LRU)
                newPageFrame->leastLru = pageHit;
            
            page->pageNum = pageNum;
            page->data = newPageFrame->Data;
            
            switch(bm->strategy)
            {
                case RS_FIFO: // Using FIFO algorithm
                    FIFO(bm, newPageFrame);
                    break;
                    
                case RS_LRU: // Using LRU algorithm
                    LRU(bm, newPageFrame);
                    break;
                    
                case RS_CLOCK: // Using CLOCK algorithm
                    CLOCK(bm, newPageFrame);
                    break;
                    
                case RS_LFU: // Using LFU algorithm
                    printf("\n LRU-k algorithm not implemented");
                    break;
                    
                case RS_LRU_K:
                    printf("\n LRU-k algorithm not implemented");
                    break;
                    
                default:
                    printf("\nAlgorithm Not Implemented\n");
                    break;
            }
        }
        return RC_OK;
    }
}


// ***** STATISTICS FUNCTIONS ***** //

// This function returns an array of page numbers.
 PageNumber *getFrameContents (BM_BufferPool *const bm)
{
    PageNumber *frameContents = malloc(sizeof(PageNumber) * bufferSize);
    Frame *frame = (Frame *) bm->mgmtData;
    
    int i = 0;
    // Repeats through all the pages in the buffer pool and setting frameContents' value to pageNum of the page
    while(i < bufferSize) {
        frameContents[i] = (frame[i].pageNumber != -1) ? frame[i].pageNumber : NO_PAGE;
        i++;
    }
    return frameContents;
}

// This function returns an array of bools, each element represents the dirtyBit of the respective page.
 bool *getDirtyFlags (BM_BufferPool *const bm)
{
    bool *dirtyFlags = malloc(sizeof(bool) * bufferSize);
    Frame *frame = (Frame *)bm->mgmtData;
    
    int i;
    // Repeats through all the pages in the buffer pool and setting dirtyFlags' value to TRUE if page is dirty else FALSE
    for(i = 0; i < bufferSize; i++)
    {
        dirtyFlags[i] = (frame[i].dirty == 1) ? true : false ;
    }
    return dirtyFlags;
}

// This function returns an array of ints (of size numPages) where the ith element is the fix count of the page stored in the ith page frame.
 int *getFixCounts (BM_BufferPool *const bm)
{
    int *fixCounts = malloc(sizeof(int) * bufferSize);
    Frame *frame= (Frame *)bm->mgmtData;
    
    int i = 0;
    // repeats through all the pages in the buffer pool and setting fixCounts' value to page's fixCount
    while(i < bufferSize)
    {
        fixCounts[i] = (frame[i].fixcount != -1) ? frame[i].fixcount : 0;
        i++;
    }
    return fixCounts;
}

// This function returns the number of pages that have been read from disk since a buffer pool has been initialized.
 int getNumReadIO (BM_BufferPool *const bm)
{
    // Adding one because with start rearIndex with 0.
    return (lastIndex + 1);
}

// This function returns the number of pages written to the page file since the buffer pool has been initialized.
 int getNumWriteIO (BM_BufferPool *const bm)
{
    return writeCount;
}



