
#include<stddef.h>
#include<stdio.h>
#include<stdlib.h>

#include<sys/stat.h>
#include<sys/types.h>
#include<unistd.h>
#include<string.h>
#include<math.h>

#include "storage_mgr.h"


FILE *fp;

 void initStorageManager (void) {
    printf("Storage manager initialized");
}

 RC createPageFile (char *fileName) {
    
    fp = fopen(fileName, "w+");
    
    if(fp == NULL) {
        return RC_FILE_NOT_FOUND;
    } else {
        SM_PageHandle empty = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
        
        if(fwrite(empty, sizeof(char), PAGE_SIZE,fp) < PAGE_SIZE)
            printf("file writing failed \n");
        else
            printf("file writing succeeded \n");
        
        // Closing file pointer to clear all file pointers
        fclose(fp);
        
        //Freeing the page handle pointer
        free(empty);
        
        return RC_OK;
    }
}

 RC openPageFile (char *fileName, SM_FileHandle *fHandle) {
    fp = fopen(fileName, "r");
    
    if(fp == NULL) {
        return RC_FILE_NOT_FOUND;
    } else {
        fHandle->fileName = fileName;
        fHandle->curPagePos = 0;
        
        struct stat Info;
        if(fstat(fileno(fp), &Info) < 0)
            return RC_ERROR;
        fHandle->totalNumPages = (int)Info.st_size/ PAGE_SIZE; //dividing the filesize by Pagesize to get total no. of pages
        
        // Closing file pointer to from the memory
        fclose(fp);
        return RC_OK;
    }
}

 RC closePageFile (SM_FileHandle *fHandle) {
    if(fp != NULL) //Checking if the file exists or not
        fp = NULL;
    return RC_OK;
}

 //This function deletes the file permanently
 RC destroyPageFile (char *fileName) {
    fp = fopen(fileName, "r");
    
    if(fp == NULL)
        return RC_FILE_NOT_FOUND;
    
    remove(fileName);
    return RC_OK;
}

 RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    //Verifying the file existence
    if (pageNum > fHandle->totalNumPages || pageNum < 0)
        return RC_READ_NON_EXISTING_PAGE;
    
    fp = fopen(fHandle->fileName, "r");
    
    if(fp == NULL)
        return RC_FILE_NOT_FOUND;
    
    int Seek = fseek(fp, (pageNum * PAGE_SIZE), SEEK_SET); //Seeking the pointer
    if(Seek == 0) {
        if(fread(memPage, sizeof(char), PAGE_SIZE, fp) < PAGE_SIZE)//reading a block of size PAGE_SIZE
            return RC_ERROR;
    } else {
        return RC_READ_NON_EXISTING_PAGE;
    }
    
    fHandle->curPagePos = (int)ftell(fp);
    
    fclose(fp);
    
    return RC_OK;
}

//This function returns the current page position
 int getBlockPos (SM_FileHandle *fHandle) {
    return fHandle->curPagePos;
}

 RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    fp = fopen(fHandle->fileName, "r");
    
    if(fp == NULL) //Verifying the file existence
        return RC_FILE_NOT_FOUND;
    
    int i=0;
    while(i < PAGE_SIZE) {
        char character = fgetc(fp);
        
        if(feof(fp))
            break;
        else
            memPage[i] = character;
    }
     i++;
    
    fHandle->curPagePos = (int)ftell(fp); //Current page position is set
    
    fclose(fp);
    return RC_OK;
}

 RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
   
     //Verifying if the current page position is on the first block; If yes, there will be no previous block to read
    if(fHandle->curPagePos <= PAGE_SIZE) {
        printf("\n First block: Previous block not present.");
        return RC_READ_NON_EXISTING_PAGE;
    } else {
        int currentPage = fHandle->curPagePos / PAGE_SIZE;
        int start = (PAGE_SIZE * (currentPage - 2));
        
        fp = fopen(fHandle->fileName, "r");
        
        if(fp == NULL)
            return RC_FILE_NOT_FOUND;
        
        fseek(fp, start, SEEK_SET);//Seeking position to start of the file
        
        int i;
        for(i = 0; i < PAGE_SIZE; i++) {
            memPage[i] = fgetc(fp);
        }
        
        fHandle->curPagePos = (int)ftell(fp);
        
        fclose(fp);
        return RC_OK;
    }
}

 RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    int currentPage = fHandle->curPagePos / PAGE_SIZE;
    int start = (PAGE_SIZE * (currentPage - 2));
    
    fp = fopen(fHandle->fileName, "r");
    
    if(fp == NULL)
        return RC_FILE_NOT_FOUND;
    
    fseek(fp, start, SEEK_SET); //Seeking file pointer position to start
    
    int i;
    
    for(i = 0; i < PAGE_SIZE; i++) {
        char c = fgetc(fp);
        if(feof(fp))
            break;
        memPage[i] = c;
    }
    
    fHandle->curPagePos = (int)ftell(fp);
    
    fclose(fp);
    return RC_OK;
}

 RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if(fHandle->curPagePos == PAGE_SIZE) {
        printf("\n Last block: Next block not present.");
        return RC_READ_NON_EXISTING_PAGE;
    } else {
        int currentPage = fHandle->curPagePos / PAGE_SIZE;
        int start = (PAGE_SIZE * (currentPage - 2));
        
        fp = fopen(fHandle->fileName, "r");
        
        if(fp == NULL)
            return RC_FILE_NOT_FOUND;
        
        fseek(fp, start, SEEK_SET); //Seeking file pointer position to start
        
        int i;
 
        for(i = 0; i < PAGE_SIZE; i++) {
            char c = fgetc(fp);
            if(feof(fp))
                break;
            memPage[i] = c;
        }
        
        fHandle->curPagePos = (int)ftell(fp);//setting the current position of the file pointer
        
        fclose(fp);
        return RC_OK;
    }
}

 RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    fp = fopen(fHandle->fileName, "r");
    
    if(fp == NULL)
        return RC_FILE_NOT_FOUND;
    
    int start = (fHandle->totalNumPages - 1) * PAGE_SIZE;
    
    fseek(fp, start, SEEK_SET);
    
    int i;

    for(i = 0; i < PAGE_SIZE; i++) {
        char character = fgetc(fp);
        if(feof(fp))
            break;
        memPage[i] = character;
    }
    
    fHandle->curPagePos = (int)ftell(fp);
    
    fclose(fp);
    return RC_OK;
}

 RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {

    if (pageNum > fHandle->totalNumPages || pageNum < 0)
        return RC_WRITE_FAILED;
    
    fp = fopen(fHandle->fileName, "r+");
    
    if(fp == NULL)
        return RC_FILE_NOT_FOUND;
    
    int start = pageNum * PAGE_SIZE;
    
    if(pageNum == 0) {
        fseek(fp, start, SEEK_SET);
        int i;
        for(i = 0; i < PAGE_SIZE; i++)
        {
            if(feof(fp))
                appendEmptyBlock(fHandle);
            fputc(memPage[i], fp);
        }
        
        fHandle->curPagePos = (int)ftell(fp);
        
        fclose(fp);
    } else {
        fHandle->curPagePos = start;
        fclose(fp);
        writeCurrentBlock(fHandle, memPage);
    }
    return RC_OK;
}

 RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    fp = fopen(fHandle->fileName, "r+");
    
    if(fp == NULL)
        return RC_FILE_NOT_FOUND;
    
    appendEmptyBlock(fHandle);
    
    fseek(fp, fHandle->curPagePos, SEEK_SET);
    
    fwrite(memPage, sizeof(char), strlen(memPage), fp);
    
    fHandle->curPagePos = (int)ftell(fp);
    
    fclose(fp);
    return RC_OK;
}


 RC appendEmptyBlock (SM_FileHandle *fHandle) {
    SM_PageHandle empty = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
    
    
    int Seek = fseek(fp, 0, SEEK_END);
    
    if( Seek == 0 ) {
        fwrite(empty, sizeof(char), PAGE_SIZE, fp);
    } else {
        free(empty);
        return RC_WRITE_FAILED;
    }
    

    free(empty);
    
    fHandle->totalNumPages++;
    return RC_OK;
}

 RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) {
    fp = fopen(fHandle->fileName, "a");
    
    if(fp == NULL)
        return RC_FILE_NOT_FOUND;
    
    while(numberOfPages > fHandle->totalNumPages)
        appendEmptyBlock(fHandle);
    
    fclose(fp);
    return RC_OK;
}


