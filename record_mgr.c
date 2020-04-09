#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

typedef struct RecordMgr
{
    BM_PageHandle pHandle; // Buffer Manager PageHandle
    BM_BufferPool bPool;
    RID recID; // Identifier for the record
    Expr *scanCondition; // condition used to scan the table
    int recordsCount; //counts the number of records in a table
    int availablePage; // location of first free page is stored
    int scannedRecordCount;// count of number of records scanned
} RecordMgr;

const int MAXIMUM_NUM_OF_PAGES = 100;
const int ATTR_SIZE = 15; // Size of the attribute name

RecordMgr *recordMgr;

//Returns the free slot available in a page
int getAvailableSlot(char *recordData, int recSize)
{
    int index, totalSlots = PAGE_SIZE / recSize;

    for (index = 0; index < totalSlots; index++)
        if (recordData[index * recSize] != '+')
            return index;
    return -1;
}


extern RC initRecordManager (void *mgmtData)
{
    initStorageManager();
    return RC_OK;
}

extern RC shutdownRecordManager ()
{
    recordMgr = NULL;
    free(recordMgr);
    return RC_OK;
}

extern RC createTable (char *name, Schema *schema)
{
    recordMgr = (RecordMgr*) malloc(sizeof(RecordMgr));

    initBufferPool(&recordMgr->bPool, name, MAXIMUM_NUM_OF_PAGES, RS_LRU, NULL);

    char recordData[PAGE_SIZE];
    char *pHandle = recordData;

    int output, index;

    *(int*)pHandle = 0;

    
    pHandle = pHandle + sizeof(int);

    
    *(int*)pHandle = 1;

    
    pHandle = pHandle + sizeof(int);

    
    *(int*)pHandle = schema->numAttr;

    
    pHandle = pHandle + sizeof(int);

    
    *(int*)pHandle = schema->keySize;

    
    pHandle = pHandle + sizeof(int);

      index=0;
      while(index < schema->numAttr)
        {
        
               strncpy(pHandle, schema->attrNames[index], ATTR_SIZE);
               pHandle = pHandle + ATTR_SIZE;

        
               *(int*)pHandle = (int)schema->dataTypes[index];

        
               pHandle = pHandle + sizeof(int);

        
               *(int*)pHandle = (int) schema->typeLength[index];

        
               pHandle = pHandle + sizeof(int);
              index++;
        }

    SM_FileHandle fHandler;

    
    if((output = createPageFile(name)) != RC_OK)
        return output;

    
    if((output = openPageFile(name, &fHandler)) != RC_OK)
        return output;

    
    if((output = writeBlock(0, &fHandler, recordData)) != RC_OK)
        return output;

    
    if((output = closePageFile(&fHandler)) != RC_OK)
        return output;

    return RC_OK;
}


extern RC openTable (RM_TableData *rel, char *name)
{
    SM_PageHandle pHandle;

    int countAttr, index;

    
    rel->name = name;

    
    rel->mgmtData = recordMgr;

    
    pinPage(&recordMgr->bPool, &recordMgr->pHandle, 0);

    
    pHandle = (char*) recordMgr->pHandle.data;

    
    recordMgr->recordsCount= *(int*)pHandle;
    pHandle = pHandle + sizeof(int);

    
    recordMgr->availablePage= *(int*) pHandle;
        pHandle = pHandle + sizeof(int);

    
        countAttr = *(int*)pHandle;
    pHandle = pHandle + sizeof(int);

    Schema *schema;

    
    schema = (Schema*) malloc(sizeof(Schema));

    
    schema->numAttr = countAttr;
    schema->attrNames = (char**) malloc(sizeof(char*) *countAttr);
    schema->typeLength = (int*) malloc(sizeof(int) *countAttr);
    schema->dataTypes = (DataType*) malloc(sizeof(DataType) *countAttr);


    
    index=0;
    while(index<countAttr) {
      schema->attrNames[index]= (char*) malloc(ATTR_SIZE);
      index++;
    }


    index=0;
    while(index < schema->numAttr)
        {
        
        strncpy(schema->attrNames[index], pHandle, ATTR_SIZE);
        pHandle = pHandle + ATTR_SIZE;

        
        schema->dataTypes[index]= *(int*) pHandle;
        pHandle = pHandle + sizeof(int);

        
        schema->typeLength[index]= *(int*)pHandle;
        pHandle = pHandle + sizeof(int);

        index++;
    }

    
    rel->schema = schema;

    
    unpinPage(&recordMgr->bPool, &recordMgr->pHandle);

    
    forcePage(&recordMgr->bPool, &recordMgr->pHandle);

    return RC_OK;
}


extern RC closeTable (RM_TableData *table)
{
    
    RecordMgr *recordMgr = table->mgmtData;

    
    shutdownBufferPool(&recordMgr->bPool);
    
    return RC_OK;
}


extern RC deleteTable (char *name)
{
    
    destroyPageFile(name);
    return RC_OK;
}


extern int getNumTuples (RM_TableData *table)
{
    
    RecordMgr *recordMgr = table->mgmtData;
    return recordMgr->recordsCount;
}




extern RC insertRecord (RM_TableData *rel, Record *record)
{
    
    RecordMgr *recordMgr = rel->mgmtData;

    
    RID *recID = &record->id;

    char *recordData, *slotPtr;

    
    int recSize = getRecordSize(rel->schema);

    
    recID->page = recordMgr->availablePage;

    
    pinPage(&recordMgr->bPool, &recordMgr->pHandle, recID->page);

    
    recordData = recordMgr->pHandle.data;

    
    recID->slot = getAvailableSlot(recordData, recSize);

    for(recID->slot=getAvailableSlot(recordData, recSize);recID->slot==-1;recID->slot=getAvailableSlot(recordData, recSize))
    {
        
        unpinPage(&recordMgr->bPool, &recordMgr->pHandle);

        
        recID->page++;

        
        pinPage(&recordMgr->bPool, &recordMgr->pHandle, recID->page);

        
        recordData = recordMgr->pHandle.data;

    }


    slotPtr = recordData;

    
    markDirty(&recordMgr->bPool, &recordMgr->pHandle);

    
    slotPtr = slotPtr + (recID->slot * recSize);

    
    *slotPtr = '+';

    
    memcpy(++slotPtr, record->data + 1, recSize - 1);

    
    unpinPage(&recordMgr->bPool, &recordMgr->pHandle);

    
    recordMgr->recordsCount++;

    
    pinPage(&recordMgr->bPool, &recordMgr->pHandle, 0);

    return RC_OK;
}


extern RC deleteRecord (RM_TableData *table, RID rid)
{
    
    RecordMgr *recordMgr = table->mgmtData;
    
    
    pinPage(&recordMgr->bPool, &recordMgr->pHandle, rid.page);

    
    recordMgr->availablePage = rid.page;
    
    
    int recSize = getRecordSize(table->schema);
    
    char *dataptr = recordMgr->pHandle.data;


    
    dataptr = dataptr + (rid.slot * recSize);
    
    
    *dataptr = '-';
        
    
    markDirty(&recordMgr->bPool, &recordMgr->pHandle);

    
    unpinPage(&recordMgr->bPool, &recordMgr->pHandle);

    return RC_OK;
}


extern RC updateRecord (RM_TableData *table, Record *record)
{
    
    RecordMgr *recordMgr = table->mgmtData;
    
    
    pinPage(&recordMgr->bPool, &recordMgr->pHandle, record->id.page);

    char *data;

    
    int recSize = getRecordSize(table->schema);

    
    RID id = record->id;

    
    data = recordMgr->pHandle.data;
    data = data + (id.slot * recSize);
    
    
    *data = '+';
    
    
    memcpy(++data, record->data + 1, recSize - 1 );
    
    
    markDirty(&recordMgr->bPool, &recordMgr->pHandle);

    
    unpinPage(&recordMgr->bPool, &recordMgr->pHandle);
    
    return RC_OK;
}



extern RC getRecord (RM_TableData *table, RID rid, Record *record)
{
    
    RecordMgr *recordMgr = table->mgmtData;
    
    
    pinPage(&recordMgr->bPool, &recordMgr->pHandle, rid.page);

    
    int recSize = getRecordSize(table->schema);
    char *ptr = recordMgr->pHandle.data;
    ptr = ptr + (rid.slot * recSize);
    
    if(*ptr != '+')
    {
        
        return RC_TUPLE_NOT_FOUND_WITH_GIVEN_RID;
    }
    else
    {
        
        char *data = record->data;
        
        
        record->id = rid;


        
        memcpy(++data, ptr + 1, recSize - 1);
    }

    
    unpinPage(&recordMgr->bPool, &recordMgr->pHandle);

    return RC_OK;
}


extern RC startScan (RM_TableData *table, RM_ScanHandle *scanHandle, Expr *scanCondition)
{
    
    if (scanCondition == NULL)
    {
        return RC_CONDITION_NOT_FOUND;
    }

    
    openTable(table, "ScanTable");

        RecordMgr *scanMgr;
    RecordMgr *tableMgr;

    
        scanMgr = (RecordMgr*) malloc(sizeof(RecordMgr));
        
    
        scanHandle->mgmtData = scanMgr;
    
    
    scanMgr->recID.slot = 0;
        
    
        scanMgr->recID.page = 1;
        
    
        scanMgr->scanCondition = scanCondition;
    
    
    scanMgr->scannedRecordCount = 0;


        
    
        tableMgr = table->mgmtData;

    
        tableMgr->recordsCount = ATTR_SIZE;

    
        scanHandle->rel= table;

    return RC_OK;
}



extern RC next (RM_ScanHandle *scanHandle, Record *tuple)
{
    
    RecordMgr *scanMgr = scanHandle->mgmtData;
    RecordMgr *tableMgr = scanHandle->rel->mgmtData;
        Schema *schema = scanHandle->rel->schema;
    
    
    if (scanMgr->scanCondition == NULL)
    {
        return RC_CONDITION_NOT_FOUND;
    }

    Value *result = (Value *) malloc(sizeof(Value));
   
    char *data;
       
    
    int recSize = getRecordSize(schema);

    
    int totalSlots = PAGE_SIZE / recSize;


    
    int recordsCount = tableMgr->recordsCount;

    
    if (recordsCount == 0)
        return RC_RM_NO_MORE_TUPLES;

    
    for(int scannedRecordCount=scanMgr->scannedRecordCount;scannedRecordCount <= recordsCount;scannedRecordCount++)
    {
        
        if (scannedRecordCount <= 0)
        {
            
            scanMgr->recID.slot = 0;
            scanMgr->recID.page = 1;

        }
        else
        {
            scanMgr->recID.slot++;

            
            if(scanMgr->recID.slot >= totalSlots)
            {
                scanMgr->recID.slot = 0;
                scanMgr->recID.page++;
            }
        }

        
        pinPage(&tableMgr->bPool, &scanMgr->pHandle, scanMgr->recID.page);
            
        
        data = scanMgr->pHandle.data;

        
        data = data + (scanMgr->recID.slot * recSize);
        
        
        tuple->id.page = scanMgr->recID.page;
        tuple->id.slot = scanMgr->recID.slot;

        
        char *dataPointer = tuple->data;

        
        *dataPointer = '-';
        
        memcpy(++dataPointer, data + 1, recSize - 1);

        
        scanMgr->scannedRecordCount++;

        
        evalExpr(tuple, schema, scanMgr->scanCondition, &result);

        
        if(result->v.boolV == TRUE)
        {
            
            unpinPage(&tableMgr->bPool, &scanMgr->pHandle);
            
            return RC_OK;
        }
    }
    
    
    unpinPage(&tableMgr->bPool, &scanMgr->pHandle);
    
    
    scanMgr->recID.page = 1;
    scanMgr->scannedRecordCount = 0;
    scanMgr->recID.slot = 0;
    
    
    
    return RC_RM_NO_MORE_TUPLES;
}


extern RC closeScan (RM_ScanHandle *scanHandle)
{
    RecordMgr *scanMgr = scanHandle->mgmtData;
    RecordMgr *recordMgr = scanHandle->rel->mgmtData;

    
    if(scanMgr->scannedRecordCount > 0)
    {
        
        unpinPage(&recordMgr->bPool, &scanMgr->pHandle);
        
        
        scanMgr->scannedRecordCount = 0;
        scanMgr->recID.slot=0;
        scanMgr->recID.page = 1;
        
    }
    
    
        scanHandle->mgmtData = NULL;
        free(scanHandle->mgmtData);
    
    return RC_OK;
}



extern int getRecordSize (Schema *schema)
{
    int size = 0, i=0;
    
    
    while( i < schema->numAttr)
    {
        
        if(schema->dataTypes[i]==DT_STRING)
        {
            size= size+ schema->typeLength[i];
        }
        
        else if(schema->dataTypes[i]==DT_FLOAT)
        {
            size= size + sizeof(float);
        }
        else if(schema->dataTypes[i]==DT_INT)
        {
            size = size + sizeof(int);
        }
        else{
            size = size + sizeof(bool);
        }
       i++;
    }
    return ++size;
}


extern Schema *createSchema (int attrNum, char **attributeNames, DataType *types, int *typeSize, int keyLength, int *keys)
{
    
    Schema *schema = (Schema *) malloc(sizeof(Schema));
    
    schema->numAttr = attrNum;
    
    schema->attrNames = attributeNames;
    
    schema->dataTypes = types;
    
    schema->typeLength = typeSize;
    
    schema->keySize = keyLength;
    
    schema->keyAttrs = keys;

    return schema;
}


extern RC freeSchema (Schema *schema)
{
    
    free(schema);
    return RC_OK;
}





extern RC createRecord (Record **record, Schema *schema)
{
    
    Record *newRec = (Record*) malloc(sizeof(Record));
    
    
    int recordlength = getRecordSize(schema);

    
    newRec->data= (char*) malloc(recordlength);

    
    newRec->id.page = newRec->id.slot = -1;

    
    char *Pointer = newRec->data;
    
    
    *Pointer = '-';
    
    
    *(++Pointer) = '\0';

    
    *record = newRec;

    return RC_OK;
}





extern RC freeRecord (Record *record)
{
    
    free(record);
    return RC_OK;
}


extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
    int offset = 0;

    
    int i=0,*res= &offset;
    *res=1;
    

    
    while( i < attrNum)
    {
        
        if(schema->dataTypes[i]==DT_STRING)
        {
            *res= *res+ schema->typeLength[i];
        }
        else if(schema->dataTypes[i]==DT_INT)
        {
            *res = *res + sizeof(int);
        }
        else if(schema->dataTypes[i]==DT_FLOAT)
        {
            *res= *res + sizeof(float);
        }
        else{
            *res = *res + sizeof(bool);
        }
       i++;
    }
    

    
    Value *element = (Value*) malloc(sizeof(Value));

    
    char *Pointer = record->data;
    
    
    Pointer = Pointer + offset;

    
    schema->dataTypes[attrNum] = (attrNum == 1) ? 1 : schema->dataTypes[attrNum];
    
    
    switch(schema->dataTypes[attrNum])
    {
        case DT_STRING:
        {
                 
            int length = schema->typeLength[attrNum];
            
            element->v.stringV = (char *) malloc(length + 1);

            
            strncpy(element->v.stringV, Pointer, length);
            element->v.stringV[length] = '\0';
            element->dt = DT_STRING;
                  break;
        }

        case DT_INT:
        {
            
            int value = 0;
            memcpy(&value, Pointer, sizeof(int));
            element->v.intV = value;
            element->dt = DT_INT;
                  break;
        }
    
        case DT_FLOAT:
        {
            
              float value;
              memcpy(&value, Pointer, sizeof(float));
              element->v.floatV = value;
            element->dt = DT_FLOAT;
            break;
        }

        case DT_BOOL:
        {
            
            bool value;
            memcpy(&value,Pointer, sizeof(bool));
            element->v.boolV = value;
            element->dt = DT_BOOL;
                  break;
        }

        default:
            break;
    }

    *value = element;
    return RC_OK;
}


extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
    int offset = 0;

    
    int i=0,*res= &offset;
    *res=1;
    

    
    while( i < attrNum)
    {
        
        if(schema->dataTypes[i]==DT_STRING)
        {
            *res= *res+ schema->typeLength[i];
        }
        else if(schema->dataTypes[i]==DT_INT)
        {
            *res = *res + sizeof(int);
        }
        else if(schema->dataTypes[i]==DT_FLOAT)
        {
            *res= *res + sizeof(float);
        }
        else{
            *res = *res + sizeof(bool);
        }
       i++;
    }
    

    
    char *dataPointer = record->data;
    
    
    dataPointer = dataPointer + offset;
        
    switch(schema->dataTypes[attrNum])
    {
        case DT_STRING:
        {
            
            
            int length = schema->typeLength[attrNum];

            
            strncpy(dataPointer, value->v.stringV, length);
            dataPointer = dataPointer + schema->typeLength[attrNum];
              break;
        }

        case DT_INT:
        {
            
            *(int *) dataPointer = value->v.intV;
            dataPointer = dataPointer + sizeof(int);
              break;
        }
        
        case DT_FLOAT:
        {
            
            *(float *) dataPointer = value->v.floatV;
            dataPointer = dataPointer + sizeof(float);
            break;
        }
        
        case DT_BOOL:
        {
            
            *(bool *) dataPointer = value->v.boolV;
            dataPointer = dataPointer + sizeof(bool);
            break;
        }

        default:
            break;
    }
    return RC_OK;
}
