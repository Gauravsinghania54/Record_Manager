-Record Manager-

Project Modules
C source files: dberror.c, test_assign3_1.c, buffer_mgr_stat.c, test_expr.c, record_mgr.c, rm_serializer.c, buffer_mgr.c,storage_mgr.c,expr.c

Header files: storage_mgr.h, dberror.h, test_helper.h, buffer_mgr.h, buffer_mgr_stat.h, dt.h, expr.h, record_mgr.h, tables.h.


Team Members: (Group 4)

1. Gaurav Singhania(A20445477)

2. Chaitanya Nagaram(A20450288)

3. Sachin Devatar(A20443522)


Abstract:

The main purpose is to implement a simple record manager that provides navigation through records, and inserting and deleting and updating and scan through the records in a table.

To Run:

With given default test cases:

Command: make test_assign3_1.c
Run: ./test_assign3_1.c
To remove object files use this command: make clean



--Table and Record Manager Functions--

initRecordManager() :

- Record manager is initialized in this function.
- initStorageManager() function of storage manager is used to initialize the storage manager.

shutdownRecordManager() :

- This function is used to shutdown the record manager.
- By de-allocating the memory space, the resources utilized by the record manager is going to free.

createTable() :

- This function used to create a table with specified name.
- It will initialize the buffer pool by calling initBufferPool() function
- The table contains all the attributes in the schema. 
- The page file is going to create, which is opened using file handler. Then, writes to the particular location of the file and after writing it will close the page file.

openTable() :

- This function opens the table from the memory with parameter "name"
- Here we set the table's meta data to our custom record manager meta data
- Memory space to 'schema' is allocated and the schema's parameters are set
- The schema is stored to the Table Handler

closeTable() :

- In this function it closes the table as indicated by the parameter 'table'
- It will be done by calling shutdownBufferPool function

deleteTable() :

- This function deletes the table with particular name specified by the parameter.
- The data related with the table is deleted by calling the destroyPageFile() function of the storage manager

getNumTuples() :

- This function returns the number of tuples in the table referenced by parameter 'table'

--Record Functions--

insertRecord() :

- It is used to insert a new record at the page and mention its space.
- Page is pinned where empty slot is available and then the record is inserted
- After the insertion process, the page is marked as dirty and then written back to memory 
- Finally the page is unpinned

deleteRecord() :

- This function deletes a record from the page and space is cleared where a new record can be inserted.
- When the page is modified it is marked as dirty.
- Finally the page is going to unpinned as it is no more required

updateRecord() :

- This function is used to update an existing record from the table .
- The page is found where the record is located and that page is pinned in the buffer pool
- After the record is updated, the page is marked as dirty
- Finally the page is going to unpinned


getRecord() :

- This Function retrieve a record with certain record_id passed as an parameter
- Page is pinned and then the record is retrieved
- Finally the page is going to unpinned


--Scan Functions--

startScan() :

- This function is used to scan all the records through given condition.
- This function starts a scan by getting data from the ScanHandle data structure which is passed as an argument to startScan()
- If condition is NULL, we will return with error code RC_CONDITION_NOT_FOUND.

next() :

- This function returns the next record that obeys the condition.
- The function will get the expression condition and checks, if NULL, error condition is returned
- Otherwise it scans all the records, pin the page having that tuple and returns it
- If none of the tuples fulfill the condition, then we will return with RC_RM_NO_MORE_TUPLES.

closeScan() :

- In this function, it will closes the scan operation.
- This function clean out all the sources which is used, by de-allocating the memory consumed by the record manager.

--Schema Functions--

getRecordSize() :

- This function is used to obtain the record size of the particular schema.
- The function counts the size of the record based on the schema, the datatype of each attribute is considered for this calculation


createSchema() :

- This function creates the schema object and assigns the memory for it
- We set the schema's parameters to the parameters passed in the createSchema() function


freeSchema() :

- This function is used to remove the schema from the memory.
- We will de-allocate the memory space occupied by the schema, by removing it from the memory.


--Attribute Functions--

createRecord() :

- This function is used to create a new record which is in the schema.
- It allocates the memory for the record and record data, for creating a new record

freeRecord() :

- This function is used to deallocate the memory allocated for the record

getAttr() :

- This function retrieves an attribute from the given record in the specified schema
- The record, schema and attribute number whose data is to be retrieved is passed through the parameter
- The attribute details are stored back to the location referenced by 'value' passed through the parameter

setAttr() :

- This function sets the attribute value in the record in the specified schema
- The data which are to be stored in the attribute is passed by 'value' parameter
