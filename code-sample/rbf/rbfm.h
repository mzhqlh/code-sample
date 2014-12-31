#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <stdio.h>
#include <iostream>
#include <stdlib.h>
using namespace std;

#include "pfm.h"

// Record ID
typedef struct
{
    unsigned pageNum;
    unsigned slotNum;
} RID;

// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;

struct Attribute {
    string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { EQ_OP = 0,  // =
    LT_OP,      // <
    GT_OP,      // >
    LE_OP,      // <=
    GE_OP,      // >=
    NE_OP,      // !=
    NO_OP       // no condition
} CompOp;

#define NORMAL 0
#define DELETED 1
#define TOMBSTONE 2


struct recordDirectory {
    int status; // 0 means normal, 1 means deleted, 2 means tombstone
    int slot_offset;
    int slot_length;
};



/****************************************************************************
 The scan iterator is NOT required to be implemented for part 1 of the project
 *****************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iteratr to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();



class RBFM_ScanIterator {
public:
    RBFM_ScanIterator()
	{
		next = 0;
	};
    ~RBFM_ScanIterator()
	{
		if (pFileHandle != NULL)
		{
			PagedFileManager* pfm = PagedFileManager::instance();
			pfm->closeFile(*pFileHandle);
		}
		recordId.clear();
		projectedAttributes.clear();
		rdDescriptor.clear();
		next = 0;
	}
    // "data" follows the same format as RecordBasedFileManager::insertRecord()
    RC getNextRecord(RID &rid, void *data);
    RC close() 
	{ 
                PagedFileManager* pfm = PagedFileManager::instance();
                pfm->closeFile(*pFileHandle);
                pFileHandle = NULL;
				
                recordId.clear();
				projectedAttributes.clear();
				rdDescriptor.clear();
				return 0;
	};
	public:
		FileHandle* pFileHandle;
		vector<RID> recordId;
		vector<string> projectedAttributes;
		int next;
		vector<Attribute> rdDescriptor;
};

class RecordBasedFileManager
{
public:
    static RecordBasedFileManager* instance();
    
    RC createFile(const string &fileName);
    
    RC destroyFile(const string &fileName);
    
    RC openFile(const string &fileName, FileHandle &fileHandle);
    
    RC closeFile(FileHandle &fileHandle);
    
    //  Format of the data passed into the function is the following:
    //  1) data is a concatenation of values of the attributes
    //  2) For int and real: use 4 bytes to store the value;
    //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
    //  !!!The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute()
    RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);
    
    //RC insertRecordIntoPage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, int* fieldPosition, int recordLength, RID &rid, int freePage);
    
    RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);
    
    // This method will be mainly used for debugging/testing
    RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);
    
    /**************************************************************************************************************************************************************
     ***************************************************************************************************************************************************************
     IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for part 1 of the project
     ***************************************************************************************************************************************************************
     ***************************************************************************************************************************************************************/
    RC deleteRecords(FileHandle &fileHandle);
    
    RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);
    
    // Assume the rid does not change after update
    RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);
    
    RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string attributeName, void *data);
    
    RC reorganizePage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const unsigned pageNumber);
    
    RC findPage(FileHandle &fileHandle, int recordLength);
    //RC findPage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, int recordLength,int minRecordLength,int minPage);
    
    RC getRecordLength(const vector<Attribute> &recordDescriptor, const void *data);
    
    // scan returns an iterator to allow the caller to go through the results one by one.
    RC scan(FileHandle &fileHandle,
            const vector<Attribute> &recordDescriptor,
            const string &conditionAttribute,// the attribute in a record which is required to satisfy the relation requirement with the value parameter
            const CompOp compOp,                  // comparision type such as "<" and "="
            const void *value,                    // used in the comparison
            const vector<string> &attributeNames, // a list of projected attributes,
            RBFM_ScanIterator &rbfm_ScanIterator);
    
	bool judgeAttribute(const void* conditon, void* attribute, CompOp compOp, AttrType type);
    //bool judgeAttribute(const void* conditon, void* attribute, CompOp compOp, AttrType type);
    
    // Extra credit for part 2 of the project, please ignore for part 1 of the project
public:
    
    RC reorganizeFile(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor);
    int GetVersion(FileHandle& fileHandle, RID rid);
    
protected:
    RecordBasedFileManager();
    ~RecordBasedFileManager();
    
private:
    static RecordBasedFileManager *_rbf_manager;
};

#endif
