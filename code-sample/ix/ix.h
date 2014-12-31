#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <queue>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

class IX_ScanIterator;
class IXFileHandle;


enum PageType
{
  PrimaryPage,
  OverflowPage
};

class IndexManager {
 public:
  static IndexManager* instance();

  // Create index file(s) to manage an index
  RC createFile(const string &fileName, const unsigned &numberOfPages);

  // Delete index file(s)
  RC destroyFile(const string &fileName);

  // Open an index and returns an IXFileHandle
  RC openFile(const string &fileName, IXFileHandle &ixFileHandle);

  // Close an IXFileHandle. 
  RC closeFile(IXFileHandle &ixfileHandle);


  // The following functions  are using the following format for the passed key value.
  //  1) data is a concatenation of values of the attributes
  //  2) For INT and REAL: use 4 bytes to store the value;
  //     For VarChar: use 4 bytes to store the length of characters, then store the actual characters.

  // Insert an entry to the given index that is indicated by the given IXFileHandle
  RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

  // Delete an entry from the given index that is indicated by the given IXFileHandle
  RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

  // scan() returns an iterator to allow the caller to go through the results
  // one by one in the range(lowKey, highKey).
  // For the format of "lowKey" and "highKey", please see insertEntry()
  // If lowKeyInclusive (or highKeyInclusive) is true, then lowKey (or highKey)
  // should be included in the scan
  // If lowKey is null, then the range is -infinity to highKey
  // If highKey is null, then the range is lowKey to +infinity
  
  // Initialize and IX_ScanIterator to supports a range search
  RC scan(IXFileHandle &ixfileHandle,
      const Attribute &attribute,
	  const void        *lowKey,
      const void        *highKey,
      bool        lowKeyInclusive,
      bool        highKeyInclusive,
      IX_ScanIterator &ix_ScanIterator);

  // Generate and return the hash value (unsigned) for the given key
  unsigned hash(const Attribute &attribute, const void *key);
  
  
  // Print all index entries in a primary page including associated overflow pages
  // Format should be:
  // Number of total entries in the page (+ overflow pages) : ?? 
  // primary Page No.??
  // # of entries : ??
  // entries: [xx] [xx] [xx] [xx] [xx] [xx]
  // overflow Page No.?? liked to [primary | overflow] page No.??
  // # of entries : ??
  // entries: [xx] [xx] [xx] [xx] [xx]
  // where [xx] shows each entry.
  RC printIndexEntriesInAPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const unsigned &primaryPageNumber);
  
  // Get the number of primary pages
  RC getNumberOfPrimaryPages(IXFileHandle &ixfileHandle, unsigned &numberOfPrimaryPages);

  // Get the number of all pages (primary + overflow)
  RC getNumberOfAllPages(IXFileHandle &ixfileHandle, unsigned &numberOfAllPages);
  void Merge(IXFileHandle &ixFileHandle, const Attribute &attribute);
  int getEmptyPageCount(IXFileHandle& ixFileHandle);
 protected:
  IndexManager	();                            // Constructor
  ~IndexManager  ();                            // Destructor
 
  unsigned getBucketID(IXFileHandle &ixFileHandle,const Attribute& attribute, const void* key);
  void Split(IXFileHandle &ixFileHandle, const Attribute &attribute);
  void readPage(PageType pageType, IXFileHandle& ixfileHandle, int pageID, void* buffer);
  void writePage(PageType pageType, IXFileHandle& ixFileHandle, int pageID, void* buffer);
  int appendPageOrReuseEmptyPage(IXFileHandle& ixFileHandle, void* buffer);
  void AddPageIntoEmptyPageList(IXFileHandle& ixFileHandle, int pageID);
private:
  static IndexManager *_index_manager;

};


class IX_ScanIterator {
 public:
  IX_ScanIterator();  							// Constructor
  ~IX_ScanIterator(); 							// Destructor

  RC getNextEntry(RID &rid, void *key);  		// Get next matching entry
  RC close();             						// Terminate index scan

  AttrType type;

  void AddPageInScan(const Attribute &attribute, 
    void* buffer, 
    const void      *lowKey,
    const void      *highKey,
    bool      lowKeyInclusive,
    bool          highKeyInclusive);

  void clear()
  {
    while(!ridQueue.empty())
      ridQueue.pop();
    while(!floatQueue.empty())
      floatQueue.pop();
    while(!intQueue.empty())
      intQueue.pop();
    while(!stringQueue.empty())
      stringQueue.pop();
  }
private:
  queue< RID > ridQueue;
  queue< float > floatQueue;
  queue< int > intQueue;
  queue< string > stringQueue;

};


class IXFileHandle {
public:
	// Put the current counter values of associated PF FileHandles into variables
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    IXFileHandle();  							// Constructor
    ~IXFileHandle(); 							// Destructor
    
    FileHandle pagedFileHandlePrimary;
    FileHandle pagedFileHandleSecondary;

    void getMetaData(int& N, int& level, int& next);
    void setMetaData(int N, int level, int next);
    int getEmptyPagePointer();
    void setEmptyPagePointer(int);
	void setPrimaryPageNum(int pageNum);
private:
    // TODO : why we need these ??????
    unsigned readPageCounter; 
    unsigned writePageCounter;
    unsigned appendPageCounter;
};

// print out the error message for a given return code
void IX_PrintError (RC rc);


class IXPageBufferHelper
{
public:
  static IXPageBufferHelper* instance();
protected:
  IXPageBufferHelper(){}
  ~IXPageBufferHelper(){}

private:
  static IXPageBufferHelper *_buffer_helper;

public:
  int getNextPageID(void* buffer);
  void setNextPageID(void* buffer, int pageID);
  void createEmptyBuffer(void* buffer);
  void setRecordPointerInBucket(void* buffer, int recordPointer, int bucketID);
  int getRecordPointerInBucket(void* buffer, int bucketID);
  void setFreeSpaceStart(void* buffer, int freeSpaceStart);
  int getFreeSpaceStart(void* buffer);
  int getFreeSpaceCount(void* buffer);
  bool tryInsertRecordIntoPageBuffer(void* buffer, void* recordData, int recordLength, AttrType attrType);
  bool tryDeleteRecordFromPageBuffer(void* buffer, void* recordRawData, int recordLength, AttrType attrType);
  unsigned hash_page(const void* data, AttrType type);
  void getAllRecordRawDataPointers(void* buffer, vector<int>& pointerList);
  void getRecordPointerListInBucket(void* buffer, int bucketID,vector<int>& pointerList);
  void printDataInPrimaryPage(void* buffer, const Attribute& attr, int pageID);
  void printDataInOverflowPage(void* buffer, const Attribute& attr, int lastPageID, int pageID);
  void clearPageData(void* buffer);
  bool IsPageEmpty(void* buffer);

};

#endif
