
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <map>
#include "../rbf/rbfm.h"
#include "../ix/ix.h"

using namespace std;


# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
// The way to use it is like the following:
//  RM_ScanIterator rmScanIterator;
//  rm.open(..., rmScanIterator);
//  while (rmScanIterator(rid, data) != RM_EOF) {
//    process the data;
//  }
//  rmScanIterator.close();

class IX_ScanIterator;

class RM_ScanIterator {
public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data); 
  RC close();
public:
  RBFM_ScanIterator rbfm_Iterator;
};

class RM_IndexScanIterator {
 public:
  RM_IndexScanIterator() {}   // Constructor
  ~RM_IndexScanIterator() {}  // Destructor

  IX_ScanIterator ixIterator;
  // "key" follows the same format as in IndexManager::insertEntry()
  RC getNextEntry(RID &rid, void *key) { return ixIterator.getNextEntry(rid, key);}   // Get next matching entry
  RC close() {ixIterator.close(); return 0;}                   // Terminate index scan
};

struct IndexFileInfo
{
  string tableName;
  string keyName;
  string fileName;
  RID rid;
};

// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  bool isSystemRequest(const string tableName);

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuples(const string &tableName);

  RC deleteTuple(const string &tableName, const RID &rid);

  // Assume the rid does not change after update
  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  RC reorganizePage(const string &tableName, const unsigned pageNumber);

  // scan returns an iterator to allow the caller to go through the results one by one. 
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);
  void AddTableInColumns(const string &tableName, const vector<Attribute> &attrs);
  void AddTableInTables(const string &tableName);

// Extra credit
public:
  RC dropAttribute(const string &tableName, const string &attributeName);

  RC addAttribute(const string &tableName, const Attribute &attr);

  RC reorganizeTable(const string &tableName);


  RC createIndex(const string &tableName, const string &attributeName);

  RC destroyIndex(const string &tableName, const string &attributeName);

  // indexScan returns an iterator to allow the caller to go through qualified entries in index
  RC indexScan(const string &tableName,
                        const string &attributeName,
                        const void *lowKey,
                        const void *highKey,
                        bool lowKeyInclusive,
                        bool highKeyInclusive,
                        RM_IndexScanIterator &rm_IndexScanIterator
       );
protected:
  void getAttributeData(const void* data, string tableName, string attributeName, void* attributeData, Attribute& att);

  void SetTableVersion(const string& tableName, int version);
  int GetTableVersion(const string& tableName);
  RelationManager();
  ~RelationManager();
  map<string, vector<RID> > map_columns;
  map<string, RID> map_tables;
  map<string, int> map_versions;
  void LoadColumns();
  void LoadTables();
  void LoadIndexCatalog();
  const string columnsFileName;
  const string tablesFileName;
  const string indexCatalogFileName;
  void GetAttributesInVersion(int version, const string& tableName, vector<Attribute>& attrs);
  Attribute getAttributeFromData(void* data);
  void ConvertData(string tableName, int sourceVersion, int targetVersion, void* sourceData, void* targetData);
  bool IsTableSystem(const string tableName);
  Attribute getAttributeByName(const string& tableName, const string& attributeName);

private:
  static RelationManager *_rm;
  //void AddTableInColumns(const string &tableName, const vector<Attribute> &attrs);
  //void AddTableInTables(const string &tableName);
  RC insertTupleInner(const string &tableName, const void *data, RID &rid);
  RC deleteTupleInner(const string &tableName, const RID &rid);
  RC updateTupleInner(const string &tableName, const void *data, const RID &rid);
  map<string, vector<IndexFileInfo> > map_indexFiles;
  
};

#endif
