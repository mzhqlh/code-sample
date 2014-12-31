#include <sys/stat.h>
#include "rm.h"
#include "../ix/ix.h"
RelationManager* RelationManager::_rm = 0;


RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
	return rbfm_Iterator.getNextRecord(rid, data);
} 

RC RM_ScanIterator::close()
{
	return rbfm_Iterator.close();
}

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

void getIndexCatalogAttributes(vector<Attribute>& attrs)
{
	Attribute attribute;
	attribute.name = "table_name";
	attribute.length = 30;
	attribute.type = TypeVarChar;
	attrs.push_back(attribute);

	attribute.name = "index_key";
	attribute.length = 30;
	attribute.type = TypeVarChar;
	attrs.push_back(attribute);

	attribute.name = "fileName";
	attribute.length = 30;
	attribute.type = TypeVarChar;
	attrs.push_back(attribute);
}

/// Create attributes for table "Tables"
void CreateTablesAttributes(vector<Attribute>& attrs)
{
	Attribute attribute;
	attribute.name = "table_name";
	attribute.length = 30;
	attribute.type = TypeVarChar;
	attrs.push_back(attribute);


	attribute.name = "table_id";
	attribute.length = sizeof(int);
	attribute.type = TypeInt;
	attrs.push_back(attribute);

	attribute.name = "version";
	attribute.length = sizeof(int);
	attribute.type = TypeInt;
	attrs.push_back(attribute);

	attribute.name = "table_type";
	attribute.length = 255;
	attribute.type = TypeVarChar;
	attrs.push_back(attribute);
}

/// Create attributes for table "Columns"
void CreateColumnsAttributes(vector<Attribute>& attrs)
{
	Attribute attribute;
	attribute.name = "column_name";
	attribute.length = 30;
	attribute.type = TypeVarChar;
	attrs.push_back(attribute);

	attribute.name = "table_name";
	attribute.length = 30;
	attribute.type = TypeVarChar;
	attrs.push_back(attribute);

	attribute.name = "position";
	attribute.length = sizeof(int);
	attribute.type = TypeInt;
	attrs.push_back(attribute);

	attribute.name = "type";
	attribute.length = 30;
	attribute.type = TypeVarChar;
	attrs.push_back(attribute);

	attribute.name = "version";
	attribute.length = sizeof(int);
	attribute.type = TypeInt;
	attrs.push_back(attribute);

	
}

/// Get the current version for given table
int RelationManager::GetTableVersion(const string& tableName)
{
	return map_versions[tableName];
}

void RelationManager::SetTableVersion(const string& tableName, int version)
{	
	RID rid = map_tables[tableName];
	void* data = malloc(PAGE_SIZE);
	char* p = (char*)data;

	readTuple(tablesFileName, rid, p);

	int lenTableName = *(int*)p;
	p += sizeof(int) + lenTableName;

	p += sizeof(int); // table id

	*(int*)p = version; // update version

	updateTupleInner(tablesFileName, data, rid);

	map_versions[tableName] = version; // update map
	free(data);
}

/// Get attribute of table in specified version
void RelationManager::GetAttributesInVersion(int version, const string& tableName, vector<Attribute>& attrs)
{
	if(tableName == columnsFileName)
	{
		CreateColumnsAttributes(attrs);
		return;
	}

	if(tableName == tablesFileName)
	{
		CreateTablesAttributes(attrs);
		return;
	}

	int len = map_columns[tableName].size();
	void* buffer = malloc(PAGE_SIZE);
	
	for(int i=0; i<len; i++)
	{
		RID rid = map_columns[tableName][i];
		readAttribute(columnsFileName, rid, "version", buffer);
		
		int v = *(int*)buffer;
		if(v == version)
		{
			readTuple(columnsFileName, rid, buffer);
			Attribute attr = getAttributeFromData(buffer);
			attrs.push_back(attr);
		}		
	}
	free(buffer);
}

void RelationManager::LoadTables()
{
	FileHandle fileHandle;
	RecordBasedFileManager::instance()->openFile(tablesFileName, fileHandle);

	vector<Attribute> attrs;
	CreateTablesAttributes(attrs);

	vector<string> attr_names;
	attr_names.push_back("table_name");
	attr_names.push_back("table_id");
	attr_names.push_back("version");
	attr_names.push_back("table_type");

	RBFM_ScanIterator iterator;
	RecordBasedFileManager::instance()->scan(fileHandle, attrs, "table_name", NO_OP, NULL, attr_names, iterator);
	
	RID rid;
	void* buffer = malloc(PAGE_SIZE);
	while(RBFM_EOF != iterator.getNextRecord(rid, buffer))
	{
		char* p = (char*)buffer;
		int lenTableName = *(int*)p;
		p += sizeof(int);

		string tableName = string(p, lenTableName);
		p += lenTableName;

		map_tables[tableName] = rid;

		p+=sizeof(int);

		int version = *(int*)p;
		map_versions[tableName] = version;
	}
	free(buffer);

	RecordBasedFileManager::instance()->closeFile(fileHandle);
}

void RelationManager::LoadColumns()
{
	FileHandle fileHandle;
	//fileHandle.version = GetTableVersion(columnsFileName);
	RecordBasedFileManager::instance()->openFile(columnsFileName, fileHandle);

	vector<Attribute> attrs;
	CreateColumnsAttributes(attrs);

	vector<string> attr_names;
	attr_names.push_back("column_name");
	attr_names.push_back("table_name");
	attr_names.push_back("position");
	attr_names.push_back("type");
	attr_names.push_back("version");

	RBFM_ScanIterator iterator;
	RecordBasedFileManager::instance()->scan(fileHandle, attrs, "column_name", NO_OP, NULL, attr_names, iterator);
	
	RID rid;
	void* buffer = malloc(PAGE_SIZE);
	while(RBFM_EOF != iterator.getNextRecord(rid, buffer))
	{	
		// TODO: Need refactoring !!!! most of code below is only to find tableName ....
		char* p = (char*)buffer;
		int lenColumnName = *(int*)p;
		p += sizeof(int);

		string str_column_name = string(p, lenColumnName);
		p += lenColumnName;

		int lenTableName = *(int*)p;
		p += sizeof(int);

		string str_table_name = string(p, lenTableName);
		p += lenTableName;

		int position = *(int*)p;

		if(map_columns.count(str_table_name) == 0)
		{
			map_columns[str_table_name] = vector<RID>();
		}

		while(map_columns[str_table_name].size() <= position)
			map_columns[str_table_name].push_back(RID());
		
		map_columns[str_table_name][position] = rid;
	}

	free(buffer);

	RecordBasedFileManager::instance()->closeFile(fileHandle);
}


void RelationManager::LoadIndexCatalog()
{
	vector<Attribute> indexCatalogAttrs;
	getIndexCatalogAttributes(indexCatalogAttrs);

	vector<string> indexFileNames;
	for(unsigned i=0; i<indexCatalogAttrs.size(); i++)
	{
		indexFileNames.push_back(indexCatalogAttrs[i].name);
	}

	RM_ScanIterator iterator;
	string n = "table_name";
	scan(indexCatalogFileName, n, NO_OP, NULL, indexFileNames, iterator);



	RID rid;
	void* data = malloc(PAGE_SIZE);
	while(iterator.getNextTuple(rid, data) == 0)
	{
		char* p = (char*)data;
		int tableNameLength = *(int*)p;
		p += sizeof(int);

		string tableName = string(p, tableNameLength);
		p += tableNameLength;

		int keyLength = *(int*)p;
		p += sizeof(int);

		string indexKey = string(p, keyLength);
		p += keyLength;

		int fileNameLength = *(int*)p;
		p += sizeof(int);

		string fileName = string(p, fileNameLength);
		p += fileNameLength;

		IndexFileInfo info;
		info.tableName = tableName;
		info.keyName = indexKey;
		info.fileName = fileName;
		info.rid = rid;
		map_indexFiles[tableName].push_back(info);
	}
	free(data);
}

void CreateTableData(string tableName, int tableID, int version, void* data, string tableType)
{
	char* p = (char*)data;

	int lenTableName = tableName.length();
	*(int*)p = lenTableName;
	p += sizeof(int);

	memcpy(p, tableName.c_str(), lenTableName);
	p += lenTableName;

	*(int*)p = tableID; 
	p += sizeof(int);

	*(int*)p = version;
	p += sizeof(int);

	int tableTypeLen = tableType.length();
	*(int*)p = tableTypeLen;
	p += sizeof(int);

	memcpy(p, tableType.c_str(), tableTypeLen);
	p += tableTypeLen;
}



RelationManager::RelationManager() : columnsFileName ("Columns"), tablesFileName("Tables"), indexCatalogFileName("indexCatalog")
{
	struct stat stFileInfo;

	if(stat(tablesFileName.c_str(), &stFileInfo) != 0)
	{
		RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
		rbfm->createFile(tablesFileName);
		//rbfm->Flag = 1;
		void* data = malloc(PAGE_SIZE);
		RID rid;
		//tablesFileName->Flag = 1; 
		CreateTableData(tablesFileName, 0, 0, data,"System");
		insertTuple(tablesFileName, data, rid);

		CreateTableData(columnsFileName, 1, 0, data,"System");
		insertTuple(tablesFileName, data, rid);

		free(data);
	}
	
	if(stat(columnsFileName.c_str(),&stFileInfo) != 0)
	{
		RecordBasedFileManager::instance()->createFile(columnsFileName);
	}

	LoadTables();
	LoadColumns();
	
	if(stat(indexCatalogFileName.c_str(), &stFileInfo) != 0)
	{
		vector<Attribute> attrs;
		getIndexCatalogAttributes(attrs);
		createTable(indexCatalogFileName, attrs);	
	}
	else 
	{
		LoadIndexCatalog();
	}
}

RelationManager::~RelationManager()
{
}

//bool RelationManager::IsTableSystem(const string & tableName)
//{
	//return tableName == columnsFileName || tableName == tablesFileName;
//}

void CreateColumnData(Attribute attr, void* data, int position, const string& tableName, int version)
{
	char* buffer = (char*)data;
	int lenName = attr.name.length();
	*(int*)buffer = lenName;
	buffer += sizeof(int);
	memcpy(buffer, attr.name.c_str(), lenName);
	buffer+=lenName;

	int lenTableName = tableName.length();
	*(int*)buffer = lenTableName;
	buffer += sizeof(int);
	memcpy(buffer, tableName.c_str(), lenTableName);
	buffer+=lenTableName;

	*(int*)buffer = position;
	buffer += sizeof(int);

	
	string type = "";
	if(attr.type == TypeInt)
		type = "INT";
	else if(attr.type == TypeReal)
		type = "REAL";
	else if(attr.type == TypeVarChar)
		type = "VARCHAR";
	*(int*)buffer = type.length();
	buffer += sizeof(int);
	memcpy(buffer, type.c_str(), type.length());
	buffer+=type.length();

	*(int*)buffer = version;
	buffer += sizeof(int);
}

void RelationManager::AddTableInColumns(const string &tableName, const vector<Attribute> &attrs)
{
	map_columns[tableName] = vector<RID>();

	vector<Attribute> columns_attrs;
	CreateColumnsAttributes(columns_attrs);

	void* data = malloc(PAGE_SIZE);
	for(int i=0; i<attrs.size(); i++)
	{
		Attribute attr = attrs[i];

		CreateColumnData(attr, data, i, tableName, 0);

		RID rid;
		insertTupleInner(columnsFileName, data, rid);

		map_columns[tableName].push_back(rid);
	}
	free(data);
}

void RelationManager::AddTableInTables(const string &tableName)
{
	void* data = malloc(PAGE_SIZE);
	
	CreateTableData(tableName, map_tables.size(), 0, data, "User");

	RID rid;
	insertTupleInner(tablesFileName, data, rid);

	map_tables[tableName] = rid;
	free(data);
}

Attribute RelationManager::getAttributeFromData(void* data)
{
	Attribute att;

	char* buffer = (char*)data;
	int lenName = *(int*)buffer;
	buffer += sizeof(int);
	att.name = string(buffer, lenName);
	buffer += lenName;

	int lenTableName = *(int*)buffer;
	buffer += sizeof(int);
	buffer += lenTableName;

	att.length = *(int*)buffer;
	buffer += sizeof(int);

	int lenType = *(int*)buffer;
	buffer += sizeof(int);
	string type = string(buffer, lenType);
	buffer += lenType;

	if(type == "INT")
		att.type = TypeInt;
	else if(type == "REAL")
		att.type = TypeReal;
	else if(type == "VARCHAR")
		att.type = TypeVarChar;

	return att;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	RC rc = RecordBasedFileManager::instance()->createFile(tableName.c_str());
	if(rc != 0)
		return rc;	
	
	AddTableInColumns(tableName, attrs);
	AddTableInTables(tableName);
    return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
	int len = map_columns[tableName].size();

	for(int i=0; i<len; i++)
	{
		RID rid = map_columns[tableName][i];
		deleteTupleInner(columnsFileName, rid);
	}

	deleteTupleInner(tablesFileName, map_tables[tableName]);

	map<string, vector<RID> >::iterator columnIter = map_columns.find(tableName);
	map_columns.erase(columnIter);

	map<string, RID>::iterator tableIter = map_tables.find(tableName);
	map_tables.erase(tableIter);

	return RecordBasedFileManager::instance()->destroyFile(tableName);
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	if(tableName == columnsFileName)
	{
		CreateColumnsAttributes(attrs);
		return 0;
	}

	if(tableName == tablesFileName)
	{
		CreateTablesAttributes(attrs);
		return 0;
	}

	int version = GetTableVersion(tableName);
	GetAttributesInVersion(version, tableName, attrs);
	return 0;
}

void RelationManager::getAttributeData(const void* data, string tableName, string attributeName, void* attributeData, Attribute& att)
{
	vector<Attribute> attrs;
	getAttributes(tableName, attrs);

	char* p = (char*)data;
	for(int i=0; i<attrs.size(); i++)
	{
		if(attrs[i].name == attributeName)
		{
			att = attrs[i];
			if(attrs[i].type == TypeVarChar)
			{
				memcpy(attributeData, p, *(int*)p + sizeof(int));
			}
			else
			{
				memcpy(attributeData, p, sizeof(int));
			}
		}

		if(attrs[i].type == TypeVarChar)
			p += *(int*)p;
		p += sizeof(int);
	}
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	if(IsTableSystem(tableName))
		return -1;
	RC rc = insertTupleInner(tableName, data, rid);
	if(rc != 0)
		return rc;
	void* attributeData = malloc(PAGE_SIZE);
	Attribute keyAttribute;

	for(int i=0; i<map_indexFiles[tableName].size(); i++)
	{
		IndexFileInfo info = map_indexFiles[tableName][i];

		getAttributeData(data, tableName, info.keyName, attributeData, keyAttribute);

		IXFileHandle fileHandle;
		IndexManager::instance()->openFile(info.fileName, fileHandle);

		IndexManager::instance()->insertEntry(fileHandle, keyAttribute, attributeData, rid);

		IndexManager::instance()->closeFile(fileHandle);

	}
	free(attributeData);
	return 0;
}

RC RelationManager::insertTupleInner(const string &tableName, const void *data, RID &rid)
{
	vector<Attribute> attrs;
	getAttributes(tableName, attrs);

	FileHandle fileHandle;
	fileHandle.version = GetTableVersion(tableName);
	RC rc = RecordBasedFileManager::instance()->openFile(tableName, fileHandle);
	if(rc != 0)
	{
		return rc;
	}

	fileHandle.version = GetTableVersion(tableName); // TODO: temp solution
	rc = RecordBasedFileManager::instance()->insertRecord(fileHandle, attrs, data, rid);
	if(rc != 0)
	{
		RecordBasedFileManager::instance()->closeFile(fileHandle);
		return rc;
	}

	rc = RecordBasedFileManager::instance()->closeFile(fileHandle);
    return rc;
}

RC RelationManager::deleteTuples(const string &tableName)
{
	if(IsTableSystem(tableName))
		return -1;
	FileHandle fileHandle;
	fileHandle.version = GetTableVersion(tableName);
	RecordBasedFileManager::instance()->openFile(tableName, fileHandle);
	RC rc = RecordBasedFileManager::instance()->deleteRecords(fileHandle);
	RecordBasedFileManager::instance()->closeFile(fileHandle);
    return rc;
}

RC RelationManager::deleteTupleInner(const string &tableName, const RID &rid)
{
	vector<Attribute> attrs;
	getAttributes(tableName, attrs);

	FileHandle fileHandle;
	fileHandle.version = GetTableVersion(tableName);
	RecordBasedFileManager::instance()->openFile(tableName, fileHandle);
	void* data = malloc(PAGE_SIZE);
	RecordBasedFileManager::instance()->readRecord(fileHandle, attrs, rid, data);
	RecordBasedFileManager::instance()->deleteRecord(fileHandle, attrs, rid);
	RecordBasedFileManager::instance()->closeFile(fileHandle);

	void* attributeData = malloc(PAGE_SIZE);
	Attribute keyAttribute;
	for(int i=0; i<map_indexFiles[tableName].size(); i++)
	{
		IXFileHandle ixFileHandle;
		IndexFileInfo indexFileInfo = map_indexFiles[tableName][i];

		IndexManager::instance()->openFile(indexFileInfo.fileName, ixFileHandle);

		getAttributeData(data, tableName, indexFileInfo.keyName, attributeData, keyAttribute);

		IndexManager::instance()->deleteEntry(ixFileHandle, keyAttribute, attributeData, rid);

		IndexManager::instance()->closeFile(ixFileHandle);
	}

	free(attributeData);
	free(data);

    return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	if(IsTableSystem(tableName))
		return -1;
	return deleteTupleInner(tableName, rid);
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	if(IsTableSystem(tableName))
		return -1;
	return updateTupleInner(tableName, data, rid);
}

RC RelationManager::updateTupleInner(const string &tableName, const void *data, const RID &rid)
{
	vector<Attribute> attrs;
	getAttributes(tableName, attrs);
	
	FileHandle fileHandle;
	fileHandle.version = GetTableVersion(tableName);
	RecordBasedFileManager::instance()->openFile(tableName, fileHandle);
	RecordBasedFileManager::instance()->updateRecord(fileHandle, attrs, data, rid);
	RecordBasedFileManager::instance()->closeFile(fileHandle);
    return 0;
}


void RelationManager::ConvertData(string tableName, int sourceVersion, int targetVersion, void* sourceData, void* targetData)
{
	vector<Attribute> sourceAttrs; 
	GetAttributesInVersion(sourceVersion, tableName, sourceAttrs);

	vector<Attribute> targetAttrs;
	GetAttributesInVersion(targetVersion, tableName, targetAttrs);

	char* pSource = (char*) sourceData;
	char* pTarget = (char*) targetData;
	for(int i=0; i<targetAttrs.size(); i++)
	{
		pSource = (char*) sourceData;
		bool found = false;
		for(int j=0; j<sourceAttrs.size(); j++)
		{
			Attribute sourceAttr = sourceAttrs[j];
			Attribute targetAttr = targetAttrs[i];
			if(sourceAttr.name == targetAttr.name && sourceAttr.type == targetAttr.type)
			{
				if(sourceAttr.type == TypeReal)
				{
					memcpy((float*)pTarget, (float*)pSource, sizeof(float));
					pTarget += sizeof(float);
				}
				else if(sourceAttr.type == TypeInt)
				{
					*(int*)pTarget = *(int*)pSource;
					pTarget += sizeof(int);
				}
				else if(sourceAttr.type == TypeVarChar)
				{
					memcpy(pTarget, pSource, *(int*)pSource + sizeof(int));
					pTarget += *(int*)pSource + sizeof(int);
				}
				found = true;
				break;
			}
				
			if(sourceAttr.type == TypeVarChar)
			{
				pSource += *(int*)pSource;				
			}

			pSource += sizeof(int);
		}

		if(!found)
		{
			*(int*)pTarget = 0;
			pTarget += sizeof(int);
		}
	}
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	FileHandle fileHandle;
	fileHandle.version = GetTableVersion(tableName);
	RC rc = RecordBasedFileManager::instance()->openFile(tableName, fileHandle);
	if(rc != 0)
		return rc;

	int version = RecordBasedFileManager::instance()->GetVersion(fileHandle, rid);
	vector<Attribute> attrs;
	

	int currentVersion = GetTableVersion(tableName);

	if(currentVersion == version || version == -1) // version == -1 indicate DELETED
	{
		GetAttributesInVersion(currentVersion, tableName, attrs);
		rc = RecordBasedFileManager::instance()->readRecord(fileHandle, attrs, rid, data);
	}
	else
	{
		GetAttributesInVersion(version, tableName, attrs);
		void* buffer = malloc(PAGE_SIZE);
		rc = RecordBasedFileManager::instance()->readRecord(fileHandle, attrs, rid, buffer);
		ConvertData(tableName, version, GetTableVersion(tableName), buffer, data);
		free(buffer);
	}
	RecordBasedFileManager::instance()->closeFile(fileHandle);
	
	return rc;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	FileHandle fileHandle;
	//fileHandle.version = GetTableVersion(tableName);
	RecordBasedFileManager::instance()->openFile(tableName, fileHandle);

	vector<Attribute> attrs;
	getAttributes(tableName, attrs);

	RecordBasedFileManager::instance()->readAttribute(fileHandle, attrs, rid, attributeName, data);
	RecordBasedFileManager::instance()->closeFile(fileHandle);
	return 0;
}

RC RelationManager::reorganizePage(const string &tableName, const unsigned pageNumber)
{
	FileHandle fileHandle;
	fileHandle.version = GetTableVersion(tableName);
	RecordBasedFileManager::instance()->openFile(tableName, fileHandle);

	vector<Attribute> attrs;
	getAttributes(tableName, attrs);
    
	RecordBasedFileManager::instance()->reorganizePage(fileHandle, attrs, pageNumber);
	RecordBasedFileManager::instance()->closeFile(fileHandle);
	return 0;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
	FileHandle* pFileHandle = new FileHandle();
	pFileHandle->version = GetTableVersion(tableName);
	RC rc = RecordBasedFileManager::instance()->openFile(tableName, *pFileHandle);
	if(rc != 0)
		return rc;

	vector<Attribute> attrs;
	getAttributes(tableName, attrs);

	RecordBasedFileManager::instance()->scan(
		*pFileHandle, attrs, 
		conditionAttribute, 
		compOp, 
		value,
		attributeNames, 
		rm_ScanIterator.rbfm_Iterator);
	
    return 0;
}

// Extra credit
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
	int tableVersion = GetTableVersion(tableName);

	vector<Attribute> attrs;
	GetAttributesInVersion(tableVersion, tableName, attrs);
	void* data = malloc(PAGE_SIZE);
	int len = attrs.size();
	for(int i=0; i<len; i++)
	{
		if(attrs[i].name == attributeName)
			continue;

		Attribute attr = attrs[i];
		CreateColumnData(attr, data, i, tableName, tableVersion + 1);

		RID rid;
		insertTupleInner(columnsFileName, data, rid);

		map_columns[tableName].push_back(rid);
	}

	SetTableVersion(tableName, tableVersion + 1);

	free(data);
    return 0;
}

// Extra credit
RC RelationManager::addAttribute(const string &tableName, const Attribute &newAttr)
{
	int tableVersion = GetTableVersion(tableName);

	vector<Attribute> attrs;
	GetAttributesInVersion(tableVersion, tableName, attrs);
	void* data = malloc(PAGE_SIZE);
	int len = attrs.size();

	for(int i=0; i<len; i++)
	{
		Attribute attr = attrs[i];
		CreateColumnData(attr, data, i, tableName, tableVersion + 1);

		RID rid;
		insertTupleInner(columnsFileName, data, rid);
		map_columns[tableName].push_back(rid);
	}


	CreateColumnData(newAttr, data, len, tableName, tableVersion + 1);
	RID r; 
	insertTupleInner(columnsFileName, data, r);
	map_columns[tableName].push_back(r);

	SetTableVersion(tableName, tableVersion + 1);

	free(data);
    return 0;
}

// Extra credit
RC RelationManager::reorganizeTable(const string &tableName)
{
	vector<Attribute> attrs;
	getAttributes(tableName, attrs);

	FileHandle fileHandle;
	fileHandle.version = GetTableVersion(tableName);
	RecordBasedFileManager::instance()->openFile(tableName.c_str(), fileHandle);

	RC rc = RecordBasedFileManager::instance()->reorganizeFile(fileHandle, attrs);
    RecordBasedFileManager::instance()->closeFile(fileHandle);
    return rc;
}

bool RelationManager::IsTableSystem(string tableName){
	 map<string, RID>::iterator tableIter = map_tables.find(tableName);
	 
	 void* data = malloc(255);
	 char* tableType = (char*)data;
	 int tableLen;

	 readAttribute(tablesFileName, tableIter->second, "table_type", tableType);

	 memcpy(&tableLen, (int*)tableType, sizeof(int));
	 tableType += sizeof(int);
	 string tbltype = string(tableType, tableLen);
	 free(data);
	 if (tbltype.compare("System") == 0)
		 return true;
	 else return false;
}

string getIndexFileName(const string& tableName, const string& attributeName)
{
	return tableName + attributeName + ".index";
}

void CreateIndexInfoData(const string& tableName, const string& attributeName, const string& fileName, void* data)
{
	char* p = (char*)data;
	*(int*)p = tableName.length();
	p += sizeof(int);

	memcpy(p, tableName.c_str(), tableName.length());
	p += tableName.length();

	*(int*)p = attributeName.length();
	p += sizeof(int);

	memcpy(p, attributeName.c_str(), attributeName.length());
	p += attributeName.length();

	*(int*)p = fileName.length();
	p += sizeof(int);

	memcpy(p, fileName.c_str(), fileName.length());
	p += fileName.length();
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
	string fileName = getIndexFileName(tableName, attributeName);
	IndexManager::instance()->createFile(fileName, 4);

	void* data = malloc(PAGE_SIZE);
	CreateIndexInfoData(tableName, attributeName, fileName, data);
	RID rid;
	insertTuple(tablesFileName, data, rid);

	CreateIndexInfoData(tableName, attributeName, fileName, data);
	insertTuple(indexCatalogFileName, data, rid);

	IndexFileInfo info;
	info.tableName = tableName;
	info.keyName = attributeName;
	info.fileName = fileName;
	info.rid = rid;

	map_indexFiles[tableName].push_back(info);

	RM_ScanIterator iterator;
	vector<Attribute> attrs;
	getAttributes(tableName, attrs);
	vector<string> attrNames;
	for(int i=0; i<attrs.size(); i++)
	{
		attrNames.push_back(attrs[i].name);
	}
	scan(tableName,attributeName, NO_OP, NULL, attrNames,iterator);

	IXFileHandle ixFileHandle;
	IndexManager::instance()->openFile(fileName, ixFileHandle);
	void* attributeData = malloc(PAGE_SIZE);
	Attribute keyAttribute;

	while(iterator.getNextTuple(rid, data) == 0)
	{
		getAttributeData(data, tableName, attributeName, attributeData, keyAttribute);
		IndexManager::instance()->insertEntry(ixFileHandle, keyAttribute, attributeData, rid);
	}

	IndexManager::instance()->closeFile(ixFileHandle);
	free(attributeData);
	free(data);
	return 0;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
	string indexFileName = getIndexFileName(tableName, attributeName);
	IndexManager::instance()->destroyFile(indexFileName);

	// delete it from index catalog
	RID rid;
	int index = -1;
	for(unsigned i = 0; i < map_indexFiles[tablesFileName].size(); i++)
	{
		IndexFileInfo info = map_indexFiles[tablesFileName][i];
		if(info.tableName == tableName && info.keyName == attributeName)
		{
			index = i;
			rid = info.rid;
			break;
		}
	}

	if(index != -1)
	{
		deleteTuple(indexCatalogFileName, rid);
		map_indexFiles[tablesFileName].erase(map_indexFiles[tablesFileName].begin() + index);
	}
	return 0;
}

Attribute RelationManager::getAttributeByName(const string& tableName, const string& attributeName)
{
	vector<Attribute> attrs;
	getAttributes(tableName, attrs);

	for(unsigned i=0; i<attrs.size(); i++)
		if(attrs[i].name == attributeName)
			return attrs[i];
}

// indexScan returns an iterator to allow the caller to go through qualified entries in index
RC RelationManager::indexScan(const string &tableName,
                    const string &attributeName,
                    const void *lowKey,
                    const void *highKey,
                    bool lowKeyInclusive,
                    bool highKeyInclusive,
                    RM_IndexScanIterator &rm_IndexScanIterator
   )
{
	string attNameWithoutTablePrefix = attributeName;
	if(attNameWithoutTablePrefix.find('.') != string::npos)
	{
		int dotPostion = attNameWithoutTablePrefix.find('.');
		attNameWithoutTablePrefix = attNameWithoutTablePrefix.substr(dotPostion + 1);
	}

	IXFileHandle* fileHandle = new IXFileHandle();

	string indexFileName = getIndexFileName(tableName, attNameWithoutTablePrefix);

	IndexManager::instance()->openFile(indexFileName, *fileHandle);
	Attribute attr = getAttributeByName(tableName,attNameWithoutTablePrefix);
	rm_IndexScanIterator.ixIterator.clear();
	IndexManager::instance()->scan(*fileHandle, attr, lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ixIterator);
	return 0;
}