#include <stdio.h>
#include <string.h>
#include <string>

#include "rbfm.h"
#include "pfm.h"

#define FREESPACE_OFFSET sizeof(int)
#define SLOTCOUNT_OFFSET 2*sizeof(int)
#define FLOAT_SIZE sizeof(float)
#define SIZE_DIRECTORY sizeof(recordDirectory)
#define INT_SIZE sizeof(int)

// Buffer helper functions
int* getSlotCountPointer(void* buffer)
{
	return ((int*)((char*) buffer + (PAGE_SIZE - SLOTCOUNT_OFFSET) ));
}

int getSlotCount(void* buffer)
{
	return *getSlotCountPointer(buffer);
}

recordDirectory* getSlotPointer(void* buffer, unsigned slotID)
{
	unsigned slotAddr = PAGE_SIZE - SLOTCOUNT_OFFSET - SIZE_DIRECTORY * (slotID + 1);
	return ((recordDirectory*)((char*) buffer + slotAddr ));
}


int* getFreeSpaceOffsetPointer(void* buffer)
{
	return ((int*)((char*) buffer + (PAGE_SIZE - FREESPACE_OFFSET) ));
}

int getFreeSpaceOffset(void* buffer)
{
	return *getFreeSpaceOffsetPointer(buffer);
}



RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
	if(!_rbf_manager)
		_rbf_manager = new RecordBasedFileManager();

	return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) 
{
	return PagedFileManager::instance()->createFile(fileName.c_str());
}

RC RecordBasedFileManager::destroyFile(const string &fileName) 
{
	return PagedFileManager::instance()->destroyFile(fileName.c_str());
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) 
{
	return PagedFileManager::instance()->openFile(fileName.c_str(), fileHandle);	
}		

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) 
{
	return PagedFileManager::instance()->closeFile(fileHandle);
}

int decorateRawData(const vector<Attribute> &recordDescriptor, const void* rawData, void* data, FileHandle& fileHandle)
{
	char* p = (char*)data;

	int version = fileHandle.version; 
	*(int*)p = version;
	p += sizeof(int);

	int fieldCount = recordDescriptor.size();
	*(int*)p = fieldCount;
	p += sizeof(int);


	int headerLength = (2 + fieldCount) * sizeof(int) ; // 1 for version, 1 for field count
	int rawDataLength = 0;
	for (int i=0; i < (int) recordDescriptor.size(); i++)
	{	
		if (recordDescriptor[i].type == TypeVarChar)
		{
			rawDataLength += *(int*)((char*)rawData + rawDataLength); 
		}
		rawDataLength += sizeof(int);

		*(int*)p = rawDataLength + headerLength;
		p += sizeof(int);
	}	

	memcpy(p, rawData, rawDataLength);
	return rawDataLength + headerLength;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *rawData, RID &rid)
{
	void* data = malloc(PAGE_SIZE);
	int recordLength = decorateRawData(recordDescriptor, rawData, data, fileHandle);

	int insertPage = findPage( fileHandle, recordLength);
	void* buffer = malloc(PAGE_SIZE);

	if (insertPage == -1)
	{
		rid.pageNum = fileHandle.getNumberOfPages();

		*getFreeSpaceOffsetPointer(buffer) = 0;
		*getSlotCountPointer(buffer) = 0;

		fileHandle.appendPage(buffer);
	}
	else
	{
		rid.pageNum = (PageNum) insertPage;	
	}
	fileHandle.readPage(rid.pageNum, buffer);
	int* slotNum = getSlotCountPointer(buffer);

	int* freeSpaceOffsetPointer = getFreeSpaceOffsetPointer(buffer);
	
	int  slotAddr = PAGE_SIZE - SLOTCOUNT_OFFSET - SIZE_DIRECTORY * (*slotNum+1);
	rid.slotNum = *slotNum; 

	recordDirectory entry;
	entry.slot_offset = getFreeSpaceOffset(buffer);
	entry.slot_length = recordLength;
	entry.status = 0;

	memcpy((char*) buffer + *freeSpaceOffsetPointer, data, recordLength);
	memcpy((char*) buffer + slotAddr, &entry, SIZE_DIRECTORY);
	*slotNum = *slotNum + 1;
	*freeSpaceOffsetPointer = *freeSpaceOffsetPointer + entry.slot_length;

	fileHandle.writePage(rid.pageNum,buffer);


	free(buffer);
	free(data);
	return 0;
}

void RemoveHeaderFromRecord(const void* data, void* rawData)
{
	char* p = (char*)data;

	//int version = *(int*)p;
	p += sizeof(int);

	int fieldCount = *(int*)p;
	p += sizeof(int);

	p += fieldCount * sizeof(int);

	int recordLength = *(int*)(p-sizeof(int));
	int rawDataLength = recordLength - (2 + fieldCount) * sizeof(int);
	memcpy(rawData, p, rawDataLength);
}

int getRecordHeaderLength(const void* data)
{
	char* p = (char*)data;

	//int version = *(int*)p;
	p += sizeof(int);

	int fieldCount = *(int*)p;
	p += sizeof(int);

	return 2 * sizeof(int) + fieldCount * sizeof(int);
}

RC readDecoratedRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data)
{
	unsigned numOfPages = fileHandle.getNumberOfPages();

	if (rid.pageNum >= numOfPages)
		return -1;

	RC rc = 0;
	void* buffer = malloc(PAGE_SIZE);
	fileHandle.readPage(rid.pageNum, buffer);
	unsigned slotNum = getSlotCount(buffer);
	if (rid.slotNum >= slotNum)
	{
		free(buffer);
		return -1;
	}
	recordDirectory recordDir = *getSlotPointer(buffer, rid.slotNum);
	if(recordDir.status == DELETED)
	{
		free(buffer);
		return -1;
	}
	else if(recordDir.status == TOMBSTONE)
	{
		RID newRID;
		newRID.pageNum = *(int*)((char*)buffer + recordDir.slot_offset);
		newRID.slotNum = *(int*)((char*)buffer + recordDir.slot_offset + sizeof(int));
		rc = readDecoratedRecord(fileHandle, recordDescriptor, newRID, data);
	}
	else
	{
		memcpy(data, (char*) buffer + recordDir.slot_offset, recordDir.slot_length);
	}

	free(buffer);
	return rc;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data)
{
	void* buffer = malloc(PAGE_SIZE);

	RC rc = readDecoratedRecord(fileHandle, recordDescriptor, rid, buffer);
	if(rc != 0)
		return rc;

	RemoveHeaderFromRecord(buffer, data);

	//printRecord(recordDescriptor, data);

	free(buffer);
	return 0;
}

int RecordBasedFileManager::GetVersion(FileHandle& fileHandle, RID rid)
{
	void* buffer = malloc(PAGE_SIZE);
	RC rc = fileHandle.readPage(rid.pageNum, buffer);
	recordDirectory* directory = getSlotPointer(buffer, rid.slotNum);

	if(directory->status == TOMBSTONE)
	{
		RID newRid;
		newRid.pageNum = *(int*)((char*)buffer + directory->slot_offset);
		newRid.slotNum =  *(int*)((char*)buffer + directory->slot_offset + sizeof(int));
		free(buffer);
		return GetVersion(fileHandle, newRid);
	}

	if(directory->status == DELETED)
	{
		free(buffer);
		return 0;
	}
	int version = *(int*)((char*)buffer + directory->slot_offset);
	free(buffer);
	return version; 
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data)
{
	int offset = 0;
	for (vector<Attribute>::const_iterator iterator = recordDescriptor.begin(); iterator != recordDescriptor.end(); iterator++)
	{
		string name = (*iterator).name;
		AttrType type = (*iterator).type;
		cout << name << ":";
		switch(type)
		{
		case TypeInt:
			{
				cout << *(int*)((char*)data + offset) << " ";
				offset +=sizeof(int);
				break;

			}
		case TypeReal:
			{
				float floatNum = *(float*)((char*)data + offset);
				cout << floatNum << " ";
				offset +=sizeof(float);
				break;
			}
		case TypeVarChar:
			{
				int length = *(int*)((char*)data + offset);
				offset += sizeof(int);
				
				string str = string((char*)data + offset, length);
				cout << str << " ";
				
				offset += sizeof(char) *length;
				break;
			}
		}
	}
	cout << endl;
	return 0;
}
RC RecordBasedFileManager::findPage(FileHandle &fileHandle, int recordLength)
{	
	int freePageNum;
	fseek(fileHandle.p, FILE_BEGIN_ADD, SEEK_SET);
	fread(&freePageNum, sizeof(int), 1, fileHandle.p);
	void* buffer = malloc(PAGE_SIZE);
	for (int i=0; i < freePageNum; i++)
	{
		fileHandle.readPage(i,buffer);
		int slotNum = getSlotCount(buffer);
		
		int freeSpace = PAGE_SIZE - getFreeSpaceOffset(buffer) - SLOTCOUNT_OFFSET - (slotNum +1) * sizeof(recordDirectory);
		if (freeSpace >= recordLength)
		{
			free(buffer);
			return i;
		}
	}
	free(buffer);
	return -1;
}


RC RecordBasedFileManager::deleteRecords(FileHandle &fileHandle)
{
	int pageCount = fileHandle.getNumberOfPages();
	void* buffer = malloc(PAGE_SIZE);

	for(int i=0; i<pageCount; i++)
	{
		fileHandle.readPage(i, buffer);

		*getFreeSpaceOffsetPointer(buffer) = 0;
		*getSlotCountPointer(buffer) = 0;

		fileHandle.writePage(i, buffer);
	}

	free(buffer);
	return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
	void* buffer = malloc(PAGE_SIZE);
	fileHandle.readPage(rid.pageNum, buffer);

	unsigned slotCount = getSlotCount(buffer);
	if(slotCount <= rid.slotNum)
		return -1;

	recordDirectory* slotAddr = getSlotPointer(buffer, rid.slotNum); 
	slotAddr->status = DELETED;
	slotAddr->slot_length = 0;
	slotAddr->slot_offset = 0; //we need to insert new record to the deleted slot before moving to new space  

	fileHandle.writePage(rid.pageNum, buffer);
	free(buffer);
	return 0;
}

// Assume the rid does not change after update
RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *rawData, const RID &rid)
{
	void* buffer = malloc(PAGE_SIZE);
	fileHandle.readPage(rid.pageNum, buffer);

	void* decoratedData = malloc(PAGE_SIZE);
	int recordLength = decorateRawData(recordDescriptor, rawData, decoratedData, fileHandle);



	recordDirectory* recordDir = getSlotPointer(buffer, rid.slotNum);
	if(recordDir->slot_length >= recordLength)
	{
		memcpy((char*)buffer+recordDir->slot_offset, decoratedData, recordLength);
	}
	else
	{
		RID newRID;
		insertRecord(fileHandle, recordDescriptor, rawData, newRID);

		recordDir->status = TOMBSTONE;
		recordDir->slot_length = 2 * sizeof(int);

		*(int*)((char*)buffer + recordDir->slot_offset) = newRID.pageNum;
		*(int*)((char*)buffer + recordDir->slot_offset + sizeof(int)) = newRID.slotNum;
	}

	fileHandle.writePage(rid.pageNum, buffer);
	free(buffer);
	free(decoratedData);

	return 0;
}

int getFieldFromRecord(void* record, int fieldID, void* fieldData)
{
	char* buffer = (char*) record;

	int offset_end = *(int*)(buffer + (2 + fieldID) * sizeof(int));
	int offset_start = getRecordHeaderLength(record);
	if(fieldID > 0)
		offset_start = *(int*)(buffer + (2 + fieldID - 1) * sizeof(int));

	memcpy(fieldData, buffer + offset_start, offset_end - offset_start);

	return offset_end - offset_start;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, 
	const vector<Attribute> &recordDescriptor, 
	const RID &rid, 
	const string attributeName, 
	void *data)
{
	void *buffer = malloc(PAGE_SIZE);
	RC rc = readDecoratedRecord(fileHandle,recordDescriptor,rid,buffer);
	if(rc != 0)
	{
		return -1;
	}
	
	if(rc == 0)
	{
		for(unsigned i = 0; i < recordDescriptor.size(); i++)
		{
			if(recordDescriptor[i].name == attributeName) 
			{
				getFieldFromRecord(buffer, i, data);
				break;
			}
		}
		free(buffer);
		return 0;
	}
	free(buffer);
	return -1;
}

RC RecordBasedFileManager::reorganizePage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const unsigned pageNumber)
{
	void* buffer = malloc(PAGE_SIZE);
	fileHandle.readPage(pageNumber, buffer);

	int slotCount = getSlotCount(buffer);
	int currentPosition = 0;
	for(int i=0; i<slotCount; i++)
	{
		recordDirectory* recordDir = getSlotPointer(buffer, i);

		if(currentPosition < recordDir->slot_offset)
		{
			memcpy((char*)buffer+currentPosition, (char*)buffer+recordDir->slot_offset, recordDir->slot_length);
			recordDir->slot_offset = currentPosition;
		}

		currentPosition += recordDir->slot_length;
	}

	fileHandle.writePage(pageNumber, buffer);

	free(buffer);
	return 0;
}
RC RecordBasedFileManager::scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, 
	const string &conditionAttribute, 
	const CompOp compOp, 
	const void *value, 
	const vector<string> &attributeNames, 
	RBFM_ScanIterator &rbfm_ScanIterator)
{	

	rbfm_ScanIterator.pFileHandle = &fileHandle;

	int slotNum;
	int numPages = fileHandle.getNumberOfPages();
	void *buffer = malloc(PAGE_SIZE);
	void *buffer1 = malloc(PAGE_SIZE);// for ATtribute
	RC rc;
	AttrType attrtype;
	for (unsigned i = 0; i < attributeNames.size(); i++)
	{
		rbfm_ScanIterator.projectedAttributes.push_back(attributeNames[i]); // Preparing for getNextRecord, which attributes are required
	}
	for (unsigned i = 0; i < recordDescriptor.size(); i++)
	{
		rbfm_ScanIterator.rdDescriptor.push_back(recordDescriptor[i]);	// All the atrrbiutes
	}
	rbfm_ScanIterator.next = 0;

	for (unsigned i = 0; i < recordDescriptor.size(); i++)
	{ 
		if (recordDescriptor[i].name == conditionAttribute)     // choose the attribute to be compared, by its name
		{
			attrtype = recordDescriptor[i].type;
			break;
		}
	}

	for (int i = 0; i < numPages; i++)
	{
		fileHandle.readPage(i, buffer); 
		slotNum = getSlotCount(buffer);							

		for (int j = 0; j < slotNum; j++)
		{
			recordDirectory* recordDir = getSlotPointer( buffer, j);
			if (recordDir->status == NORMAL)
			{   
				RID rid;
				rid.pageNum = i;
				rid.slotNum = j;
				rc = readAttribute(fileHandle, recordDescriptor, rid, conditionAttribute, buffer1); // buffer1, the attribute data
				if (rc != 0)
				{
					free(buffer);
					free(buffer1);
					return -1;
				}

				if (judgeAttribute(value, buffer1, compOp, attrtype))  //satisfy the requirement
				{
					RID AcceptId;
					AcceptId.pageNum = i;			//Always keep the first ID, no matter whether there is a tombstone
					AcceptId.slotNum = j;
					rbfm_ScanIterator.recordId.push_back(AcceptId);
				}
			}

		}
	}

	free(buffer);
	free(buffer1);
	return 0;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{
	int len;
	if (next < recordId.size())
	{
		//cout << "In RBFM_ScanIterator::getNextRecord" << endl;
		//cout << "next : " << next << endl;
		//if(pFileHandle == NULL)
		//	cout << "FileHandle is NULL" << endl;
		//cout << "name of file handle " << pFileHandle->name << endl;

		void* buffer = malloc(PAGE_SIZE);
		rid.pageNum = recordId[next].pageNum;
		rid.slotNum = recordId[next].slotNum;
		RecordBasedFileManager* scanRbfm = RecordBasedFileManager::instance();
		scanRbfm->readRecord(*pFileHandle, rdDescriptor, recordId[next], buffer);

		//cout << "after reading record" <<endl;
		int offsetAttr; // every time we need to find a specific attribute by loop, we need to set it zero, and calculate the offset for each projected offset
		int offsetRecord = 0;  // this is used to the projected attribute we found in continuous place
		for (unsigned i = 0; i < projectedAttributes.size(); i++)
		{
			offsetAttr = 0; // need to set zero, cause we have to find the next required attribute from the beginning and enumerating every possible matching attribute.
			for (unsigned j = 0; j < rdDescriptor.size(); j++)
			{
				switch(rdDescriptor[j].type)
				{
				case TypeInt:
					len = sizeof(int);
					break;
				case TypeReal:
					len = sizeof(int);
					break;
				case TypeVarChar:
					len = *((int*)((char*)buffer + offsetAttr)) + sizeof(int);
					break;
				}
				if (rdDescriptor[j].name == projectedAttributes[i])
				{
					memcpy((char*)data + offsetRecord, (char*)buffer + offsetAttr, len);
					offsetRecord += len;
					break;
				}


				offsetAttr +=len; // we need to update the offsetAttr so that we always copy from the right place of each attribute required, for one time.
			}
		}
		next++; //next record
		free(buffer);
		return 0;
	}
	return RBFM_EOF;
}


bool RecordBasedFileManager::judgeAttribute(const void* value, void* attribute, CompOp compOp, AttrType type)
{
	if (compOp == NO_OP)
		return true;

	int result;
	switch (type) {
	case TypeVarChar:
		{	
			int Length = *((int*) value);
			string valueStr = string((char*)((char*)value +sizeof(int)),Length); //try memcpy(&tempStr, (char*)((char*)value + sizeof(int)),length);
			Length = *((int*) attribute);
			string  attrstr = string((char*)((char*)attribute + sizeof(int)), Length);
			result = attrstr.compare(valueStr);
			break;
		}
	case TypeInt:
		result = *((int*) attribute) - *((int*) value); 
		break;
	case TypeReal:
		float temp = *((float*) attribute) - *((float *) value);

		result = 0;
		if(temp < -0.000001)
			result = -1;
		if(temp > 0.000001)
			result = 1;
		break;
	}

	switch (compOp)
	{
	case NO_OP:
		return true;
		break;
	case EQ_OP:
		return(result == 0);
		break;
	case LT_OP:
		return(result < 0);
		break;
	case GT_OP:
		return(result > 0);
		break;
	case LE_OP:
		return (result <= 0);
		break;
	case GE_OP:
		return (result >= 0);
		break;
	case NE_OP:
		return (result != 0);
		break;
	}
	return false;
}

RC RecordBasedFileManager::reorganizeFile(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor)
{
	void* buffer = malloc(PAGE_SIZE);
	void* blankBuffer = malloc(PAGE_SIZE);

	*getFreeSpaceOffsetPointer(blankBuffer) = 0;
	*getSlotCountPointer(blankBuffer) = 0;

	PageNum pagenum = fileHandle.getNumberOfPages();
	PageNum Zero = 0;
	fseek(fileHandle.p, FILE_BEGIN_ADD, SEEK_SET);
	fwrite(&Zero, sizeof(PageNum), 1, fileHandle.p);

	for (unsigned i=0; i < pagenum; i++)
	{
		RC rc = fileHandle.readPage(i,buffer);
		if(rc != 0)
			cout << "reading page failed" << endl;
		fileHandle.writePage(i, blankBuffer);

		int slotNum = getSlotCount(buffer);
		for(int j = 0; j < slotNum; j++)
		{
			recordDirectory* recordDir = getSlotPointer(buffer , j);
			if ((recordDir->status != TOMBSTONE) && (recordDir->status != DELETED))
			{
				RID rid;
				int headerLength = getRecordHeaderLength((char*)buffer + recordDir->slot_offset);
				insertRecord(fileHandle, recordDescriptor, (char*)buffer + recordDir->slot_offset + headerLength, rid);
			}
		}
	}
	//cout << count << endl;
	free(buffer);
	free(blankBuffer);
	return 0;
}

