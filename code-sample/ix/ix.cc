
#include "ix.h"
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <string.h>
#include <math.h>
using namespace std;

#define PAGE_BUCKET_COUNT 31
#define ERROR_CREATEFILE 1
#define ERROR_DESTROYFILE 2
#define ERROR_OPENFILE 3
#define ERROR_CLOSEFILE 4
#define ERROR_INSERTENTRY 5
#define ERROR_DELETEENTRY 6
#define ERROR_PRINTINDEXENTRIES 7

IndexManager* IndexManager::_index_manager = 0;

IXPageBufferHelper* IXPageBufferHelper::_buffer_helper = 0;


void printData(const Attribute& attr, void* key)
{
	cout << "[";
	char* p = (char*)key;
	if(attr.type == TypeInt)
	{
		cout << *(int*)key;
		p += sizeof(int);
	}
	else if(attr.type == TypeReal)
	{
		cout << *(float*)key;
		p += sizeof(int);
	}
	else if(attr.type == TypeVarChar)
	{
		int len = *(int*)key;
		p += sizeof(4);
		string s = string(p, len);
		cout << s;
		p += len;
	}

	cout << "/";
	cout << *(int*)p;

	p += sizeof(int);
	cout << ",";

	cout << *(int*)p;
	cout << "]";
}


IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{

}

IndexManager::~IndexManager()
{
}

string getPrimaryIndexName(const string& fileName)
{
	return fileName + ".index1";
}

string getSecondaryIndexName(const string& fileName)
{
	return fileName + ".index2";
}

RC IndexManager::createFile(const string &fileName, const unsigned &numberOfPages)
{
	RC rc = PagedFileManager::instance()->createFile(getPrimaryIndexName(fileName).c_str());
	if(rc != 0)
	{
		IX_PrintError(ERROR_CREATEFILE);
		return rc;
	}

	FileHandle fileHandle;
	PagedFileManager::instance()->openFile(getPrimaryIndexName(fileName).c_str(), fileHandle);
	void* data = malloc(PAGE_SIZE);
	IXPageBufferHelper::instance()->createEmptyBuffer(data);
	
	for(unsigned i=0; i<numberOfPages; i++)
	{
		fileHandle.appendPage(data);
	}
	
	free(data);
	PagedFileManager::instance()->closeFile(fileHandle);
	

	rc = PagedFileManager::instance()->createFile(getSecondaryIndexName(fileName).c_str());

	
	FILE* p = fopen(getSecondaryIndexName(fileName).c_str(), "r+b");
	fseek(p, sizeof(int),SEEK_SET);
	fwrite(&numberOfPages, sizeof(int), 1, p);

	int level = 0;
	fseek(p, 2*sizeof(int),SEEK_SET);
	fwrite(&level, sizeof(int), 1, p);

	int next = 0;
	fseek(p, 3*sizeof(int),SEEK_SET);
	fwrite(&next, sizeof(int), 1, p);

	int emptyPagePointer = -1;
	fseek(p, 4*sizeof(int),SEEK_SET);
	fwrite(&emptyPagePointer, sizeof(int), 1, p);

	fclose(p);

	if(rc != 0)
		IX_PrintError(ERROR_CREATEFILE);
	return rc;
}

RC IndexManager::destroyFile(const string &fileName)
{
	RC rc = PagedFileManager::instance()->destroyFile(getPrimaryIndexName(fileName).c_str());
	if(rc != 0)
	{
		IX_PrintError(ERROR_DESTROYFILE);
		return rc;
	}

	rc = PagedFileManager::instance()->destroyFile(getSecondaryIndexName(fileName).c_str());
	if(rc != 0)
		IX_PrintError(ERROR_DESTROYFILE);
	return rc;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixFileHandle)
{
	RC rc = PagedFileManager::instance()->openFile(getPrimaryIndexName(fileName).c_str(), ixFileHandle.pagedFileHandlePrimary);
	if(rc != 0)
	{
		IX_PrintError(ERROR_OPENFILE);
		return rc;
	}
	rc = PagedFileManager::instance()->openFile(getSecondaryIndexName(fileName).c_str(), ixFileHandle.pagedFileHandleSecondary);
	if(rc != 0)
		IX_PrintError(ERROR_OPENFILE);
	return rc;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	RC rc = PagedFileManager::instance()->closeFile(ixfileHandle.pagedFileHandlePrimary);
	if(rc != 0)
	{
		IX_PrintError(ERROR_CLOSEFILE);
		return rc;
	}
	rc = PagedFileManager::instance()->closeFile(ixfileHandle.pagedFileHandleSecondary);
	if(rc != 0)
		IX_PrintError(ERROR_CLOSEFILE);
	return rc;

}

void GetIndexAttributes(vector<Attribute>& attrs, const Attribute& attribute)
{
	attrs.clear();

	Attribute attr; 
	attr.name = "key_value";
	attr.type = attribute.type;
	attr.length = 30;
	attrs.push_back(attr);

	attr.name = "pageID";
	attr.type = TypeInt;
	attr.length = 4;
	attrs.push_back(attr);

	attr.name = "slotID";
	attr.type = TypeInt;
	attr.length = 4;
	attrs.push_back(attr);
}


void BuildIndexRecordRawData(const Attribute& attribute, const void* key, const RID &rid, void* data, int& dataLength)
{
	char* p = (char*)data;
	dataLength = 0;
	if(attribute.type == TypeInt || attribute.type == TypeReal)
	{
		memcpy(data, key, sizeof(int));
		p += sizeof(int);
		dataLength += sizeof(int);
	}
	else
	{
		int length = *(int*)key;
		memcpy(data, key, sizeof(int) + length);
		p += sizeof(int) + length;
		dataLength += sizeof(int) + length;
	}

	*(int*)p = rid.pageNum;
	p += sizeof(int);
	dataLength += sizeof(int);

	*(int*)p = rid.slotNum;
	p += sizeof(int);
	dataLength += sizeof(int);
}

int compareKeys(const Attribute& attr, const void* key1, const void* key2)
{
	if(attr.type == TypeInt)
	{
		return *(int*)key1 - *(int*)key2;
	}
	if(attr.type == TypeReal)
	{
		float diff = *(float*)key1 - *(float*)key2;
		if(diff < 0)
			return -1;
		if(diff > 0)
			return 1;
		return 0;
	}
	if(attr.type == TypeVarChar)
	{
		int len1 = *(int*)key1;
		int len2 = *(int*)key2;
		string str1 = string((char*)key1 + sizeof(int), len1);
		string str2 = string((char*)key2 + sizeof(int), len2);
		if(str1 < str2)
			return -1;
		if(str1 > str2)
			return 1;
		return 0;
	}
	return 0;
}


int powInt(int base, int times)
{
	int v = 1;
	for(int i=0; i<times; i++)
	{
		v *= base;
	}
	return v;
}

void IndexManager::Merge(IXFileHandle &ixFileHandle, const Attribute &attribute)
{
	int N, level, next;
	ixFileHandle.getMetaData(N, level, next);
	int lastPageID;
	next -= 1;
	bool isInPrimary = true;
	if (next == -1)
	{
		if (level!=0)
		{
		level -= 1;
		next = N * powInt(2, level) - 1;
		}
		else 
			return;
	}
	ixFileHandle.setMetaData(N, level, next);
	int bucketID_1 = next;
	int bucketID_2 = next + N * powInt(2, level);
	int nextPageID = 0;
	void* buffer_2 = malloc(PAGE_SIZE);
	void* buffer_1 = malloc(PAGE_SIZE);
	void* helpBuffer = malloc(PAGE_SIZE);
	ixFileHandle.pagedFileHandlePrimary.readPage(bucketID_2, buffer_2);
	nextPageID = IXPageBufferHelper::instance()->getNextPageID(buffer_2);
	
	//put the primary page for bucketID_2 to the end of secondary page, hence set the nextpageID to 1
	IXPageBufferHelper::instance()->setNextPageID(buffer_2, -1);

	
	//delete bucketID_2
	IXPageBufferHelper::instance()->createEmptyBuffer(helpBuffer);
	ixFileHandle.pagedFileHandlePrimary.writePage(bucketID_2, helpBuffer);
	
	//need a pageID in bucketID_2 for bucketID_1 to link with
	int connectionID = 0;
	
	if (nextPageID == -1 && !IXPageBufferHelper::instance()->IsPageEmpty(buffer_2))
	{
		ixFileHandle.pagedFileHandleSecondary.appendPage(buffer_2);
		connectionID = ixFileHandle.pagedFileHandleSecondary.getNumberOfPages()-1;
	}
	else {
		connectionID = nextPageID;
		while(nextPageID !=-1)
		{
			lastPageID = nextPageID;
			ixFileHandle.pagedFileHandleSecondary.readPage(nextPageID, helpBuffer);
			nextPageID = IXPageBufferHelper::instance()->getNextPageID(helpBuffer);
		}

		if(!IXPageBufferHelper::instance()->IsPageEmpty(buffer_2))
		{
			ixFileHandle.pagedFileHandleSecondary.appendPage(buffer_2);
			nextPageID = ixFileHandle.pagedFileHandleSecondary.getNumberOfPages() - 1;
			IXPageBufferHelper::instance()->setNextPageID(helpBuffer, nextPageID);
			ixFileHandle.pagedFileHandleSecondary.writePage(lastPageID, helpBuffer);
		}
	}


	ixFileHandle.pagedFileHandlePrimary.readPage(bucketID_1, buffer_1);
	int PageID_1 = bucketID_1;
	nextPageID = IXPageBufferHelper::instance()->getNextPageID(buffer_1);
	if (nextPageID == -1) 
	{
		IXPageBufferHelper::instance()->setNextPageID(buffer_1, connectionID);
		ixFileHandle.pagedFileHandlePrimary.writePage(PageID_1, buffer_1);
	}
	else
	{
		while(nextPageID != -1)
		{
			lastPageID = nextPageID;
			ixFileHandle.pagedFileHandleSecondary.readPage(nextPageID, buffer_1);
			nextPageID = IXPageBufferHelper::instance()->getNextPageID(buffer_1);
		}
		IXPageBufferHelper::instance()->setNextPageID(buffer_1, connectionID);
		ixFileHandle.pagedFileHandleSecondary.writePage(lastPageID, buffer_1);
	}
	free(buffer_2);
	free(buffer_1);
	free(helpBuffer);
}

int getRecordLength(void* buffer, AttrType type)
{
	if(type == TypeVarChar)
	{
		return 3 * sizeof(int) + *(int*)buffer;
	}
	return 3 * sizeof(int);
}

void IndexManager::AddPageIntoEmptyPageList(IXFileHandle& ixFileHandle, int pageID)
{
	if(pageID == -1)
		return;

	int emptyPagePointer = ixFileHandle.getEmptyPagePointer();
	if(emptyPagePointer == -1)
	{
		ixFileHandle.setEmptyPagePointer(pageID);
		return;
	}

	void* buffer = malloc(PAGE_SIZE);
	while(true)
	{
		readPage(OverflowPage, ixFileHandle, emptyPagePointer, buffer);
		int next = IXPageBufferHelper::instance()->getNextPageID(buffer);
		if(next == -1)
			break;
		emptyPagePointer = next;
	}

	IXPageBufferHelper::instance()->setNextPageID(buffer, pageID);
	writePage(OverflowPage, ixFileHandle, emptyPagePointer, buffer);
	free(buffer);
}

void IndexManager::Split(IXFileHandle &ixFileHandle, const Attribute &attribute)
{
	int N, level, next;
	ixFileHandle.getMetaData(N, level, next);

	int bucket1 = next;
	int bucket2 = next + N * powInt(2, level);

	int input_pageID = next;
	int bucket1_pageID = next;
	int bucket2_pageID = next + N * powInt(2, level);

	PageType input_pageType = PrimaryPage;
	PageType bucket1_pageType = PrimaryPage;
	PageType bucket2_pageType = PrimaryPage;

	void* input_pageBuffer = malloc(PAGE_SIZE);
	void* bucket1_output_buffer = malloc(PAGE_SIZE);
	IXPageBufferHelper::instance()->createEmptyBuffer(bucket1_output_buffer);
	void* bucket2_output_buffer = malloc(PAGE_SIZE);
	IXPageBufferHelper::instance()->createEmptyBuffer(bucket2_output_buffer);
	ixFileHandle.pagedFileHandlePrimary.appendPage(bucket2_output_buffer);

	while(input_pageID != -1)
	{
		readPage(input_pageType, ixFileHandle, input_pageID, input_pageBuffer);
		if(input_pageType == PrimaryPage)
		{
			int nextID = IXPageBufferHelper::instance()->getNextPageID(input_pageBuffer);
			IXPageBufferHelper::instance()->setNextPageID(bucket1_output_buffer, nextID);
		}

		vector<int> recordPointerList;
		IXPageBufferHelper::instance()->getAllRecordRawDataPointers(input_pageBuffer, recordPointerList);

		for(int i=0; i<recordPointerList.size(); i++)
		{
			int recordPointer = recordPointerList[i];
			char* recordData = (char*)input_pageBuffer + recordPointer;
			
			int recordLength = getRecordLength(recordData, attribute.type);
			int targetBucket = hash(attribute, recordData) % ( N * powInt(2, level+1));
			if(targetBucket == bucket1)
			{
				if( !IXPageBufferHelper::instance()->tryInsertRecordIntoPageBuffer(bucket1_output_buffer, recordData, recordLength, attribute.type))
				{
					writePage(bucket1_pageType, ixFileHandle, bucket1_pageID, bucket1_output_buffer);
					bucket1_pageType = OverflowPage;
					bucket1_pageID = IXPageBufferHelper::instance()->getNextPageID(bucket1_output_buffer);
					readPage(bucket1_pageType, ixFileHandle, bucket1_pageID, bucket1_output_buffer);
					IXPageBufferHelper::instance()->clearPageData(bucket1_output_buffer);
					IXPageBufferHelper::instance()->tryInsertRecordIntoPageBuffer(bucket1_output_buffer, recordData, recordLength, attribute.type);
				}
			}
			else if(targetBucket == bucket2)
			{
				if( !IXPageBufferHelper::instance()->tryInsertRecordIntoPageBuffer(bucket2_output_buffer, recordData, recordLength, attribute.type))
				{	
					void* emptyPage = malloc(PAGE_SIZE);
					IXPageBufferHelper::instance()->createEmptyBuffer(emptyPage);
					int nextID = appendPageOrReuseEmptyPage(ixFileHandle, emptyPage);
					free(emptyPage);
					IXPageBufferHelper::instance()->setNextPageID(bucket2_output_buffer, nextID);
					writePage(bucket2_pageType, ixFileHandle, bucket2_pageID, bucket2_output_buffer);
					IXPageBufferHelper::instance()->createEmptyBuffer(bucket2_output_buffer);
					bucket2_pageID = nextID;
					bucket2_pageType = OverflowPage;
					IXPageBufferHelper::instance()->tryInsertRecordIntoPageBuffer(bucket2_output_buffer, recordData, recordLength, attribute.type);
				}
			}
			else
			{
				cout << "AH!!! SOMETHING WRONG FOR KEY : " << endl;
				printData(attribute, recordData);
				cout << "target:" << targetBucket << ", bucket1:" << bucket1 << ", bucket2:" << bucket2 << endl;
			}
		}

		input_pageID = IXPageBufferHelper::instance()->getNextPageID(input_pageBuffer);
		input_pageType = OverflowPage;
	}

	int nextBucket1PageID = IXPageBufferHelper::instance()->getNextPageID(bucket1_output_buffer);
	AddPageIntoEmptyPageList(ixFileHandle, nextBucket1PageID);

	IXPageBufferHelper::instance()->setNextPageID(bucket1_output_buffer, -1);
	writePage(bucket1_pageType, ixFileHandle, bucket1_pageID, bucket1_output_buffer);
	writePage(bucket2_pageType, ixFileHandle, bucket2_pageID, bucket2_output_buffer);

	free(input_pageBuffer);
	free(bucket1_output_buffer);
	free(bucket2_output_buffer);

	next++;
	if(next >= N * powInt(2, level))
	{
		level++;
		next = 0;
	}
	ixFileHandle.setMetaData(N, level, next);
}

unsigned IndexManager::getBucketID(IXFileHandle &ixFileHandle,const Attribute& attribute, const void* key)
{
	unsigned hashValue = hash(attribute, key);
	int N, level, Next;
	ixFileHandle.getMetaData(N, level, Next);

	unsigned bucketID = hashValue % (N * powInt(2, level + 1));
	if(bucketID >= Next + N * powInt(2, level))
	{
		bucketID = hashValue % (N * powInt(2, level));
	}
	return bucketID;
}

RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	void* recordData = malloc(PAGE_SIZE);
	int recordDataLength;
	BuildIndexRecordRawData(attribute, key, rid, recordData, recordDataLength);

	unsigned bucketID = getBucketID(ixFileHandle, attribute, key);

	void* currentPageBuffer = malloc(PAGE_SIZE);	
	unsigned currentPageID = bucketID;
	PageType currentPageType = PrimaryPage;

	bool needSplit = false;
	while(true)
	{
		readPage(currentPageType, ixFileHandle, currentPageID, currentPageBuffer);
		if(IXPageBufferHelper::instance()->tryInsertRecordIntoPageBuffer(currentPageBuffer, recordData, recordDataLength, attribute.type))
		{	
			writePage(currentPageType, ixFileHandle, currentPageID, currentPageBuffer);
			break;
		}
		int nextPageID = IXPageBufferHelper::instance()->getNextPageID(currentPageBuffer);
		if(nextPageID == -1)
		{
			needSplit = true;
			void* newPageBuffer = malloc(PAGE_SIZE);
			IXPageBufferHelper::instance()->createEmptyBuffer(newPageBuffer);
			IXPageBufferHelper::instance()->tryInsertRecordIntoPageBuffer(newPageBuffer, recordData, recordDataLength, attribute.type);
			int newPageID = appendPageOrReuseEmptyPage(ixFileHandle, newPageBuffer);
			writePage(OverflowPage, ixFileHandle, newPageID, newPageBuffer);
			
			IXPageBufferHelper::instance()->setNextPageID(currentPageBuffer, newPageID);
			writePage(currentPageType, ixFileHandle, currentPageID, currentPageBuffer);
			free(newPageBuffer);
			break;
		}

		currentPageID = nextPageID;
		currentPageType = OverflowPage;
	}

	if(needSplit)
		Split(ixFileHandle, attribute);

	free(recordData);
	free(currentPageBuffer);
	return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	unsigned bucketID = getBucketID(ixFileHandle, attribute, key);

	void* currentPageBuffer = malloc(PAGE_SIZE);
	int currentPageID = bucketID;
	PageType currentPageType = PrimaryPage;

	void* recordData = malloc(PAGE_SIZE);
	int recordDataLength;
	BuildIndexRecordRawData(attribute, key, rid, recordData, recordDataLength);

	bool deleted = false;
	bool needMerge = false;

	int lastPageID = -1;
	PageType lastPageType = PrimaryPage;
	while(true)
	{
		if(currentPageID == -1)
			break;
		readPage(currentPageType, ixFileHandle, currentPageID, currentPageBuffer);

		if( IXPageBufferHelper::instance()->tryDeleteRecordFromPageBuffer(currentPageBuffer, recordData, recordDataLength, attribute.type) )
		{
			writePage(currentPageType, ixFileHandle, currentPageID, currentPageBuffer);
			if( IXPageBufferHelper::instance()->IsPageEmpty(currentPageBuffer))
			{	
				needMerge = true;
				if(lastPageID != -1)
				{
					void* lastPageBuffer = malloc(PAGE_SIZE);
					readPage(lastPageType, ixFileHandle, lastPageID, lastPageBuffer);
					int nextPageID = IXPageBufferHelper::instance()->getNextPageID(currentPageBuffer);
					IXPageBufferHelper::instance()->setNextPageID(lastPageBuffer, nextPageID);
					writePage(lastPageType, ixFileHandle, lastPageID, lastPageBuffer);

					IXPageBufferHelper::instance()->setNextPageID(currentPageBuffer, -1);
					writePage(currentPageType, ixFileHandle, currentPageID, currentPageBuffer);

					AddPageIntoEmptyPageList(ixFileHandle, currentPageID);
					free(lastPageBuffer);
				}
			}
			deleted = true;	
			break;
		}
		lastPageID = currentPageID;
		lastPageType = currentPageType;
		currentPageID = IXPageBufferHelper::instance()->getNextPageID(currentPageBuffer);
		currentPageType = OverflowPage;
	}

	if(needMerge)
		Merge(ixFileHandle, attribute);

	if(!deleted)
	{
		IX_PrintError(ERROR_DELETEENTRY);
		return 1;
	}
	return 0;
}

unsigned IndexManager::hash(const Attribute &attribute, const void *key)
{
	char* p = (char*)key;
	long long hashValue = 0;
	if(attribute.type == TypeVarChar)
	{
		int length = *(int*)key;
		p += sizeof(int);

		hashValue = 5381;
		for(int i=0; i<length; i++)
		{
			if (i < 4 || i == length-2 || i == length/2)
			{
				hashValue = ((hashValue<<5) + hashValue) +*p ;
				hashValue %= 65536;
			}
			p++;
		}
	}
	else 
	{
		if (attribute.type == TypeInt)
			hashValue = (long long)*(int*)key * 65536;
		else	
			hashValue = *(float*)key;
	
		hashValue = (hashValue << 15) - hashValue - 1;
		hashValue = hashValue ^ ( hashValue >> 12);
		hashValue = hashValue + (hashValue << 2);
		hashValue = hashValue ^ (hashValue >> 4);
		hashValue = (hashValue + (hashValue << 3)) + (hashValue << 11);
		hashValue = hashValue ^ (hashValue >> 16);
	}
	return hashValue % 65536;
}



RC IndexManager::printIndexEntriesInAPage(IXFileHandle &ixFileHandle, const Attribute &attribute, const unsigned &primaryPageNumber) 
{
	void* buffer = malloc(PAGE_SIZE);
	int currentPageID = primaryPageNumber;
	unsigned MaxPage = 0;
	getNumberOfPrimaryPages(ixFileHandle,MaxPage);
	if (MaxPage-1<primaryPageNumber)
		{IX_PrintError(ERROR_PRINTINDEXENTRIES); return -1;}
	PageType currentPageType = PrimaryPage;
	int totalEntryCount = 0;
	while(true)
	{
		if(currentPageID == -1)
			break;
		readPage(currentPageType, ixFileHandle, currentPageID, buffer);
		vector<int> v;
		IXPageBufferHelper::instance()->getAllRecordRawDataPointers(buffer, v);
		totalEntryCount += v.size();

		currentPageType = OverflowPage;
		currentPageID = IXPageBufferHelper::instance()->getNextPageID(buffer);
	}

	cout << "Number of total entries in the page (+ overflow pages) :" << totalEntryCount << endl;
	
	readPage(PrimaryPage, ixFileHandle, primaryPageNumber, buffer);
	IXPageBufferHelper::instance()->printDataInPrimaryPage(buffer, attribute, primaryPageNumber);

	int lastPageID = primaryPageNumber;
	currentPageID = IXPageBufferHelper::instance()->getNextPageID(buffer);

	while(true)
	{
		if(currentPageID == -1)
			break;
		readPage(OverflowPage, ixFileHandle, currentPageID, buffer);
		IXPageBufferHelper::instance()->printDataInOverflowPage(buffer, attribute, lastPageID, currentPageID);

		lastPageID = currentPageID;
		currentPageID = IXPageBufferHelper::instance()->getNextPageID(buffer);
	}

	free(buffer);
	return 0;
}

RC IndexManager::getNumberOfPrimaryPages(IXFileHandle &ixFileHandle, unsigned &numberOfPrimaryPages) 
{
	int N, level, next;
	ixFileHandle.getMetaData(N, level, next);
	numberOfPrimaryPages = next + N * powInt(2, level);
	return 0;
}

RC IndexManager::getNumberOfAllPages(IXFileHandle &ixFileHandle, unsigned &numberOfAllPages) 
{
	unsigned primaryPageNumber;
	getNumberOfPrimaryPages(ixFileHandle, primaryPageNumber);
	unsigned overflowPageNumber = ixFileHandle.pagedFileHandleSecondary.getNumberOfPages() - getEmptyPageCount(ixFileHandle);

	numberOfAllPages = primaryPageNumber + overflowPageNumber + 1; // 1 for meta data page...
	return 0;
}

void IX_ScanIterator::AddPageInScan(const Attribute &attribute, 
	void* buffer, 
	const void      *lowKey,
    const void      *highKey,
    bool			lowKeyInclusive,
    bool        	highKeyInclusive)
{
	vector<int> recordPointerList;

	if(lowKey != NULL && highKey != NULL && compareKeys(attribute, lowKey, highKey) == 0
		&& lowKeyInclusive == true && highKeyInclusive == true)
	{
		int bucketID = IXPageBufferHelper::instance()->hash_page(lowKey, attribute.type) % PAGE_BUCKET_COUNT;
		IXPageBufferHelper::instance()->getRecordPointerListInBucket(buffer, bucketID, recordPointerList);
	}
	else
	{
		IXPageBufferHelper::instance()->getAllRecordRawDataPointers(buffer, recordPointerList);
	}

	for(int i=0; i<recordPointerList.size(); i++)
	{
		int recordPointer = recordPointerList[i];
		char* p = (char*)buffer + recordPointer;

		if(lowKey != NULL)
		{
			int cmp = compareKeys(attribute, lowKey, p);
			if(cmp > 0)
				continue;
			if(cmp == 0 && !lowKeyInclusive)
				continue;
		}

		if(highKey != NULL)
		{
			int cmp = compareKeys(attribute, p, highKey);
			if(cmp > 0)
				continue;
			if(cmp == 0 && !highKeyInclusive)
				continue;
		}

		if(attribute.type == TypeInt)
		{
			intQueue.push( *(int*)p );
			p += sizeof(int);
		}
		else if(attribute.type == TypeReal)
		{
			floatQueue.push( *(float*)p );
			p += sizeof(int);
		}
		else if(attribute.type == TypeVarChar)
		{
			int len = *(int*)p;
			p += sizeof(int);

			string s = string(p, len);
			stringQueue.push(s);
			p += len;
		}
		
		RID rid;
		rid.pageNum = *(int*)p;
		p += sizeof(int);
		rid.slotNum = *(int*)p;
		p += sizeof(int);
		ridQueue.push(rid);
	}
}

RC IndexManager::scan(IXFileHandle &ixFileHandle,
    const Attribute &attribute,
    const void      *lowKey,
    const void      *highKey,
    bool			lowKeyInclusive,
    bool        	highKeyInclusive,
    IX_ScanIterator &ix_ScanIterator)
{
	ix_ScanIterator.clear();
	ix_ScanIterator.type = attribute.type;

	void* currentPageBuffer = malloc(PAGE_SIZE);
	PageType currentPageType;
	int currentPageID;

	unsigned pageCount;
	getNumberOfPrimaryPages(ixFileHandle, pageCount);

	for(int i=0; i<pageCount; i++)
	{
		currentPageID = i;
		currentPageType = PrimaryPage;


		while(currentPageID != -1)
		{
			readPage(currentPageType, ixFileHandle, currentPageID, currentPageBuffer);

			ix_ScanIterator.AddPageInScan(attribute, currentPageBuffer, lowKey, highKey, lowKeyInclusive, highKeyInclusive);

			currentPageID = IXPageBufferHelper::instance()->getNextPageID(currentPageBuffer);
			currentPageType = OverflowPage;
		}
	}

	free(currentPageBuffer);
	return 0;
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{		
	if(ridQueue.empty())
		return IX_EOF;

	rid = ridQueue.front();
	ridQueue.pop();
	if(type == TypeInt)
	{
		int v = intQueue.front();
		intQueue.pop();
		*(int*)key = v;
	}
	else if(type == TypeReal)
	{
		float v = floatQueue.front();
		floatQueue.pop();
		*(float*)key = v;
	}
	else if(type == TypeVarChar)
	{
		char* p = (char*)key;
		string s = stringQueue.front();
		stringQueue.pop();
		int len = s.length();
		*(int*)p = len;
		p += sizeof(int);
		//memcpy(p, s, len);
		for(int i=0; i<len; i++)
		{
			*p = s[i];
			p++;
		}
	}

	return 0;
}

RC IX_ScanIterator::close()
{
	return 0;
}


IXFileHandle::IXFileHandle()
{
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	unsigned primaryRead, primaryWrite, primaryAppend;
	pagedFileHandlePrimary.collectCounterValues(primaryRead, primaryWrite, primaryAppend);

	unsigned secondaryRead, secondaryWrite, secondaryAppend;
	pagedFileHandleSecondary.collectCounterValues(secondaryRead, secondaryWrite, secondaryAppend);

	readPageCount = primaryRead + secondaryRead;
	writePageCount = primaryWrite + secondaryWrite;
	appendPageCount = primaryAppend + secondaryAppend;
	return 0;
}

int IXFileHandle::getEmptyPagePointer()
{
	int emptyPagePointer;
	FILE* p = pagedFileHandleSecondary.p;
	fseek(p, 4 * sizeof(int),SEEK_SET);
	fread(&emptyPagePointer, sizeof(int), 1, p);

	return emptyPagePointer;
}

void IXFileHandle::setEmptyPagePointer(int pagePointer)
{
	FILE* p = pagedFileHandleSecondary.p;
	fseek(p, 4 * sizeof(int),SEEK_SET);
	fwrite(&pagePointer, sizeof(int), 1, p);
}

int IndexManager::getEmptyPageCount(IXFileHandle& ixFileHandle)
{
	int p = ixFileHandle.getEmptyPagePointer();
	int count = 0;
	void* buffer = malloc(PAGE_SIZE);
	while(p != -1)
	{
		count++;
		readPage(OverflowPage, ixFileHandle, p, buffer);
		p = IXPageBufferHelper::instance()->getNextPageID(buffer);
	}
	free(buffer);
	return count;
}



void IX_PrintError (RC rc)
{
	string ErrorMessage="";
	switch (rc){
	case ERROR_CREATEFILE:
		 ErrorMessage = "Stderr Error Information: Illegally createfile";
		 break;

	case ERROR_DESTROYFILE:
		 ErrorMessage = "Stderr Error Information: Illegally Destroyfile";
		 break;

	case ERROR_OPENFILE:
		 ErrorMessage = "Stderr Error Information: Illegally Openfile";
		 break;

	case ERROR_CLOSEFILE:
		 ErrorMessage = "Stderr Error Information: Illegally Closefile";
		 break;

	case ERROR_INSERTENTRY:
		 ErrorMessage = "Stderr Error Information: Illegally Insertentry";
		 break;
	
	case ERROR_DELETEENTRY:
		 ErrorMessage = "Stderr Error Information: Illegally Deleteentry";
		 break;

	case ERROR_PRINTINDEXENTRIES:
		 ErrorMessage = "Stderr Error Information: Illegally Printindexentries in A Page";
		 break;
	
	}
	if(rc != 0)
	{	
		fprintf(stderr, ErrorMessage.c_str());
		fprintf(stderr, "\n");
	}
}


void IXFileHandle::getMetaData(int& N, int& level, int& next)
{
	FILE* p = pagedFileHandleSecondary.p;
	fseek(p, sizeof(int),SEEK_SET);
	fread(&N, sizeof(int), 1, p);

	fseek(p, 2*sizeof(int),SEEK_SET);
	fread(&level, sizeof(int), 1, p);

	fseek(p, 3*sizeof(int),SEEK_SET);
	fread(&next, sizeof(int), 1, p);
}

void IXFileHandle::setMetaData(int N, int level, int next)
{
	FILE* p = pagedFileHandleSecondary.p;
	fseek(p, sizeof(int),SEEK_SET);
	fwrite(&N, sizeof(int), 1, p);

	fseek(p, 2*sizeof(int),SEEK_SET);
	fwrite(&level, sizeof(int), 1, p);

	fseek(p, 3*sizeof(int),SEEK_SET);
	fwrite(&next, sizeof(int), 1, p);
}

void IndexManager::readPage(PageType pageType, IXFileHandle& ixFileHandle, int pageID, void* buffer)
{
	if(pageType == PrimaryPage)
	{
		ixFileHandle.pagedFileHandlePrimary.readPage(pageID, buffer);
	}
	else if(pageType == OverflowPage)
	{
		ixFileHandle.pagedFileHandleSecondary.readPage(pageID, buffer);
	}
}

void IndexManager::writePage(PageType pageType, IXFileHandle& ixFileHandle, int pageID, void* buffer)
{
	if(pageType == PrimaryPage)
	{
		ixFileHandle.pagedFileHandlePrimary.writePage(pageID, buffer);
	}
	else if(pageType == OverflowPage)
	{
		ixFileHandle.pagedFileHandleSecondary.writePage(pageID, buffer);
	}	
}

int IndexManager::appendPageOrReuseEmptyPage(IXFileHandle& ixFileHandle, void* buffer)
{
	int emptyPagePointer = ixFileHandle.getEmptyPagePointer();
	if(emptyPagePointer == -1)
	{
		ixFileHandle.pagedFileHandleSecondary.appendPage(buffer);
		return ixFileHandle.pagedFileHandleSecondary.getNumberOfPages() - 1;
	}
	void* tempBuffer = malloc(PAGE_SIZE);
	readPage(OverflowPage, ixFileHandle, emptyPagePointer, tempBuffer);
	int nextPageID = IXPageBufferHelper::instance()->getNextPageID(tempBuffer);
	ixFileHandle.setEmptyPagePointer(nextPageID);
	free(tempBuffer);
	return emptyPagePointer;
}



IXPageBufferHelper* IXPageBufferHelper::instance()
{
    if(!_buffer_helper)
        _buffer_helper = new IXPageBufferHelper();

    return _buffer_helper;
}

int IXPageBufferHelper::getNextPageID(void* buffer)
{
	return *(int*)((char*)buffer + PAGE_SIZE - sizeof(int));
}

void IXPageBufferHelper::setNextPageID(void* buffer, int pageID)
{
	*(int*)((char*)buffer + PAGE_SIZE - sizeof(int)) = pageID;	
}

void IXPageBufferHelper::createEmptyBuffer(void* buffer)
{
	memset(buffer, 0, PAGE_SIZE);
	setNextPageID(buffer, -1);
	setFreeSpaceStart(buffer, 0);
	for(int i=0; i<PAGE_BUCKET_COUNT; i++)
		setRecordPointerInBucket(buffer, -1, i);
}

void IXPageBufferHelper::clearPageData(void* buffer)
{
	memset(buffer, 0, PAGE_SIZE - sizeof(int));//keep the field of next page ID
	setFreeSpaceStart(buffer, 0);
	for(int i=0; i<PAGE_BUCKET_COUNT; i++)
		setRecordPointerInBucket(buffer, -1, i);
}


void IXPageBufferHelper::setRecordPointerInBucket(void* buffer, int recordPointer, int bucketID)
{
	int* p = (int*)((char*)buffer + PAGE_SIZE - (3 + bucketID) * sizeof(int));
	*p = recordPointer;
}

int IXPageBufferHelper::getRecordPointerInBucket(void* buffer, int bucketID)
{
	return *(int*)((char*)buffer + PAGE_SIZE - (3 + bucketID) * sizeof(int));
}

void IXPageBufferHelper::setFreeSpaceStart(void* buffer, int freeSpaceStart)
{
	*(int*)((char*)buffer + PAGE_SIZE - 2 * sizeof(int)) = freeSpaceStart;	
}

int IXPageBufferHelper::getFreeSpaceStart(void* buffer)
{
	return *(int*)((char*)buffer + PAGE_SIZE - 2 * sizeof(int));
}

int IXPageBufferHelper::getFreeSpaceCount(void* buffer)
{
	return PAGE_SIZE - getFreeSpaceStart(buffer) - (2 + PAGE_BUCKET_COUNT) * sizeof(int);
}

bool IXPageBufferHelper::tryInsertRecordIntoPageBuffer(void* buffer, void* recordData, int recordLength, AttrType attrType)
{
	if( getFreeSpaceCount(buffer) <= recordLength + sizeof(int) )
		return false;

	int freeSpaceStart = getFreeSpaceStart(buffer);

	*(int*)((char*)buffer + freeSpaceStart) = -1; // set next record pointer to be -1

	memcpy((char*)buffer + freeSpaceStart + sizeof(int) , recordData, recordLength);

	setFreeSpaceStart(buffer, freeSpaceStart + recordLength + sizeof(int));

	int bucketID = hash_page(recordData, attrType) % PAGE_BUCKET_COUNT;
	int recordPointer = getRecordPointerInBucket(buffer, bucketID);

	if(recordPointer == -1)
	{
		setRecordPointerInBucket(buffer, freeSpaceStart, bucketID);
	}
	else
	{
		while(true)
		{
			if(*(int*)((char*)buffer + recordPointer) == -1)
				break; 
			recordPointer = *(int*)((char*)buffer + recordPointer);
		}
		*(int*)((char*)buffer + recordPointer) = freeSpaceStart;
	}
	return true;
}

bool IXPageBufferHelper::tryDeleteRecordFromPageBuffer(void* buffer, void* recordRawData, int recordLength, AttrType attrType)
{
	int bucketID = hash_page(recordRawData, attrType) % PAGE_BUCKET_COUNT;	
	int recordPointer = getRecordPointerInBucket(buffer, bucketID);
	if(recordPointer == -1)
		return false;

	int cmp = memcmp( (char*)buffer + recordPointer + sizeof(int), recordRawData, recordLength);
	if(cmp == 0)
	{
		setRecordPointerInBucket(buffer, *(int*)((char*)buffer + recordPointer), bucketID);
		return true;
	}

	int currentRecordPointer = *(int*)((char*)buffer + recordPointer);
	int lastRecordPointer = recordPointer;
	while(true)
	{
		if(currentRecordPointer == -1)
			return false;

		cmp = memcmp( (char*)buffer + currentRecordPointer + sizeof(int), recordRawData, recordLength);
		if(cmp == 0)
		{
			*(int*)((char*)buffer + lastRecordPointer) = *(int*)((char*)buffer + currentRecordPointer);
			return true;
		}

		lastRecordPointer = currentRecordPointer;
		currentRecordPointer = *(int*)((char*)buffer + currentRecordPointer);
	}
	return false;
}


unsigned IXPageBufferHelper::hash_page(const void* key, AttrType type)
{
	char* p = (char*)key;
	unsigned hashValue = 0;
	if(type == TypeVarChar)
	{
		int length = *(int*)key;
		p += sizeof(int);

		for(int i=0; i<length; i++)
		{
			if (i<4 || i == length-1 || i==length/2)
				hashValue = hashValue * 37 + *p;
			p++;
		}
	}
	else
	{
		if (type == TypeInt)
			hashValue = *(int*)key;
		else	
			hashValue = *(float*)key;
		hashValue = ((hashValue >> 16) ^ hashValue) * 0x45d9f3b;
		hashValue = ((hashValue >> 16) ^ hashValue) * 0x45d9f3b;
		hashValue = ((hashValue >> 16) ^ hashValue); 
		// The magic number was caculated using
		// a special multi-threaded test program that ran for many hours, which calculates the
		// avalance effect, independence of output bit changes, and the probability of a change
		// in each output bit if any input bit is changed.
	}
	return hashValue;
}

void IXPageBufferHelper::getAllRecordRawDataPointers(void* buffer, vector<int>& pointerList)
{
	pointerList.clear();
	for(int i=0; i<PAGE_BUCKET_COUNT; i++)
	{
		getRecordPointerListInBucket(buffer, i, pointerList);
	}
}

void IXPageBufferHelper::getRecordPointerListInBucket(void* buffer, int bucketID,vector<int>& pointerList)
{
	int recordPointer = getRecordPointerInBucket(buffer, bucketID);
	while(recordPointer != -1)
	{
		pointerList.push_back(recordPointer + sizeof(int));
		recordPointer = *(int*)((char*)buffer + recordPointer);
	}
}

void IXPageBufferHelper::printDataInPrimaryPage(void* buffer, const Attribute& attr, int pageID)
{
	vector<int> v;
	getAllRecordRawDataPointers(buffer, v);

	cout << "primary Page No." << pageID << endl;

	cout << "a. # of entries :" << v.size() << endl;
	cout << "b. entries:";

	for(int i=0; i<v.size(); i++)
	{
		int recordPointer = v[i];
		printData(attr, (char*)buffer + recordPointer);
	}
	cout << endl;
}

void IXPageBufferHelper::printDataInOverflowPage(void* buffer, const Attribute& attr, int lastPageID, int pageID)
{
	vector<int> v;
	getAllRecordRawDataPointers(buffer, v);
	cout << "overflow Page No." << pageID << " linked to [primary | overflow] page " << lastPageID << endl;
	cout << "# of entries : " << v.size() << endl;
	cout << "entries:";
	for(int i=0; i<v.size(); i++)
	{
		int recordPointer = v[i];
		printData(attr, (char*)buffer + recordPointer);		
	}
	cout << endl;
}

bool IXPageBufferHelper::IsPageEmpty(void* buffer)
{
	for(int i=0; i<PAGE_BUCKET_COUNT; i++)
	{
		if( getRecordPointerInBucket(buffer, i) != -1)
			return false;
	}
	return true;
}

