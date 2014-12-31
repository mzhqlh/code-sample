
#include "qe.h"
#include "../ix/ix.h"

int GHJoin::joinCount = 0;
int BNLJoin::joinCount = 0;
int INLJoin::joinCount = 0;
Filter::Filter(Iterator* input, const Condition &condition) 
{
	innerIterator = input;
	this->condition = condition;
}

bool isMeetCondition(const Condition &condition, void* data, const vector<Attribute>& attributes)
{
	if(condition.op == NO_OP)
		return true;

	char* start = (char*)data;
	char* p = (char*)data;
	int leftValueOffset;
	int rightValueOffset;
	AttrType dataType; 
	int offset = 0;
	for(unsigned i=0; i<attributes.size(); i++)
	{
		Attribute currentAttr = attributes[i];
		if(currentAttr.name == condition.lhsAttr)
		{
			dataType = currentAttr.type;
			leftValueOffset = offset;
		}

		if(condition.bRhsIsAttr && condition.rhsAttr == currentAttr.name)
		{
			rightValueOffset = offset;
		}
		if(currentAttr.type == TypeVarChar)
		{	
			int length = *(int*)p;
			p += length; 
			offset += length;
		}
		p += sizeof(int);
		offset += sizeof(int);
	}

	char* pData1 = start + leftValueOffset;
	char* pData2 = condition.bRhsIsAttr ? 
		start + rightValueOffset : 
		(char*)condition.rhsValue.data;

	return RecordBasedFileManager::instance()->judgeAttribute(pData2, pData1, condition.op, dataType);
}

RC Filter::getNextTuple(void *data)
{
	vector<Attribute> attrs; 
	getAttributes(attrs);

	while(true)
	{
		RC rc = innerIterator->getNextTuple(data);
		if(rc != 0)
		{
			return rc;
		}

		if(isMeetCondition(condition, data, attrs))
			return 0;
	}
}
// For attribute in vector<Attribute>, name it as rel.attr
void Filter::getAttributes(vector<Attribute> &attrs) const
{
	innerIterator->getAttributes(attrs);
}

RC Project::getNextTuple(void *data) 
{
	void* buffer = malloc(PAGE_SIZE);
	RC rc = innerIterator->getNextTuple(buffer);
	if(rc != 0)
	{
		free(buffer);
		return rc;
	}

	char* start = (char*)buffer;
	char* p = (char*) buffer;

	int* offsets = new int[innerAttributes.size()];
	for(unsigned i=0; i<innerAttributes.size(); i++)
	{
		offsets[i] = p - start;

		if(innerAttributes[i].type == TypeVarChar)
		{
			p += *(int*)p;
		}
		p += sizeof(int);
	}

	p = (char*) data;
	for(unsigned j=0; j<projectedAttrNames.size(); j++)
	{
		for(unsigned i=0; i<innerAttributes.size(); i++)
		{
			if(projectedAttrNames[j] == innerAttributes[i].name)
			{
				*(int*)p = *(int*)(start + offsets[i]);
				p += sizeof(int);

				if(innerAttributes[i].type == TypeVarChar)
				{
					int length = *(int*)(start + offsets[i]);
					memcpy(p, start + offsets[i] + sizeof(int), length);
					p += *(int*)(start + offsets[i]);
				} 
			}
		}
	}
	delete[] offsets;
	free(buffer);
	return 0;
}

// For attribute in vector<Attribute>, name it as rel.attr
void Project::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	
	for(unsigned i=0; i<projectedAttrNames.size(); i++)
	{
		string name = projectedAttrNames[i];

		for(unsigned j=0; j<innerAttributes.size(); j++)
		{
			if(innerAttributes[j].name == name)
			{
				attrs.push_back(innerAttributes[j]);
				break;
			}
		}
	}
}


RC GHJoin::getNextTuple(void *data)
{
	RID rid;
	RC rc = innerIterator.getNextRecord(rid, data);

	if(rc != 0)
	{
		RecordBasedFileManager::instance()->destroyFile("Merged_" + joinName);
	}
	return rc;
}

// For attribute in vector<Attribute>, name it as rel.attr
void GHJoin::getAttributes(vector<Attribute> &attrs) const
{
	leftIn->getAttributes(attrs);
	vector<Attribute> v;
	rightIn->getAttributes(v);
	for(unsigned i=0; i<v.size(); i++)
		attrs.push_back(v[i]);
}

int GetHashValue(void* data, vector<Attribute> attrs, string keyAttribute)
{
	char* p = (char*)data;
	for(unsigned i=0; i<attrs.size(); i++)
	{
		Attribute currentAttr = attrs[i];
		if(currentAttr.name == keyAttribute)
		{
			return IndexManager::instance()->hash(currentAttr, p);
		}
		if(currentAttr.type == TypeVarChar)
			p += *(int*)p;
		p += sizeof(int);
	}
	return 0;
}

void CreatePartitions(Iterator* in, int numPartitions, string* names, string keyAttribute)
{
	FileHandle* fileHandles = new FileHandle[numPartitions];
	for(int i=0; i<numPartitions; i++)
	{
		RecordBasedFileManager::instance()->createFile(names[i]);
		RecordBasedFileManager::instance()->openFile(names[i], fileHandles[i]);
	}
	vector<Attribute> attrs; 
	in->getAttributes(attrs);
	void* data = malloc(PAGE_SIZE);

	// TODO : I don't know if this is what we should do 
	RID rid;
	while(in->getNextTuple(data) == 0)
	{
		int hashValue = GetHashValue(data, attrs, keyAttribute);
		hashValue %= numPartitions;

		RecordBasedFileManager::instance()->insertRecord(fileHandles[hashValue], attrs, data, rid);
	}

	for(unsigned i=0; i<numPartitions; i++)
	{
		RecordBasedFileManager::instance()->closeFile(fileHandles[i]);
	}

	free(data);
	delete[] fileHandles;
}

void CreateMergedFile(string fileName)
{
	RecordBasedFileManager::instance()->createFile(fileName);
}

template <class T>
T getKeyValue(void* data, const vector<Attribute>& attrs, string keyAttribute)
{
	char* p = (char*)data;
	for(unsigned i=0; i<attrs.size(); i++)
	{
		if(attrs[i].name == keyAttribute)
		{
			switch(attrs[i].type)
			{
				case TypeInt:
					return *(T*)p;
				case TypeReal:
					return *(T*)p;
			}
		}

		if(attrs[i].type == TypeVarChar)
		{
			p += *(int*)p;
		}
		p += sizeof(int);
	}
}

template <> string getKeyValue<string>(void* data, const vector<Attribute>& attrs, string keyAttribute)
{
	char* p = (char*)data;
	for(unsigned i=0; i<attrs.size(); i++)
	{
		if(attrs[i].name == keyAttribute)
		{
			int length = *(int*)p;
			p+= sizeof(int);
			return string(p, length);
		}

		if(attrs[i].type == TypeVarChar)
		{
			p += *(int*)p;
		}
		p += sizeof(int);
	}
}

void AddIntoMergedFile(void* data1, void* data2, vector<Attribute> attrs1, vector<Attribute> attrs2, FileHandle& fileHandle)
{
	void* mergedData = malloc(PAGE_SIZE);
	vector<Attribute> mergedAttributes;

	char* pMerged = (char*)mergedData;
	char* p = (char*)data1;
	for(unsigned i=0; i<attrs1.size(); i++)
	{
		mergedAttributes.push_back(attrs1[i]);

		memcpy(pMerged, p, sizeof(int));
		if(attrs1[i].type == TypeVarChar)
		{
			int length = *(int*)p;
			memcpy(pMerged + sizeof(int), p + sizeof(int), length);
			p += length;
			pMerged += length;
		}
		p += sizeof(int);
		pMerged += sizeof(int);
	}

	p = (char*) data2;
	for(unsigned i=0; i<attrs2.size(); i++)
	{
		mergedAttributes.push_back(attrs2[i]);

		memcpy(pMerged, p, sizeof(int));
		if(attrs2[i].type == TypeVarChar)
		{
			int length = *(int*)p;
			memcpy(pMerged + sizeof(int), p + sizeof(int), length);
			p += length;
			pMerged += length;
		}
		p += sizeof(int);
		pMerged += sizeof(int);
	}

	RID rid;
	RecordBasedFileManager::instance()->insertRecord(fileHandle, mergedAttributes, mergedData, rid);

	free(mergedData);
}

template <class T>
void GHJoin::Probe(string* leftNames, string* rightNames)
{
	vector<Attribute> leftAttrs;
	leftIn->getAttributes(leftAttrs);

	vector<Attribute> rightAttrs;
	rightIn->getAttributes(rightAttrs);

	void* leftData = malloc(PAGE_SIZE);
	void* rightData = malloc(PAGE_SIZE);

	vector<Attribute> mergedAttributes;
	vector<string> mergedAttrNames;
	vector<string> leftAttrNames;
	for(unsigned i=0; i<leftAttrs.size(); i++)
	{
		leftAttrNames.push_back(leftAttrs[i].name);
		mergedAttrNames.push_back(leftAttrs[i].name);
		mergedAttributes.push_back(leftAttrs[i]);
	}

	vector<string> rightAttrNames;
	for(unsigned i=0; i<rightAttrs.size(); i++)
	{
		rightAttrNames.push_back(rightAttrs[i].name);
		mergedAttrNames.push_back(rightAttrs[i].name);
		mergedAttributes.push_back(rightAttrs[i]);
	}

	RID rid;
	string temp = "";
	for(unsigned i=0; i<numPartitions; i++)
	{
		FileHandle leftFileHandle;
		RecordBasedFileManager::instance()->openFile(leftNames[i], leftFileHandle);

		RBFM_ScanIterator iterator;
		RecordBasedFileManager::instance()->scan(leftFileHandle, leftAttrs, temp, NO_OP, NULL, leftAttrNames, iterator);

		map<T, vector<RID> > hashMap;
		while(iterator.getNextRecord(rid, leftData) == 0)
		{
			T keyValue = getKeyValue<T>(leftData, leftAttrs, condition.lhsAttr);
			hashMap[keyValue].push_back(rid);
		}


		FileHandle rightFileHandle;
		RecordBasedFileManager::instance()->openFile(rightNames[i], rightFileHandle);

		RBFM_ScanIterator rightIterator;
		RecordBasedFileManager::instance()->scan(rightFileHandle, rightAttrs, temp, NO_OP, NULL, rightAttrNames, rightIterator);
		
		while(rightIterator.getNextRecord(rid, rightData) == 0)
		{
			T keyValue = getKeyValue<T>(rightData, rightAttrs, condition.rhsAttr);

			for(unsigned j=0; j<hashMap[keyValue].size(); j++)
			{
				RID leftRid = hashMap[keyValue][j];
				RecordBasedFileManager::instance()->readRecord(leftFileHandle, leftAttrs, leftRid, leftData);
				AddIntoMergedFile(leftData, rightData, leftAttrs, rightAttrs, mergedFileHandle);
			}
		}

		RecordBasedFileManager::instance()->closeFile(rightFileHandle);
		RecordBasedFileManager::instance()->closeFile(leftFileHandle);
	}

	RecordBasedFileManager::instance()->scan(mergedFileHandle, mergedAttributes, temp, NO_OP, NULL, mergedAttrNames, innerIterator);

	free(leftData);
	free(rightData);
}


GHJoin::~GHJoin()
{      
    leftIn = NULL;
    rightIn = NULL;
}

void GHJoin::Init()
{
	string* leftNames = new string[numPartitions];
	string* rightNames = new string[numPartitions];

	for(unsigned i = 0; i < numPartitions; i++)
	{
		leftNames[i] = "left_" + joinName + "_" + to_string(i);
		rightNames[i] = "right_" + joinName + "_" + to_string(i);
	}

	CreatePartitions(leftIn, numPartitions, leftNames, condition.lhsAttr);
	CreatePartitions(rightIn, numPartitions, rightNames, condition.rhsAttr);

	string mergedFileName = "Merged_" + joinName;
	CreateMergedFile(mergedFileName);

	AttrType keyDataType;
	vector<Attribute> attrs;
	leftIn->getAttributes(attrs);
	for(unsigned i=0; i<attrs.size(); i++)
	{
		if(attrs[i].name == condition.lhsAttr)
		{
			keyDataType = attrs[i].type;
			break;
		}
	}

	
	RecordBasedFileManager::instance()->openFile(mergedFileName, mergedFileHandle);

	if(keyDataType == TypeInt)
	{
		Probe<int>(leftNames, rightNames);
	}
	else if(keyDataType == TypeVarChar)
	{
		Probe<string>(leftNames, rightNames);
	}
	else if(keyDataType == TypeReal)
	{
		Probe<float>(leftNames, rightNames);
	}

	for(unsigned i=0; i<numPartitions; i++)
	{
		RecordBasedFileManager::instance()->destroyFile(leftNames[i]);
		RecordBasedFileManager::instance()->destroyFile(rightNames[i]);
	}
	delete[] leftNames;
	delete[] rightNames;
}


RC BNLJoin::getNextTuple(void *data)
{
	RID rid;
	RC rc = innerIterator.getNextRecord(rid, data);
	if(rc != 0)
	{
		RecordBasedFileManager::instance()->destroyFile("BNL_Merge_" + joinName);
	}
	return rc;
}
// For attribute in vector<Attribute>, name it as rel.attr
void BNLJoin::getAttributes(vector<Attribute> &attrs) const
{
	leftIn->getAttributes(attrs);
	vector<Attribute> v;
	rightIn->getAttributes(v);
	for(unsigned i=0; i<v.size(); i++)
		attrs.push_back(v[i]);
}

bool IsKeyEqual(void* data1, void* data2, vector<Attribute> attrs1, vector<Attribute> attrs2, string keyName1, string keyName2, AttrType keyDataType)
{
	if(keyDataType == TypeInt)
	{
		int key1 = getKeyValue<int>(data1, attrs1, keyName1);
		int key2 = getKeyValue<int>(data2, attrs2, keyName2);
		return key1 == key2;
	}
	else if(keyDataType == TypeReal)
	{
		float key1 = getKeyValue<float>(data1, attrs1, keyName1);
		float key2 = getKeyValue<float>(data2, attrs2, keyName2);
		return key1 == key2;
	}
	else if(keyDataType == TypeVarChar)
	{
		string key1 = getKeyValue<string>(data1, attrs1, keyName1);
		string key2 = getKeyValue<string>(data2, attrs2, keyName2);
		return key1 == key2;
	}
	return true;
}

void BNLJoin::Init()
{
	vector<Attribute> leftAttrs;
	vector<Attribute> rightAttrs;
	
	leftIn->getAttributes(leftAttrs);
	rightIn->getAttributes(rightAttrs);

	void* rightData = malloc(PAGE_SIZE);

	string mergedFileName = "BNL_Merge_" + joinName;
	RecordBasedFileManager::instance()->createFile(mergedFileName);
	RecordBasedFileManager::instance()->openFile(mergedFileName, mergedFileHandle);

	while(true)
	{
		int recordCount = 0;
		for(int i=0; i<numRecords; i++)
		{
			if(leftIn->getNextTuple(blocks[i]) != 0)
			{
				break;
			}
			recordCount++;
		}

		if(recordCount == 0)
			break;

		while(rightIn->getNextTuple(rightData) == 0)
		{
			for(int i=0; i<recordCount; i++)
			if(IsKeyEqual(blocks[i], rightData, leftAttrs, rightAttrs, condition.lhsAttr, condition.rhsAttr,keyDataType))
			{
				//cout << "found matched records !!!!!!" << endl;
				AddIntoMergedFile(blocks[i], rightData, leftAttrs, rightAttrs, mergedFileHandle);
			}
		}

		rightIn->setIterator();
	}

	vector<Attribute> mergedAttributes;
	getAttributes(mergedAttributes);
	string temp = "";
	vector<string> mergedAttrNames;
	for(int i=0; i<mergedAttributes.size(); i++)
	{
		mergedAttrNames.push_back(mergedAttributes[i].name);
	}
	RecordBasedFileManager::instance()->scan(mergedFileHandle, mergedAttributes, temp, NO_OP, NULL, mergedAttrNames, innerIterator);

	free(rightData);
	for(int i=0; i<numRecords; i++)
	{
		free(blocks[i]);
	}
}

template <class T>
T AggregateValue(Iterator* input, Attribute aggAttr, AggregateOp op, int& count)
{
	void* data = malloc(PAGE_SIZE);
	vector<Attribute> attrs;
	input->getAttributes(attrs);

	T sum = 0;
	T max = -99999999;
	T min = 99999999;
	int count_ = 0;
	while(input->getNextTuple(data) == 0)
	{
		T keyValue = getKeyValue<T>(data, attrs, aggAttr.name);
		sum += keyValue;
		if(keyValue > max)
			max = keyValue;
		if(keyValue < min)
			min = keyValue;
		count_++;
	}

	count = count_;
	free(data);
	if(op == SUM)
		return sum;
	if(op == AVG)
		return sum / count;
	if(op == MIN)
		return min;
	if(op == MAX)
		return max;
	return 0;
}

template <>
string AggregateValue<string>(Iterator* input, Attribute aggAttr, AggregateOp op, int& count)
{
	void* data = malloc(PAGE_SIZE);
	vector<Attribute> attrs;
	input->getAttributes(attrs);
	count = 0;
	if(input->getNextTuple(data) != 0)
	{
		free(data);
		return "";
	}
	string firstKeyValue = getKeyValue<string>(data, attrs, aggAttr.name);
	string max = firstKeyValue;
	string min = firstKeyValue;

	int count_ = 1;
	while(input->getNextTuple(data) == 0)
	{
		string keyValue = getKeyValue<string>(data, attrs, aggAttr.name);
		if(keyValue > max)
			max = keyValue;
		if(keyValue < min)
			min = keyValue;
		count_++;
	}
	count = count_;
	free(data);
	if(op == MIN)
		return min;
	if(op == MAX)
		return max;
	return 0;

}

Aggregate::Aggregate(Iterator *input,          // Iterator of input R
                  Attribute aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        )
{
	returned = false;
	this->aggAttr = aggAttr;
	operation = op;
	if(aggAttr.type == TypeInt)
	{
		valInt = AggregateValue<int>(input, aggAttr, op, count);
	}
	else if(aggAttr.type == TypeReal)
	{
		valFloat = AggregateValue<float>(input, aggAttr, op, count);
	}
	else if(aggAttr.type == TypeVarChar)
	{
		valString = AggregateValue<string>(input, aggAttr, op, count);
	}
	
}

RC Aggregate::getNextTuple(void *data)
{
	if(returned == true)
		return -1;
	returned = true;
	if(operation == COUNT)
	{
		*(int*)data = count;
		return 0;
	}
	else 
	{
		if(aggAttr.type == TypeInt)
		{
			*(int*)data = valInt;
		}
		else if(aggAttr.type == TypeReal)
		{
			*(float*)data = valFloat;
		}
		else if(aggAttr.type == TypeVarChar)
		{
			*(int*)data = valString.length();
			memcpy((char*)data + sizeof(int), valString.c_str(), valString.length());
		}
	}
	return 0;
}

// Please name the output attribute as aggregateOp(aggAttr)
// E.g. Relation=rel, attribute=attr, aggregateOp=MAX
// output attrname = "MAX(rel.attr)"
void Aggregate::getAttributes(vector<Attribute> &attrs) const
{
	Attribute att = aggAttr;

	string op;
	switch(operation)
	{
		case SUM:
			op = "SUM"; break;
		case MIN: 
			op = "MIN"; break;
		case MAX:
			op = "MAX"; break;
		case AVG:
			op = "AVG"; break;
		case COUNT:
			op = "COUNT"; break;
	} 

	if(operation == COUNT)
	{
		att.type = TypeInt;
	}

	att.name = op + "(" + att.name + ")";
	attrs.clear();
	attrs.push_back(att);
}

void INLJoin::Init()
{
	void* leftData = malloc(PAGE_SIZE);
	void* rightData = malloc(PAGE_SIZE);

	vector<Attribute> leftAttrs;
	vector<Attribute> rightAttrs;
	leftIn->getAttributes(leftAttrs);
	rightIn->getAttributes(rightAttrs);

	AttrType keyDataType;
	for(int i=0; i<leftAttrs.size(); i++)
	{
		if(leftAttrs[i].name == condition.lhsAttr)
		{
			keyDataType = leftAttrs[i].type;
		}
	}

	string mergedFileName = "INL_Merge_" + joinName;
	RecordBasedFileManager::instance()->createFile(mergedFileName);
	RecordBasedFileManager::instance()->openFile(mergedFileName, mergedFileHandle);

	while(leftIn->getNextTuple(leftData) == 0)
	{
		while(rightIn->getNextTuple(rightData) == 0)
		{
			if(IsKeyEqual(leftData, rightData, leftAttrs, rightAttrs, condition.lhsAttr, condition.rhsAttr, keyDataType))
			{
				AddIntoMergedFile(leftData, rightData, leftAttrs, rightAttrs, mergedFileHandle);
			}
		}

		rightIn->setIterator(NULL, NULL, true, true);

	}

	vector<Attribute> mergedAttributes;
	getAttributes(mergedAttributes);
	string temp = "";
	vector<string> mergedAttrNames;
	for(int i=0; i<mergedAttributes.size(); i++)
	{
		mergedAttrNames.push_back(mergedAttributes[i].name);
	}
	RecordBasedFileManager::instance()->scan(mergedFileHandle, mergedAttributes, temp, NO_OP, NULL, mergedAttrNames, innerIterator);
}

RC INLJoin::getNextTuple(void *data)
{
	RID rid;
	RC rc = innerIterator.getNextRecord(rid, data);
	if(rc != 0)
	{
		RecordBasedFileManager::instance()->destroyFile("INL_Merge_" + joinName);
	}
	return rc;
}

// For attribute in vector<Attribute>, name it as rel.attr
void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
	leftIn->getAttributes(attrs);
	vector<Attribute> v;
	rightIn->getAttributes(v);
	for(unsigned i=0; i<v.size(); i++)
		attrs.push_back(v[i]);
}





