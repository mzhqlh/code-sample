#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include "pfm.h"
#include <string.h>


PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}

PagedFileManager::PagedFileManager()
{
}

PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const char *fileName)
{	
	struct stat stFileInfo;
	if(stat(fileName,&stFileInfo) == 0) return -1;
	FILE *fstream;
	if ((fstream = fopen(fileName, "w+b"))!= NULL)
	{	
		PageNum pagenum = 0;
		fseek(fstream, 0, SEEK_SET);
		fwrite(&pagenum, sizeof(PageNum),1,fstream);
		fflush(fstream);
		fclose(fstream);
		return 0;
	}
	return -1;
}


RC PagedFileManager::destroyFile(const char *fileName)
{
	  return remove(fileName);
}


RC PagedFileManager::openFile(const char *fileName, FileHandle &fileHandle)
{
	if (fileHandle.p != NULL)
	{
		return -1;
	}
	if ((fileHandle.p = fopen (fileName, "r+b")) != NULL)
	{
		fileHandle.name = fileName;
		return 0;
	}	
	else
	{
		return -1;
	}
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    if(!fileHandle.p)
		return -1;
	if (!fclose(fileHandle.p))
	{
		fflush(fileHandle.p);
		fileHandle.name.clear();
		fileHandle.p = NULL;
		return 0;	
	}
	return -1;
}

FileHandle::FileHandle() : readCount(0), writeCount(0), appendCount(0)
{
	version = 0;
 	p = NULL;
}


FileHandle::~FileHandle()
{
	
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
    if (!p)
		return -1;

	//if (getNumberOfPages() <= pageNum)
	//	return -1;

	fseek(p, FILE_BEGIN_ADD + PAGE_SIZE * (pageNum +1), SEEK_SET);
	if (fread(data, PAGE_SIZE, 1, p)!=1)
		  return -1;	
	readCount++;
	return 0;
}


RC FileHandle::appendPage(const void *data)
{
    if (p == NULL)
		return -1;

	PageNum pagenum = getNumberOfPages() + 1;
	fseek(p, FILE_BEGIN_ADD,SEEK_SET);
	if (fwrite(&pagenum, sizeof(PageNum), 1, p) != 1)
		return -1;
	fflush(p);
	appendCount++;
	return writePage(pagenum - 1, data);
}

RC FileHandle::writePage( PageNum pageNum, const void *data)
{
	if (p == NULL)
	   return -1;

	//if ( getNumberOfPages() <= pageNum)
	//    return -1;

	fseek(p, FILE_BEGIN_ADD + PAGE_SIZE * (pageNum+1), SEEK_SET);
	if (fwrite(data, PAGE_SIZE, 1, p)!=1)
	    return -1;
	fflush(p);
	writeCount++;
	return 0;
}


unsigned FileHandle::getNumberOfPages()
{
	if(!p)
	{
		cout << "error" << endl;
	}
   	
   	PageNum pageNum;
    fseek(p, 0, SEEK_SET);
   fread(&pageNum,sizeof(PageNum),1,p);
   return pageNum;
}

void FileHandle::collectCounterValues(unsigned& read, unsigned& write, unsigned& append)
{
	read = readCount;
	write = writeCount;
	append = appendCount;
}

