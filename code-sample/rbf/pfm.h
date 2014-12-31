#ifndef _pfm_h_
#define _pfm_h_
typedef int RC;
typedef unsigned PageNum;
#define PAGE_SIZE 4096
#define FILE_BEGIN_ADD 0
class FileHandle;
#include"stdio.h"
#include<iostream>
#include<string>
#include<string.h>
using namespace std;

class PagedFileManager
{
public:
    static PagedFileManager* instance();                     // Access to the _pf_manager instance
    
    RC createFile    (const char *fileName);                         // Create a new file
    RC destroyFile   (const char *fileName);                         // Destroy a file
    RC openFile      (const char *fileName, FileHandle &fileHandle); // Open a file
    RC closeFile     (FileHandle &fileHandle);                       // Close a file
protected:
    PagedFileManager();                                   // Constructor
    ~PagedFileManager();                                  // Destructor
    
private:
    static PagedFileManager *_pf_manager;
};


class FileHandle
{
public:
    FileHandle();                                                    // Default constructor
    ~FileHandle();                                                   // Destructor
    
    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();                                       // Get the number of pages in the file
    FILE *p;
    string name;
    int version;


    void collectCounterValues(unsigned& read, unsigned& write, unsigned& append);
private:
    unsigned readCount;
    unsigned writeCount;
    unsigned appendCount;

};

#endif
