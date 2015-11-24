/*
 * Copyright 2012 Jianping Wang
 * wangjpzju@gmail.com
 * */

#ifndef UTIL_H
#define UTIL_H

#include <iostream>
#include <fstream>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdint.h>
#include <string.h>
#include <vector>

using namespace std;

#define MAX_LEN	100

#define PAGE_SIZE		4096

#define EMY	0
#define CREATE_TABLE	1
#define INSERT		2
#define SELECT_NOWHERE	3			
#define CREATE_INDEX	4
#define DROP_TABLE	5
#define DELETE_NOWHERE	6
#define DELETE_WHERE	10
#define QUIT		7
#define DROP_INDEX	8
#define SELECT_WHERE	9
#define ERROR		11


#define INT		0
#define FLOAT		1
#define CHAR		2


#define EQUAL		0	//=
#define NO_EQUAL	1
#define LARGER		2	//>
#define SMALLER		3	//<
#define	NO_LARGER	4	//<=
#define NO_SMALLER	5	//>=

static short INDEX_INT = -1;
static short INDEX_FLOAT = -2;

class Condition
{
public:
        string attr;
        int op;
        string value;
};

class Row
{
public:
        vector<string> val;
};

class Attribute
{
public:
        string name;
        int type;
        int length;
        bool primary;
        bool unique;
};

class Index
{
public:
        string name;
        string table;
        string attr;
};

class Table
{
public:
        string name;
        int attr_num;
        int record_cnt;//how many record
	int block_cnt;//how many block it has occupied
	int length;
        vector<Attribute> attrs;
        //vector<Index> idx;
	//vector<int> available;
	//Table(){
	//	available.push_back(0);
	//}
};

class BlockAndIndex{
public:
	short blockNum;
	short index;
	short queryNum;
};

#endif
