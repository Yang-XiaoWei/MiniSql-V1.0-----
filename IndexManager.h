/*
* Copyright 2012 Yuhong Zhong
* zhong.yu.hong@163.com
*/

#ifndef _INDEX_MANAGER_H_
#define _INDEX_MANAGER_H_

#include<stack>
#include "util.h"

/*
每个indexnode的数据结构如下：
叶节点一开始一个short记录兄弟节点的前一个, 第二个short记录父节点。跟最后一个int记录下一个兄弟节点组成双向链。
根节点的父节点值为-1。没有前一个兄弟或后一个兄弟的值都为-1
有两个short长度的头。第一个表示4096BYTE中的记录数目，第二个表示每个索引的长度
-1表示INT， -2表示FLOAT，为正数表示字符串长度。
每一个索引单元由包括两个short，一个指该索引指针指向的blockNum。另一个是index值，
若-1，表示该节点不是叶节点。若非负数，表示这是个叶节点，且表示其在blockNum中index值。
*/
class IndexNode{
public:
	IndexNode(char *bufferData);
	IndexNode(const IndexNode& copyNode);
	//空间由buffer申请并返回可用的首地址
	
	void changeNode(char *copyData, int size = PAGE_SIZE);			//指定长度，主要用于节点分裂

	//返回的是下一个要查询的block号，以及在当前block中查到的查询位置。
	BlockAndIndex selectPosition(char * selectValue);
	bool isLeaf();
	bool isEmpty();
	bool isFull();
	bool isHalf();
	int getType();
	int getLength();
	short getRecordNum();
	short getTotalNum();
	char *getData();
	//只有叶节点这两个函数才有效。
	short getPreBlock();
	short getNextBlock();
	
	short getParent();
	//根据节点中的index获取其指向的block号
	short getChildBlock(int index);
	void splitLeaf(int nextBlock);
	void splitParent();
	void addRecordNum(short count);
	void subRecordNum(short count);

	BlockAndIndex queryEqual(char * queryValue);
	void memberInit();

private:
	int compareGT(char *selectValue, char *keyValue);
	bool compareEqual(char *selectValue, char *keyValue);
	
private:
	short recordNum;			//表示已有记录数
	int type;						//类型
	int length;					//表示长度。这三个变量其实就是index的head两个short表示的内容					
	short totalNum;			//该index中可以存放的索引数。
	/*
	以上三个变量都可以从block中读取出来，方便起见保存一下。
	*/
	char *data;				//index节点的内容，也就是整个block的内容，长为PAGE_SIZE

};


//B+树实现索引模块
class IndexManager{
private:
	static IndexManager *s_indexManager;

	IndexNode *root;				//root节点，常驻于内存。
	IndexNode *queryNode;	//指向当前查询的index block。一开始为root
	string file;
	short blockNum;			//记录索引文件占了几个block
	//int type;
	//int length;

private:
	IndexManager();
	//要分裂的节点。以及该节点的block号码。
	char *splitLeafNode(short splitNum);
	//char *splitParentNode(IndexNode *queryNode);
	//节点分裂时，分裂出来的节点下属的儿子们的父亲编号改了。重置
	void childChangeParent(char *splitData, short fatherNum);
	//表格名，属性名，关键字，获得该关键字所在叶子节点block号
	short getLeafBlock(const string tableName, const string attrName, char *queryValue);
	//要插入的关键字，其指向的叶节点的block号。
	//void insertInParent(char *keyValue, short splittBlock);
	//要插入的关键字，对于叶节点来说，valuePosition存储其在table文件中的block号和index下标
	//对于非叶节点插入，则表示新分裂出来块的块号。index值为-1
	void insertInParent(char *keyValue, const BlockAndIndex& valuePosition);
	//要删除的关键字, 以及当前块号
	BlockAndIndex deleteInParent(char *keyValue, short thisBlock);

public:

	static IndexManager * shared();
	void changeRoot(const string& fileName);
	IndexNode * getRoot();

	//表格信息， 要插入的值，以及其在文件中的block号和index号。
	void insertKey(string tableName, string attrName, char *keyValue, const BlockAndIndex& valuePosition);
	//表格信息，插入值的位置，插入的值，用于默认创建主键索引的插入入口。
	void insertKey(const Table& tableInfo, const BlockAndIndex& valuePosition, Row& insertValue);
	//表格名字，属性名字，要删除的关键字。
	BlockAndIndex deleteKey(string tableName, string attrName, char * keyValue);
	
	//等值查询，如果结果的blockNum值为-1，表示结果不存在。若不是，则可以读取出结果。
	BlockAndIndex queryKey(string tableName, string attrName, char *queryValue);
	void createIndex(const Table &tableInfo, const string& attrName, const string& indexName);

	void writeRoot();
};

#endif
