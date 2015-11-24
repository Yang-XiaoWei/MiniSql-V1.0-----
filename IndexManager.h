/*
* Copyright 2012 Yuhong Zhong
* zhong.yu.hong@163.com
*/

#ifndef _INDEX_MANAGER_H_
#define _INDEX_MANAGER_H_

#include<stack>
#include "util.h"

/*
ÿ��indexnode�����ݽṹ���£�
Ҷ�ڵ�һ��ʼһ��short��¼�ֵܽڵ��ǰһ��, �ڶ���short��¼���ڵ㡣�����һ��int��¼��һ���ֵܽڵ����˫������
���ڵ�ĸ��ڵ�ֵΪ-1��û��ǰһ���ֵܻ��һ���ֵܵ�ֵ��Ϊ-1
������short���ȵ�ͷ����һ����ʾ4096BYTE�еļ�¼��Ŀ���ڶ�����ʾÿ�������ĳ���
-1��ʾINT�� -2��ʾFLOAT��Ϊ������ʾ�ַ������ȡ�
ÿһ��������Ԫ�ɰ�������short��һ��ָ������ָ��ָ���blockNum����һ����indexֵ��
��-1����ʾ�ýڵ㲻��Ҷ�ڵ㡣���Ǹ�������ʾ���Ǹ�Ҷ�ڵ㣬�ұ�ʾ����blockNum��indexֵ��
*/
class IndexNode{
public:
	IndexNode(char *bufferData);
	IndexNode(const IndexNode& copyNode);
	//�ռ���buffer���벢���ؿ��õ��׵�ַ
	
	void changeNode(char *copyData, int size = PAGE_SIZE);			//ָ�����ȣ���Ҫ���ڽڵ����

	//���ص�����һ��Ҫ��ѯ��block�ţ��Լ��ڵ�ǰblock�в鵽�Ĳ�ѯλ�á�
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
	//ֻ��Ҷ�ڵ���������������Ч��
	short getPreBlock();
	short getNextBlock();
	
	short getParent();
	//���ݽڵ��е�index��ȡ��ָ���block��
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
	short recordNum;			//��ʾ���м�¼��
	int type;						//����
	int length;					//��ʾ���ȡ�������������ʵ����index��head����short��ʾ������					
	short totalNum;			//��index�п��Դ�ŵ���������
	/*
	�����������������Դ�block�ж�ȡ�����������������һ�¡�
	*/
	char *data;				//index�ڵ�����ݣ�Ҳ��������block�����ݣ���ΪPAGE_SIZE

};


//B+��ʵ������ģ��
class IndexManager{
private:
	static IndexManager *s_indexManager;

	IndexNode *root;				//root�ڵ㣬��פ���ڴ档
	IndexNode *queryNode;	//ָ��ǰ��ѯ��index block��һ��ʼΪroot
	string file;
	short blockNum;			//��¼�����ļ�ռ�˼���block
	//int type;
	//int length;

private:
	IndexManager();
	//Ҫ���ѵĽڵ㡣�Լ��ýڵ��block���롣
	char *splitLeafNode(short splitNum);
	//char *splitParentNode(IndexNode *queryNode);
	//�ڵ����ʱ�����ѳ����Ľڵ������Ķ����ǵĸ��ױ�Ÿ��ˡ�����
	void childChangeParent(char *splitData, short fatherNum);
	//����������������ؼ��֣���øùؼ�������Ҷ�ӽڵ�block��
	short getLeafBlock(const string tableName, const string attrName, char *queryValue);
	//Ҫ����Ĺؼ��֣���ָ���Ҷ�ڵ��block�š�
	//void insertInParent(char *keyValue, short splittBlock);
	//Ҫ����Ĺؼ��֣�����Ҷ�ڵ���˵��valuePosition�洢����table�ļ��е�block�ź�index�±�
	//���ڷ�Ҷ�ڵ���룬���ʾ�·��ѳ�����Ŀ�š�indexֵΪ-1
	void insertInParent(char *keyValue, const BlockAndIndex& valuePosition);
	//Ҫɾ���Ĺؼ���, �Լ���ǰ���
	BlockAndIndex deleteInParent(char *keyValue, short thisBlock);

public:

	static IndexManager * shared();
	void changeRoot(const string& fileName);
	IndexNode * getRoot();

	//�����Ϣ�� Ҫ�����ֵ���Լ������ļ��е�block�ź�index�š�
	void insertKey(string tableName, string attrName, char *keyValue, const BlockAndIndex& valuePosition);
	//�����Ϣ������ֵ��λ�ã������ֵ������Ĭ�ϴ������������Ĳ�����ڡ�
	void insertKey(const Table& tableInfo, const BlockAndIndex& valuePosition, Row& insertValue);
	//������֣��������֣�Ҫɾ���Ĺؼ��֡�
	BlockAndIndex deleteKey(string tableName, string attrName, char * keyValue);
	
	//��ֵ��ѯ����������blockNumֵΪ-1����ʾ��������ڡ������ǣ�����Զ�ȡ�������
	BlockAndIndex queryKey(string tableName, string attrName, char *queryValue);
	void createIndex(const Table &tableInfo, const string& attrName, const string& indexName);

	void writeRoot();
};

#endif
