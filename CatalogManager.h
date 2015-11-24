/*
 * Copyright 2012 Yuhong Zhong
 * zhong.yu.hong@163.com
*/
/*
catalog �ļ����ݡ����ȼ�¼�б��������Ȼ����index������
1. Ȼ�󣬱�����֣���������, ռ�ÿ�����������������ݣ����£����������Ƿ��������Ƿ�Ψһ�����ͣ��Լ����ȡ�
��¼�����Ժ�
�ظ�1�Ĺ���ֱ����¼�����б����Ϣ��
2.index�������£�����������֣�index���֣�ָ����������֡�
*/

#ifndef CATALOGMANAGER_H
#define CATALOGMANAGER_H

#include "Interpreter.h"
#include "util.h"

static const int MAX_STRING = 32;

class CatalogManager{
private:
	vector<Table> cTables;
	vector<Index> cIndex;
	static CatalogManager * s_catalogManager;

private:
	CatalogManager();

public:
	//string file_name="miniSQL.catalog";
	static CatalogManager * shared();
	
	void create_table(const Table pt);
	void drop_table(const Table& tableInfo);
	void read_catalog();
	void write_catalog();

	const string search_index(const string& tableName, const string& attrName);
	//�����ֶ�ָ��index���ֽ�������
	bool create_index(const Index& indexInfo);
	//����Ĭ�ϴ�����������������Ϊ������_��������
	bool create_index(const Table& tableInfo);

	void drop_index(string indexName);

	Table* search_table(string tbl_name);
	void drop_table(string tbl_name);
	void debug();
};

#endif
