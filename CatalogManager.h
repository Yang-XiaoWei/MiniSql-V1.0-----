/*
 * Copyright 2012 Yuhong Zhong
 * zhong.yu.hong@163.com
*/
/*
catalog 文件内容。首先记录有表格数量，然后是index数量。
1. 然后，表格名字，属性数量, 占用块数量。后跟属性内容，如下：属性名，是否主键，是否唯一，类型，以及长度。
记录完属性后。
重复1的过程直到记录完所有表格信息。
2.index内容如下：所属表格名字，index名字，指向的属性名字。
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
	//用于手动指定index名字建立索引
	bool create_index(const Index& indexInfo);
	//用于默认创建主键索引，名字为“表名_属性名”
	bool create_index(const Table& tableInfo);

	void drop_index(string indexName);

	Table* search_table(string tbl_name);
	void drop_table(string tbl_name);
	void debug();
};

#endif
