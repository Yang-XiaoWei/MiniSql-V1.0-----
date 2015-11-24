/*
 * Copyright 2012 Jianping Wang
 * wangjpzju@gmail.com
 * */

#ifndef RECORDMANAGER_H
#define RECORDMANAGER_H

#include "util.h"
//#include <sstream>

class RecordManager
{
private:
	static RecordManager * s_recordManager;

private:
	RecordManager();
	bool compare(string value1, string value2, int operation, int type);

public:
	static RecordManager *shared();
	
	char * serialize(const Table& table_info,const Row& insert_row);

	Row unserialize(Table& table_info, char *all);

	void create_table(Table& table_info);

	BlockAndIndex insert(Table& table_info, Row& insert_row);

	bool meet_condition(Row &row, vector<Condition> &cond, vector<Attribute> attrs);

	void delete_without_condition(Table &table_info);

	void delete_with_condition(Table &table_info,vector<Condition> &cond);

	vector<Row> select_with_condition(Table &table_info,vector<Condition> &cond);

	vector<Row> select_without_condition(Table &table_info);

};


#endif





