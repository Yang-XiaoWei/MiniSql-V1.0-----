/*
 * Copyright 2012 Jianping Wang
 * wangjpzju@gmail.com
 * */


#include <iostream>

#include "util.h"
#include "CatalogManager.h"
#include "Interpreter.h"
#include "BufferManager.h"
#include "RecordManager.h"
#include "IndexManager.h"

using namespace std;

Interpreter ip;
BufferManager bm; 

void list_tuples(vector<Row> &list)
{
	for(int i=0;i<list.size();i++){
		for(int j=0;j<list[i].val.size();j++){
			cout<<list[i].val[j]<<" ";
		}
		cout<<endl;
	}
}
void api()
{
	Table *ti;
	vector<Row> list;
	BlockAndIndex insertPosition = {-1, -1, -1};
	switch(ip.opt){
	case CREATE_TABLE:
		ti=CatalogManager::shared()->search_table(ip.table_name);
		//cout<<(ti==NULL)<<endl;
		if(ti!=NULL){
			cout<<"table already exists"<<endl;
			break;
		}
		CatalogManager::shared()->create_table(ip.table_info);
		CatalogManager::shared()->create_index(ip.table_info);
		//CatalogManager::shared()->debug();
		cout<<"create table successfully"<<endl;
		break;
	case INSERT:
		ti=CatalogManager::shared()->search_table(ip.table_name);
		if(ti==NULL){
			cout<<"table doesn't exist"<<endl;
			break;
		}
		insertPosition = RecordManager::shared()->insert(*ti,ip.insert_row);
		//文件可以插入，则插入idnex。
		if(insertPosition.blockNum != -1)
			IndexManager::shared()->insertKey(*ti, insertPosition, ip.insert_row);

		break;
	case SELECT_WHERE:
		ti=CatalogManager::shared()->search_table(ip.table_name);
		if(ti==NULL){
			cout<<"table doesn't exist"<<endl;
			break;
		}
		list=RecordManager::shared()->select_with_condition(*ti,ip.cond);
		if(list.size()==0){
			cout<<"no record"<<endl;
			break;
		}
		list_tuples(list);
		list.clear();
		break;
	case SELECT_NOWHERE:
		ti=CatalogManager::shared()->search_table(ip.table_name);
		if(ti==NULL){
			cout<<"table doesn't exist"<<endl;
			break;
		}
		list=RecordManager::shared()->select_without_condition(*ti);
		if(list.size()==0){
			cout<<"no record"<<endl;
			break;
		}
		list_tuples(list);
		list.clear();
		break;
	case DELETE_WHERE:
		ti=CatalogManager::shared()->search_table(ip.table_name);
		if(ti==NULL){
			cout<<"table doesn't exist"<<endl;
			break;
		}
		RecordManager::shared()->delete_with_condition(*ti,ip.cond);
		break;
	case DELETE_NOWHERE:
		ti=CatalogManager::shared()->search_table(ip.table_name);
		if(ti==NULL){
			cout<<"table doesn't exist"<<endl;
			break;
		}
		RecordManager::shared()->delete_without_condition(*ti);
		break;
	case CREATE_INDEX:
		ti = CatalogManager::shared()->search_table(ip.index_info.table);
		if(ti == NULL)
			cout << "table not exist" << endl;
		else{
			if(CatalogManager::shared()->create_index(ip.index_info))
				IndexManager::shared()->createIndex(*ti, ip.index_info.attr, ip.index_info.name);
		}
		break;
	case DROP_TABLE:
		ti = CatalogManager::shared()->search_table(ip.table_name);
		if(ti == NULL){
			cout << "table not exist!" << endl;
			return;
		}
		CatalogManager::shared()->drop_table(*ti);
		break;
	case DROP_INDEX:
		CatalogManager::shared()->drop_index(ip.index_info.name);
		break;
	case ERROR:
		cout<<"sql command error, check it"<<endl;
		break;
	case QUIT:
		CatalogManager::shared()->write_catalog();
		IndexManager::shared()->writeRoot();
		bm.flush_all_cells();
		exit(0);
	}
};

int main()
{

	CatalogManager::shared()->read_catalog();
	//CatalogManager::shared()->debug();
        char cmd[MAX_LEN]="";
        char tmp[MAX_LEN];
	cout<<"welcome to miniSQL"<<endl;
	for(;;){
		//cout<<">>";
	        for(;;) {
        	        cin.getline(tmp,MAX_LEN);
                	if(tmp[strlen(tmp)-1]==';') {
                        	strcat(cmd,tmp);
                        	break;
                	} else {
                        	strcat(cmd,tmp);
                	}
        	}
		if(strlen(cmd)==1){
			cout<<"empty command, retype it"<<endl;
			continue;
		}
	/*
		if(strcmp(cmd, "quit;") == 0)
				break;*/
	
		ip.split(cmd);
        	ip.parser();
		//cout<<">>";
		api();
		ip.clear();
		strcpy(cmd,"");
		strcpy(tmp,"");
	}
       
        return 0;
}


