#include "CatalogManager.h"
#include "IndexManager.h"

CatalogManager * CatalogManager::s_catalogManager = NULL;
CatalogManager * CatalogManager::shared(){
	static bool s_firstUsedCatalog = true;
	if(s_firstUsedCatalog){
		s_firstUsedCatalog = false;
		s_catalogManager = new CatalogManager();
	}
	return s_catalogManager;
}

CatalogManager::CatalogManager(){

}

void CatalogManager::create_table(const Table pt){
	cTables.push_back(pt);
	char file[100];
	sprintf(file,"dbfile/%s.record", pt.name.c_str());
	fstream dataFile(file, ios::out);
	dataFile.close();

}

void CatalogManager::drop_table(const Table& tableInfo){
	//这里有个陷阱，当我删除cTables里的表格信息是，tableInfo的值也没了。
	//因为传进来的是个引用。在函数外部是用一个指针取值传进来的。绕过const修改了这块内容。
	string tableName = tableInfo.name;
	string deleteFile = "dbfile/" + tableName + ".record";
	if(remove(deleteFile.c_str()) == -1){
		cout << "delete file fail!" << endl;
		return;
	}
	else
		cout << "delete file success!" << endl;

	for(vector<Table>::iterator iter = cTables.begin(); iter != cTables.end(); ){
		if(iter->name == tableName){
			iter = cTables.erase(iter);
			break;
		}
		else
			iter ++;
	}
	
	for(vector<Index>::iterator iter = cIndex.begin(); iter != cIndex.end(); ){
		if(iter->table == tableName){
			deleteFile = "dbfile/" + iter->name + ".index";
			if(remove(deleteFile.c_str()) == -1){
				cout << "delete index file" << deleteFile << "fail!" << endl;
			}
			else
				cout << "delete index file" << deleteFile << "success!" << endl;

			iter = cIndex.erase(iter);
		}
		else
			iter ++;
	}


}

void CatalogManager::read_catalog(){
	fstream catalogFile("dbfile/miniSQL.catalog", ios::in);
	if(! catalogFile){
		cout << "catalog file not exit!" << endl;
		return;
	}
	char *intData = new char[sizeof(int)];
	char *strData = new char[MAX_STRING];
	memset(strData, 0, MAX_STRING);
	//先读入多少个表格和多少个索引文件
	catalogFile.read(intData, sizeof(int));
	int tableNum = *(int *)intData;
	catalogFile.read(intData, sizeof(int));
	int indexNum = *(int *)intData;

	Attribute tmpAttr;
	Table tmpTable;
	Index tmpIndex;

	for(int i = 0; i < tableNum; ++ i){
		//表格名字
		catalogFile.read(strData, MAX_STRING);
		tmpTable.name.assign(strData);
		//属性数量
		catalogFile.read(intData, sizeof(int));
		tmpTable.attr_num = *(int *)intData;
		//block 数量
		catalogFile.read(intData, sizeof(int));
		tmpTable.block_cnt = *(int *)intData;
		//记录表中记录数量和记录长度。
		catalogFile.read(intData, sizeof(int));
		tmpTable.record_cnt = *(int *)intData;
		catalogFile.read(intData, sizeof(int));
		tmpTable.length = *(int *)intData;

		tmpTable.attrs.clear();
		for(int j = 0; j < tmpTable.attr_num; ++ j){
			//属性名
			catalogFile.read(strData, MAX_STRING);
			tmpAttr.name.assign(strData);
			//是否主键
			catalogFile.read(intData, sizeof(int));
			tmpAttr.primary = *(int *)intData;
			//是否唯一
			catalogFile.read(intData, sizeof(int));
			tmpAttr.unique = *(int *)intData;
			//类型
			catalogFile.read(intData, sizeof(int));
			tmpAttr.type = *(int *)intData;
			//长度
			catalogFile.read(intData, sizeof(int));
			tmpAttr.length = *(int *)intData;
			//将属性放入表格记录
			tmpTable.attrs.push_back(tmpAttr);
		}
		cTables.push_back(tmpTable);
	}

	for(int i = 0; i < indexNum; ++ i){
		//所属表名
		catalogFile.read(strData, MAX_STRING);
		tmpIndex.table.assign(strData);
		//索引名
		catalogFile.read(strData, MAX_STRING);
		tmpIndex.name.assign(strData);
		//索引所指的属性名
		catalogFile.read(strData, MAX_STRING);
		tmpIndex.attr.assign(strData);
		cIndex.push_back(tmpIndex);
	}

	if(cTables.empty())
		cout << "no table in the database!" << endl;

	delete[] intData;
	delete[] strData;

	catalogFile.close();
	//if()
}

void CatalogManager::write_catalog(){
	fstream catalogFile("dbfile/miniSQL.catalog", ios::out);
	int tableNum = cTables.size();
	int indexNum = cIndex.size();
	char *intData = new char[sizeof(int)];
	char *strData = new char[MAX_STRING];
	memset(strData, 0, MAX_STRING);
	//先写有多少个表格和多少个索引文件。
	*(int *)intData = tableNum;
	catalogFile.write(intData, sizeof(int));
	*(int *)intData = indexNum;
	catalogFile.write(intData, sizeof(int));

	int nameLength = 0;
	//依次记录每个表格的信息。
	for(int i = 0; i < tableNum; ++ i){
		//记录表名
		nameLength = cTables[i].name.length();
		catalogFile.write(cTables[i].name.c_str(), nameLength);
		catalogFile.write(strData, MAX_STRING - nameLength);
		//记录表中属性数量以及文件占用的block数量
		*(int *)intData = cTables[i].attr_num;
		catalogFile.write(intData, sizeof(int));
		*(int *)intData = cTables[i].block_cnt;
		catalogFile.write(intData, sizeof(int));
		//记录表中记录数量和记录长度。
		*(int *)intData = cTables[i].record_cnt;
		catalogFile.write(intData, sizeof(int));
		*(int *)intData = cTables[i].length;
		catalogFile.write(intData, sizeof(int));
		//记录每个属性的名字，是否主见，是否唯一，类型，长度
		for(int j = 0; j < cTables[i].attr_num; ++j){
			//属性名
			nameLength = cTables[i].attrs[j].name.length();
			catalogFile.write(cTables[i].attrs[j].name.c_str(), nameLength);
			catalogFile.write(strData, MAX_STRING - nameLength);
			//是否主键
			*(int *)intData = cTables[i].attrs[j].primary;
			catalogFile.write(intData, sizeof(int));
			//是否唯一
			*(int *)intData = cTables[i].attrs[j].unique;
			catalogFile.write(intData, sizeof(int));
			//类型
			*(int *)intData = cTables[i].attrs[j].type;
			catalogFile.write(intData, sizeof(int));
			//长度
			*(int *)intData = cTables[i].attrs[j].length;
			catalogFile.write(intData, sizeof(int));
		}
	}

	for(int i = 0; i < indexNum; ++ i){
		//所属表名
		nameLength = cIndex[i].table.length();
		catalogFile.write(cIndex[i].table.c_str(), nameLength);
		catalogFile.write(strData, MAX_STRING - nameLength);
		//索引名
		nameLength = cIndex[i].name.length();
		catalogFile.write(cIndex[i].name.c_str(), nameLength);
		catalogFile.write(strData, MAX_STRING - nameLength);
		//索引所指的属性名
		nameLength = cIndex[i].attr.length();
		catalogFile.write(cIndex[i].attr.c_str(), nameLength);
		catalogFile.write(strData, MAX_STRING - nameLength);

	}

	delete[] intData;
	delete[] strData;

	catalogFile.close();
}

const string CatalogManager::search_index(const string& tableName, const string& attrName){
	for(vector<Index>::iterator iter = cIndex.begin(); iter != cIndex.end(); ++ iter){
		if(iter->table == tableName && iter->attr == attrName)
			return iter->name;
	}
	return "\0";
}

bool CatalogManager::create_index(const Index& indexInfo){
	for(vector<Index>::iterator iter = cIndex.begin(); iter != cIndex.end(); ++ iter){
		if(iter->name == indexInfo.name){
			cout << "index name exist!" << endl;
			return false;
		}
		else if(iter->attr == indexInfo.attr){
			cout << "there is already an index file been created!" << endl;
			return false;
		}
	}
	cIndex.push_back(indexInfo);
	return true;
}

bool CatalogManager::create_index(const Table& tableInfo){
	for(vector<Attribute>::const_iterator iter = tableInfo.attrs.begin(); iter != tableInfo.attrs.end(); ++ iter){
		if(iter->primary == true){
			Index newIndex;
			newIndex.attr = iter->name;
			newIndex.table = tableInfo.name;
			newIndex.name = tableInfo.name + "_" + iter->name;
			create_index(newIndex);
			IndexManager::shared()->createIndex(tableInfo, iter->name, newIndex.name);
		}
	}

	return false;
}

void CatalogManager::drop_index(string indexName){
	string fileName = "dbfile/" + indexName + ".index";
	if(remove(fileName.c_str()) == -1){
		cout << "delete index file fail!" << endl;
		return;
	}
	else
		cout << "delete index file success!" << endl;

	for(vector<Index>::iterator iter = cIndex.begin(); iter != cIndex.end(); ){
		if(iter->name == indexName){
			iter = cIndex.erase(iter);
			return;
		}
		else
			++ iter;
	}

}

Table* CatalogManager::search_table(string tbl_name){
	int flag=0;
	//cout<<tbl_name<<endl;
	for(int i=0;i<cTables.size();i++){
		if(!strcmp(cTables[i].name.c_str(),tbl_name.c_str())){
			flag=1;
			//cout<<"found the table"<<endl;
			return &cTables[i];
		}
	}
	if(flag==0){
		//cout<<"found no table"<<endl;
		return NULL;
	}
}

void CatalogManager::drop_table(string tbl_name){
	vector<Table>::iterator j;
	Table tmp;
	for(j=cTables.begin();j!=cTables.end();j++){
		tmp=*j;
		if(!strcmp(tmp.name.c_str(),tbl_name.c_str())){
			cTables.erase(j);
		}
	}
}

void CatalogManager::debug(){
	for(int i=0;i<cTables.size();i++){
		cout<<cTables[i].name<<endl;
		cout<<cTables[i].block_cnt<<endl;
		cout<<cTables[i].attr_num<<endl;
		for(int j=0;j<cTables[i].attrs.size();j++){
			cout<<cTables[i].attrs[j].name<<" "<<cTables[i].attrs[j].type<<" "<<
				cTables[i].attrs[j].length<<" "<<cTables[i].attrs[j].primary<<" "<<cTables[i].attrs[j].unique<<endl;
		}
	}
}
