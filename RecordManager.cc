#include "RecordManager.h"
#include "BufferManager.h"
#include "IndexManager.h"
#include "CatalogManager.h"

#define IN_USE	'*'

//only used for temporary, not a good habit
#define COMPARE_RESULT(op) \
	return type == INT ? vInt1 op vInt2 : (type == FLOAT ? vFloat1 op vFloat2 : value1 op value2);

extern BufferManager bm;

RecordManager * RecordManager::s_recordManager = NULL;

RecordManager * RecordManager::shared(){
	static bool s_firstUsedRecord = true;
	if(s_firstUsedRecord){
		s_firstUsedRecord = false;
		s_recordManager = new RecordManager();
	}

	return s_recordManager;
}

RecordManager::RecordManager(){

}

bool RecordManager::compare(string value1, string value2, int operation, int type){
	int vInt1, vInt2;
	float vFloat1, vFloat2;
	string valueTmp;
	if(type == INT){
		vInt1 = atoi(value1.c_str());
		vInt2 = atoi(value2.c_str());
	}
	else if(type == FLOAT){
		vFloat1 = atof(value1.c_str());
		vFloat2 = atof(value2.c_str());
	}

	switch(operation){
	case EQUAL:
		COMPARE_RESULT(==);
	case NO_EQUAL:
		COMPARE_RESULT(!=);
	case LARGER:
		COMPARE_RESULT(>);
	case SMALLER:
		COMPARE_RESULT(<);
	case NO_LARGER:
		COMPARE_RESULT(<=);
	case NO_SMALLER:
		COMPARE_RESULT(>=);
	default:
		return false;
	}

};

char * RecordManager::serialize(const Table& table_info,const Row& insert_row){
		char *all=new char[table_info.length+2];
		all[0]=IN_USE;
		string tmp;
		int p=1;
		for(int i=0;i<insert_row.val.size();i++){
			if(table_info.attrs[i].type==INT){
				int t=atoi(insert_row.val[i].c_str());
				/*for(int j=0;j<4;j++){
					char a=(char)t%(1<<8);
					t=t/(1<<8);
					all[p++]=a;
				}*/
				*(int *)(all + p) = t;
				p += 4;
			}
			if(table_info.attrs[i].type==FLOAT){
				/*string left="";
			        string right="";
				int stop=0;
				for(int j=0;j<strlen(insert_row.val[i].c_str());j++){
					if(insert_row.val[i][j]=='.'){
						stop=j;
						break;
					}
				}
				for(int j=0;j<stop;j++){
					left+=insert_row.val[i][j];
				}
				for(int j=stop+1;j<strlen(insert_row.val[i].c_str());j++){
					right+=insert_row.val[i][j];
				}
				int t1=atoi(left.c_str());
				int t2=atoi(right.c_str());
				for(int j=0;j<2;j++){
					char a=(char)t1%(1<<8);
					t1=t1/(1<<8);
					all[p++]=a;
				}
				for(int j=0;j<2;j++){
					char a=(char)t2%(1<<8);
					t2=t2/(1<<8);
					all[p++]=a;
				}*/
				float t = atof(insert_row.val[i].c_str());
				*(float *)(all + p) = t;
				p += 4;
			}
			if(table_info.attrs[i].type==CHAR){
				for(int j=0;j<strlen(insert_row.val[i].c_str());j++){
					all[p++]=insert_row.val[i][j];
				}
				int rest=table_info.attrs[i].length-strlen(insert_row.val[i].c_str());
				for(int j=0;j<rest;j++){
					all[p++]=NIL;
				}
			}
		}
		
		return all;
	}

Row RecordManager::unserialize(Table& table_info, char *all){
		int p=1;//begin to parse
		Row tmp;
		for(int i=0;i<table_info.attrs.size();i++){
			if(table_info.attrs[i].type==INT){
				/*int aa=0;
				string as="";
				int b=0;
				int a[4];
				a[0]=(int)all[p++];
				a[1]=(int)all[p++];
				a[2]=(int)all[p++];
				a[3]=(int)all[p++];
				aa=a[3]*(1<<24)+a[2]*(1<<16)+a[1]*(1<<8)+a[0];
				ostringstream oss;
				oss<<aa;
				as=oss.str();*/
				int a = *(int *)(all + p);
				p += 4;
				char intData[20];
				sprintf(intData, "%d", a);
				string as(intData);
				tmp.val.push_back(as);
			}
			if(table_info.attrs[i].type==FLOAT){
				/*int left=0;
				string lefts="";
				int right=0;
				string rights="";
				int b=0;
				int a[4];
				a[0]=(int)all[p++];
				a[1]=(int)all[p++];
				a[2]=(int)all[p++];
				a[3]=(int)all[p++];
				left=a[0]+a[1]*(1<<8);
				right=a[2]+a[3]*(1<<8);
				ostringstream oss,oss1;
				oss<<left;
				lefts=oss.str();
				oss1<<right;
				rights=oss1.str();
				string all_=lefts+'.'+rights;*/
				//cout<<all_<<endl;
				float fData = *(float *)(all + p);
				p += 4;
				char fC[20];
				sprintf(fC, "%f", fData);
				string all_(fC);
				tmp.val.push_back(fC);
			}
			if(table_info.attrs[i].type==CHAR){
				string str="";
				int beg=p;
				int flag=0;
				for(;p<table_info.attrs[i].length+beg;p++){
					if(all[p]!='0'&&flag==0){
						str+=all[p];
					}else{
						flag=1;
					}
				}
				tmp.val.push_back(str);
			}
		}
		return tmp;
		
	}

void RecordManager::create_table(Table& table_info){
		table_info.block_cnt++;
		return;
	}

BlockAndIndex RecordManager::insert(Table& table_info, Row& insert_row){
		BlockAndIndex insertPosition = {-1, -1, -1};

		char * all=NULL;
		all=serialize(table_info,insert_row);
		//cout<<all<<endl;
		Row read_row=unserialize(table_info, all);
		for(int i=0;i<read_row.val.size();i++){
			//cout<<read_row.val[i]<<endl;
		}
		int record_len=table_info.length+1;
		int record_count=PAGE_SIZE/record_len;
		string file_name=table_info.name+".record";
		char my_block[4096+1]="";
		for(int i=0;i<4096+1;i++){
			my_block[i]=NIL;
		}
		char *buf=NULL;
		int offset=0;
		int full=1;
		char *check_all=new char[table_info.length+2];
		for(int i=0;i<table_info.block_cnt;i++){
			buf=bm.buffer_read(file_name,i);//each block
			//copy the buf to my_block
			for(int kk=0;kk<4096+1;kk++){
				my_block[kk]=buf[kk];
			}
			//before we insert in such blocks, check if the primary key dump
			for(int k=0;k<record_count;k++){
				offset=record_len*k;
				if(buf[offset]!=NIL){
					for(int r=0;r<record_len;r++){
						check_all[r]=buf[offset+r];
						//cout<<(int)(char)buf[offset+r]<<" ";		
					}
					Row tmp=unserialize(table_info,check_all);
					int priy_num=0;
					int uni_num=0;
					for(int j=0;j<table_info.attrs.size();j++){
						//check primary key and unique
						if(table_info.attrs[j].primary==true){
							priy_num=j;
						}
						if(table_info.attrs[j].unique==true){
							uni_num=j;
						}
					}
					if(tmp.val[priy_num]==insert_row.val[priy_num]){
						cout<<"a primay attribute has already existed!"<<endl;
						return insertPosition;
					}
					if(tmp.val[uni_num]==insert_row.val[uni_num]){
						cout<<"an unique attribute has already existed!"<<endl;
						return insertPosition;	
					}
				}
			}
			//to see if there is a place to insert it!
			for(int k=0;k<record_count;k++){
				offset=record_len*k;
				if(my_block[offset]==NIL){
					//there is a place to insert
					//you don't have to split another block
					//cout<<"-------keeping------"<<endl;
					full=0;
					for(int r=0;r<record_len;r++){
						//buf[offset+r]=all[r];
						my_block[offset+r]=all[r];
						insertPosition.blockNum = i;
						insertPosition.index = k;
						//cout<<(int)(char)buf[offset+r]<<" ";
					}
					bm.buffer_write(file_name,i,my_block);
					goto final;//goto is fun!!
				}else{
					//do nothing
				}
			}
		}
final:
		if(full==1){
			//cout<<"-----split--------"<<endl;
			buf=bm.buffer_read(file_name,table_info.block_cnt);//the new block
			for(int kk=0;kk<4096+1;kk++){
				my_block[kk]=buf[kk];
			}
			for(int k=0;k<record_count;k++){
				offset=record_len*k;
				if(my_block[offset]==NIL){
					for(int r=0;r<record_len;r++){
						my_block[offset+r]=all[r];
						//cout<<(int)(unsigned char)buf[offset+r]<<" ";
					}
					bm.buffer_write(file_name,table_info.block_cnt,my_block);
					table_info.block_cnt++;//increase!!!!
					goto exit;
				}else{
				//do nothing
				}
			}
		}
exit:
		delete[] all;
		return insertPosition;
	}

bool RecordManager::meet_condition(Row &row, vector<Condition> &cond, vector<Attribute> attrs){
		int *ret=new int[cond.size()];
		for(int i=0;i<cond.size();i++){
			ret[i]=0;
		}
		for(int i=0;i<cond.size();i++){
			int attr_num=0;
			for(int j=0;j<attrs.size();j++){
				if(attrs[j].name==cond[i].attr){
					attr_num=j;
					break;
				}
			}/*
			if(cond[i].op==EQUAL){
				if(row.val[attr_num]==cond[i].value){
					ret[i]=1;
				}else{
					ret[i]=0;
				}
			}
			if(cond[i].op==NO_EQUAL){
				if(row.val[attr_num]!=cond[i].value){
					ret[i]=1;
				}else{
					ret[i]=0;
				}
			}
			if(cond[i].op==LARGER){
				if(row.val[attr_num]>cond[i].value){
					ret[i]=1;
				}else{
					ret[i]=0;
				}
			}
			if(cond[i].op==SMALLER){
				if(row.val[attr_num]<cond[i].value){
					ret[i]=1;
				}else{
					ret[i]=0;
				}
			}
			if(cond[i].op==NO_LARGER){
				if(row.val[attr_num]<=cond[i].value){
					ret[i]=1;
				}else{
					ret[i]=0;
				}
			}
			if(cond[i].op==NO_SMALLER){
				if(row.val[attr_num]>=cond[i].value){
					ret[i]=1;
				}else{
					ret[i]=0;
				}
			}*/
			string strRowValue;
			for(int j = 0; j < row.val[attr_num].size(); ++ j){
				if(row.val[attr_num][j] != 0)
					strRowValue += row.val[attr_num][j];
				else
					break;
			}
			if(compare(strRowValue, cond[i].value, cond[i].op, attrs[attr_num].type))
				ret[i] = 1;
			else
				ret[i] = 0;
		}
		int flag=0;
		for(int k=0;k<cond.size();k++){
			if(ret[k]==0){
				flag=1;
			}
		}
		if(flag==0){
			return true;
		}else{
			return false;
		}
	}

void RecordManager::delete_without_condition(Table &table_info){
		
		char *all=new char[table_info.length+2];
		int record_len=table_info.length+1;
		int record_count=PAGE_SIZE/record_len;
		string file_name=table_info.name+".record";
		char *buf=NULL;
		int offset=0;
		Row tmp;
		for(int i=0;i<table_info.block_cnt;i++){
			buf=bm.buffer_read(file_name,i);
			for(int k=0;k<record_count;k++){
				offset=record_len*k;
				buf[offset]=NIL;
			}
		}
		return;
	}

void RecordManager::delete_with_condition(Table &table_info,vector<Condition> &cond){

		if(cond.size() == 1 && cond.at(0).op == EQUAL){
			string searchResult = CatalogManager::shared()->search_index(table_info.name, cond[0].attr);
			if(searchResult != "\0"){
				int type = INT;
				int length = 0;
				for(vector<Attribute>::iterator iter = table_info.attrs.begin(); iter != table_info.attrs.end(); ++ iter){
					if(iter->name == cond[0].attr){
						type = iter->type;
						length = iter->length;
						break;
					}
				}
				char *queryKey = NULL;
				if(type == INT){
					queryKey = new char[sizeof(int)];
					*(int *)queryKey = atoi(cond[0].value.c_str());
				}
				else if(type == FLOAT){
					queryKey = new char[sizeof(float)];
					*(float *)queryKey = atof(cond[0].value.c_str());
				}
				else{
					queryKey = new char[length];
					memset(queryKey, 0, length);
					memcpy(queryKey, cond[0].value.c_str(), cond[0].value.length());
				}
				BlockAndIndex result = IndexManager::shared()->deleteKey(table_info.name, cond.at(0).attr, queryKey);

				if(result.blockNum != -1){
					//在index中查询得到
					string fileName = table_info.name + ".record";
					char *buf = bm.buffer_read(fileName, result.blockNum);
					buf[(1 + table_info.length) * result.index] = NIL;
				}
				else
					cout << "data not exist in the table" << endl;

				delete[] queryKey;
			}

		}



		
		char *all=new char[table_info.length+2];
		int record_len=table_info.length+1;
		int record_count=PAGE_SIZE/record_len;
		string file_name=table_info.name+".record";
		char *buf=NULL;
		int offset=0;
		Row tmp;
		for(int i=0;i<table_info.block_cnt;i++){
			buf=bm.buffer_read(file_name,i);
			//cout<<file_name<<endl;
			for(int k=0;k<record_count;k++){
				offset=record_len*k;
				if(buf[offset]!=NIL){//first bit, for the no emtpy records
					for(int r=0;r<record_len;r++){
						all[r]=buf[offset+r];
						//cout<<(int)(char)buf[offset+r]<<" ";		
					}
					tmp=unserialize(table_info,all);
					//for(int j=0;j<tmp.val.size();j++){
						//cout<<tmp.val[j]<<endl;
					//}
					bool tmp_bool=meet_condition(tmp,cond,table_info.attrs);
					//cout<<tmp_bool<<endl;
					if(tmp_bool==true){
						buf[offset]=NIL;
						bm.buffer_cell[i].dirty=true;//has been modified
					}
					tmp.val.clear();
					//goto final;//goto is fun
				}
			}
		}
	final:
		return;
	}

vector<Row> RecordManager::select_with_condition(Table &table_info,vector<Condition> &cond){
		vector<Row> ret;
		//use the index to search to result
		if(cond.size() == 1 && cond.at(0).op == EQUAL){
			string searchResult = CatalogManager::shared()->search_index(table_info.name, cond[0].attr);
			if(searchResult != "\0"){
				int type = INT;
				int length = 0;
				for(vector<Attribute>::iterator iter = table_info.attrs.begin(); iter != table_info.attrs.end(); ++ iter){
					if(iter->name == cond[0].attr){
						type = iter->type;
						length = iter->length;
						break;
					}
				}
				char *queryKey = NULL;
				if(type == INT){
					queryKey = new char[sizeof(int)];
					*(int *)queryKey = atoi(cond[0].value.c_str());
				}
				else if(type == FLOAT){
					queryKey = new char[sizeof(float)];
					*(float *)queryKey = atof(cond[0].value.c_str());
				}
				else{
					queryKey = new char[length];
					memset(queryKey, 0, length);
					memcpy(queryKey, cond[0].value.c_str(), cond[0].value.length());
				}
				BlockAndIndex result = IndexManager::shared()->queryKey(table_info.name, cond.at(0).attr, queryKey);
				if(result.blockNum != -1){
					//在index中查询得到
					string fileName = table_info.name + ".record";
					char *buf = bm.buffer_read(fileName, result.blockNum);
					Row tmp = unserialize(table_info, buf + (1 + table_info.length) * result.index);
					ret.push_back(tmp);
				}
				else
					cout << "data not exist in the table" << endl;

				delete[] queryKey;
				return ret;

			}

		}

		char *all=new char[table_info.length+2];
		int record_len=table_info.length+1;
		int record_count=PAGE_SIZE/record_len;
		string file_name=table_info.name+".record";
		//cout<<file_name<<endl;
		char *buf=NULL;
		int offset=0;
		for(int i=0;i<table_info.block_cnt;i++){
			buf=bm.buffer_read(file_name,i);
			
			for(int k=0;k<record_count;k++){
				offset=record_len*k;
				if(buf[offset]!=NIL){//first bit, for the no emtpy records
					for(int r=0;r<record_len;r++){
						all[r]=buf[offset+r];
					}
					Row tmp=unserialize(table_info,all);
					//for(int j=0;j<tmp.val.size();j++){
					//	cout<<tmp.val[j]<<endl;
					//}
					bool tmp_bool=meet_condition(tmp,cond,table_info.attrs);
					//cout<<tmp_bool<<endl;
					if(tmp_bool==true){//meet the criteria
						ret.push_back(tmp);
					}
					//goto final;//goto is fun
				}
			}
		}
		return ret;
	}

vector<Row> RecordManager::select_without_condition(Table &table_info){
		vector<Row> ret;
		char *all=new char[table_info.length+2];
		int record_len=table_info.length+1;
		int record_count=PAGE_SIZE/record_len;
		string file_name=table_info.name+".record";
		char *buf=NULL;
		int offset=0;
		for(int i=0;i<table_info.block_cnt;i++){
			buf=bm.buffer_read(file_name,i);
			
			for(int k=0;k<record_count;k++){
				offset=record_len*k;
				if(buf[offset]!=NIL){//first bit, for the no emtpy records
					for(int r=0;r<record_len;r++){
						all[r]=buf[offset+r];
					}
					Row tmp=unserialize(table_info,all);
					ret.push_back(tmp);
				}
			}
		}
		return ret;
	}

