/*
 * Copyright 2012 Jianping Wang
 * wangjpzju@gmail.com
 * */

#ifndef BufferManager_H
#define BufferManager_H

#include "util.h"

//#define NIL			'$'
#define NIL				0
#define MAX_BLOCK_NUMBER	4	
#define MAX_FILE_NAME		100

using namespace std;

class buffer{
	public:
		string file_name;
		int offset;
		int lru_val;
		char buffer_vals[PAGE_SIZE+1];
		bool dirty;
		buffer(){
			buffer_init();
		}
		void buffer_init(){
			dirty=0;
			file_name="";
			offset=0;
			lru_val=0;
			/*for(int i=0;i<PAGE_SIZE;i++){
				buffer_vals[i]=NIL;
			}*/
			memset(buffer_vals, 0, PAGE_SIZE);
			buffer_vals[PAGE_SIZE]='\0';
		}
		void flush_back(){
			/*if(dirty==0){
				return;
			}*/
			char file[MAX_FILE_NAME];
			sprintf(file,"dbfile/%s",file_name.c_str());

			//cout<<"-------buffer---"<<offset<<"--------"<<endl;
			//for(int k=0;k<200;k++){
			//	cout<<buffer_vals[k];
			//}
			//cout<<endl;
			int fd=open(file,O_RDWR|O_CREAT,0);
			lseek(fd,(off_t)PAGE_SIZE*offset,SEEK_SET);
			write(fd,buffer_vals,PAGE_SIZE);
			buffer_init();
			close(fd);

		/*	fstream dataFile;
			dataFile.open(file, ios::in | ios::out);
			dataFile.seekp(PAGE_SIZE * offset);
			dataFile.write(buffer_vals, PAGE_SIZE);
			dataFile.close();*/
			//cout<<"-------buffer---"<<offset<<"--------"<<endl;
			//for(int k=0;k<200;k++){
			//	cout<<buffer_vals[k];
			//}
			//cout<<endl;
			/*FILE * fp = fopen(file,"wb");
			fseek(fp,0,SEEK_SET);
			fseek(fp,PAGE_SIZE * offset,SEEK_SET);
			fwrite(buffer_vals,PAGE_SIZE,1,fp);
			fclose(fp);*/
			//buffer_init();
		}
		void fetch(string pfile_name,int poffset){
			file_name=pfile_name;
			offset=poffset;
			lru_val=0;
			dirty=1;
			char file[MAX_FILE_NAME];
			sprintf(file,"dbfile/%s",file_name.c_str());
/*

			int fd=open(file,O_RDWR|O_CREAT,0);
			lseek(fd,(off_t)PAGE_SIZE*offset,SEEK_SET);
			read(fd,buffer_vals,PAGE_SIZE);
			close(fd);
*/
			/*fstream dataFile;
			dataFile.open(file, ios::in | ios::out);
			dataFile.seekg(PAGE_SIZE * offset);
			dataFile.read(buffer_vals, PAGE_SIZE);
			dataFile.close();*/

			FILE * fp = fopen(file,"rb");
			fseek(fp,0,SEEK_SET);
			fseek(fp,PAGE_SIZE * offset,SEEK_SET);
			fread(buffer_vals,PAGE_SIZE,1,fp);
			fclose(fp);

		}
};

class BufferManager{
	public:
		int buffer_number;
		int cell_count;
		buffer buffer_cell[MAX_BLOCK_NUMBER];
		vector<int> pin;
		BufferManager(){
			buffer_number=MAX_BLOCK_NUMBER;
			for(int i=0;i<buffer_number;i++){
				buffer_cell[i].buffer_init();
			}
			cell_count=0;
		}
		//~BufferManager(){
		//	flush_all_cells();	
		//}
		void set_pin(int pin_number){
			assert(pin_number<buffer_number);
			pin.push_back(pin_number);
		}
		void flush_all_cells(){
			for(int i=0;i<cell_count;i++){
			//	cout<<"-------buffer---"<<i<<"--------"<<endl;
			//	for(int k=0;k<200;k++){
			//		cout<<buffer_cell[i].buffer_vals[k];
			//	}
			//	cout<<endl;
				buffer_cell[i].flush_back();
			}
		}
		char *buffer_read(string pfile_name,int poffset){
			int shoot=-1;
			for(int i=0;i<cell_count;i++){
				if(!strcmp(buffer_cell[i].file_name.c_str(),pfile_name.c_str())&&buffer_cell[i].offset==poffset){//got it
					shoot=i;
				}
			}
			if(shoot!=-1){
				for(int i=0;i<cell_count;i++){
					if(i==shoot){
						buffer_cell[i].lru_val=0;
					}else{
						buffer_cell[i].lru_val++;
					}
					return buffer_cell[shoot].buffer_vals;
				}
			}else{
				if(cell_count==buffer_number){//full
					int max_lru=-1;
					int removal=-1;
					int flag=0;
					for(int i=0;i<buffer_number;i++){//locate
						if(buffer_cell[i].lru_val>max_lru){
							for(int j=0;j<pin.size();j++){//check if it is unremovable
								if(pin[j]==i){
									flag=1;
								}
							}
							if(flag!=1){
								max_lru=buffer_cell[i].lru_val;
								removal=i;
							}
							flag=0;
						}
					}
					if(removal==-1){
						std::cout<<"buffer manager is full, please check the pin blocks..."<<std::endl;
						exit(0);
					}
					for(int i=0;i<buffer_number;i++){//swap out and read into the shoot cell
						if(i==removal){
							buffer_cell[i].flush_back();
							buffer_cell[i].fetch(pfile_name,poffset);
							buffer_cell[i].lru_val=0;
							buffer_cell[i].dirty=1;
							buffer_cell[i].file_name=pfile_name;
							buffer_cell[i].offset=poffset;
						}else{
							buffer_cell[i].lru_val++;
						}
					}
					return buffer_cell[removal].buffer_vals;
				}else{
					buffer_cell[cell_count].lru_val=0;
					buffer_cell[cell_count].offset=poffset;
					buffer_cell[cell_count].fetch(pfile_name,poffset);
					buffer_cell[cell_count].dirty=1;
					buffer_cell[cell_count].file_name=pfile_name;
					for(int i=0;i<cell_count;i++){
						buffer_cell[i].lru_val++;
					}
					cell_count++;
					return buffer_cell[cell_count-1].buffer_vals;
				}
			}
		}	
		void buffer_write(string pfile_name,int poffset,char* value){			
			int shoot=-1;
			for(int i=0;i<cell_count;i++){
				if(!strcmp(buffer_cell[i].file_name.c_str(),pfile_name.c_str())&&buffer_cell[i].offset==poffset){//got it
					shoot=i;
				}
			}
			if(shoot!=-1){
				for(int i=0;i<cell_count;i++){
					if(i==shoot){
						buffer_cell[i].lru_val=0;
					}else{
						buffer_cell[i].lru_val++;
					}
				}
				//strcpy(buffer_cell[shoot].buffer_vals,value);
				for(int i=0;i<PAGE_SIZE+1;i++){
					buffer_cell[shoot].buffer_vals[i]=value[i];
				}
			}else{
				if(cell_count==buffer_number){//full
					int max_lru=-1;
					int removal=-1;
					int flag=0;
					for(int i=0;i<buffer_number;i++){//locate
						if(buffer_cell[i].lru_val>max_lru){
							for(int j=0;j<pin.size();j++){//check if it is unremovable
								if(pin[j]==i){
									flag=1;
								}
							}
							if(flag!=1){
								max_lru=buffer_cell[i].lru_val;
								removal=i;
							}
							flag=0;
						}
					}
					if(removal==-1){
						std::cout<<"buffer manager is full, please check the pin blocks..."<<std::endl;
						exit(0);
					}
					for(int i=0;i<buffer_number;i++){//swap out
						if(i==removal){
							//cout<<"------so many buffers....-----"<<endl;
							buffer_cell[i].flush_back();
							//strcpy(buffer_cell[i].buffer_vals,value);
							for(int j=0;j<PAGE_SIZE;j++){
								buffer_cell[shoot].buffer_vals[j]=value[j];	
							}
							buffer_cell[i].lru_val=0;
							buffer_cell[i].dirty=1;
							buffer_cell[i].file_name=pfile_name;
							buffer_cell[i].offset=poffset;
						}else{
							buffer_cell[i].lru_val++;
						}
					}
				}else{
					//cout<<"------so many buffers....-----"<<endl;
					buffer_cell[cell_count].lru_val=0;
					buffer_cell[cell_count].offset=poffset;
					for(int j=0;j<PAGE_SIZE;j++){
						buffer_cell[shoot].buffer_vals[j]=value[j];	
					}
					buffer_cell[cell_count].dirty=1;
					buffer_cell[cell_count].file_name=pfile_name;
					for(int i=0;i<cell_count;i++){
						buffer_cell[i].lru_val++;
					}
					cell_count++;
				}
			}
			return;
		}
		void debug(){
			for(int i=0;i<cell_count;i++){
				std::cout<<buffer_cell[i].lru_val<<" "<<buffer_cell[i].buffer_vals<<" "<<buffer_cell[i].offset<<endl;
			}
		}
};
/*
int main()
{
	BufferManager bm;
	bm.set_pin(0);
	bm.buffer_write("index",0,"1111111");
	bm.buffer_write("index",1,"4444444");
	bm.buffer_write("index",2,"8888888");
	bm.buffer_write("index",3,"5555555");
	bm.buffer_write("index",5,"3333333");
	//bm.buffer_write("index",0,"1111111");
	//bm.buffer_write("index",3,"5555555");
	//bm.buffer_write("index",4,"2222222");
	//bm.buffer_write("index",5,"3333333");
	//bm.buffer_write("index",4,"2222222");
	//bm.buffer_write("index",1,"4444444");
	bm.debug();
	//cout<<bm.buffer_read("index",0)<<endl;
	//bm.debug();
	//cout<<bm.buffer_read("index",2)<<endl;
	//bm.debug();
	//cout<<bm.buffer_read("index",5)<<endl;
	//bm.debug();
	return 0;
}

*/



#endif


