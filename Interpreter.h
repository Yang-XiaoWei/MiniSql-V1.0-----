/*
 * Copyright 2012 Jianping Wang
 * wangjpzju@gmail.com
 * */

#ifndef INTERPRETER_H
#define INTERPRETER_H

#include <iostream>
#include <string.h>
#include <vector>

#include "util.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

using namespace std;

class Interpreter
{
public:
        int opt;
        string table_name;
        string index_name;
        Row insert_row;
        vector<string> cmd;
        Table table_info;//Index included
        vector<Attribute> all_attr;
        Index index_info;
        vector<Condition> cond;
        Interpreter() {
                opt=EMY;
                table_name="";
                index_name="";
        }
        ~Interpreter() {
        }
	void clear(){
		opt=EMY;
                table_name="";
		index_name="";
		cmd.clear();
		cond.clear();
		all_attr.clear();
		insert_row.val.clear();
	}
        void split(char *cmd_all) {
                char tmp[MAX_LEN];
                int j=0;
                int flag=0;
                for(int i=0; i<strlen(cmd_all); i++) {
                        while(cmd_all[i]!=' '&&cmd_all[i]!=','&&cmd_all[i]!=';'&&cmd_all[i]!='('&&cmd_all[i]!=')'&&cmd_all[i]!='\'') {
                                tmp[j++]=cmd_all[i++];
                                flag=1;
                                if(i==strlen(cmd_all)) {
                                        break;
                                }
                        }
                        if(flag==1) {
                                tmp[j]='\0';
                                cmd.push_back(tmp);
                                j=0;
                        }
                        flag=0;
                }

        }
        void parser() {
                Attribute tmpa;
		if(!strcmp(cmd[0].c_str(),"create")&&strcmp(cmd[1].c_str(),"table")&&strcmp(cmd[1].c_str(),"index")){
			//cout<<"here"<<endl;
			opt=ERROR;
			return;
		}
                if(!strcmp(cmd[0].c_str(),"create")&&!strcmp(cmd[1].c_str(),"table")) {
                        opt = CREATE_TABLE;
			table_name=cmd[2];
                        table_info.name=cmd[2];
			table_info.record_cnt=0;
			table_info.block_cnt=1;
                        string key;
                        vector<string> uv;
                        int len=0;
                        for(int i=0; i<cmd.size(); i++) {
                                if(!strcmp(cmd[i].c_str(),"primary")) {
                                        len=i;
                                        key=cmd[i+2];
                                        break;
                                }
                        }
                        vector<string> new_cmd;
                        for(int i=3; i<len; i++) {
                                if(!strcmp(cmd[i].c_str(),"unique")) {
                                        if(!strcmp(cmd[i-2].c_str(),"char")) {
                                                uv.push_back(cmd[i-3]);
                                        } else {
                                                uv.push_back(cmd[i-2]);
                                        }
                                } else {
                                        new_cmd.push_back(cmd[i]);
                                }
                        }
                        Attribute tmpa;
			int total_length=0;
                        for(int i=0; i<new_cmd.size(); i++) {
                                if(!strcmp(new_cmd[i+1].c_str(),"char")) {
                                        tmpa.length=atoi(new_cmd[i+2].c_str());
					total_length+=tmpa.length;
                                        tmpa.name=new_cmd[i];
                                        tmpa.type=CHAR;
                                        i=i+2;
                                        all_attr.push_back(tmpa);
                                } else {
                                        if(!strcmp(new_cmd[i+1].c_str(),"int")) {
                                                tmpa.length=4;
						total_length+=4;
                                                tmpa.name=new_cmd[i];
                                                tmpa.type=INT;
                                                i=i+1;
                                                all_attr.push_back(tmpa);
                                        } else {
                                                if(!strcmp(new_cmd[i+1].c_str(),"float")) {
                                                        tmpa.length=4;
							total_length+=4;
                                                        tmpa.name=new_cmd[i];
                                                        tmpa.type=FLOAT;
                                                        i=i+1;
                                                        all_attr.push_back(tmpa);
                                                }
						else{
							opt=ERROR;
							return;
						}
                                        }
                                }
                        }
                        for(int i=0; i<all_attr.size(); i++) {
                                all_attr[i].primary=false;
                                all_attr[i].unique=false;
                                if(!strcmp(all_attr[i].name.c_str(),key.c_str())) {
                                        all_attr[i].primary=true;

                                } else {
                                        for(int k=0; k<uv.size(); k++) {
                                                if(!strcmp(all_attr[i].name.c_str(),uv[k].c_str())) {
                                                        all_attr[i].unique=true;
                                                }
                                        }
                                }
                        }
                        Index tmpi;
                        tmpi.name=key;
                        tmpi.table=table_name;
                        index_info=tmpi;
			table_info.attr_num=all_attr.size();
			table_info.attrs=all_attr;
			table_info.length=total_length;
                        //cout<<table_info.length<<endl<<table_info.name<<endl<<table_info.attr_num<<endl;
			//for(int i=0;i<table_info.attrs.size();i++){
			//	cout<<table_info.attrs[i].type<<endl;
			//}
			return;

                }
                if(!strcmp(cmd[0].c_str(),"insert")) {
                        table_name=cmd[2];
                        opt=INSERT;
                        for(int i=4; i<cmd.size(); i++) {
                                insert_row.val.push_back(cmd[i]);
				//cout<<cmd[i]<<endl;
			}
			return;
                }
                if(!strcmp(cmd[0].c_str(),"select")) {
                        table_name=cmd[3];
                        int beg=0;
                        for(int k=0; k<cmd.size(); k++) {
                                if(!strcmp(cmd[k].c_str(),"where")) {
					opt=SELECT_WHERE;
                                        beg=k;
                                        break;
                                }
				if(k+1==cmd.size()){
					opt=SELECT_NOWHERE;
				}
                        }
                        Condition tmpc;
                        for(int i=beg+1; i<cmd.size(); i++) {
                                if(!strcmp(cmd[i].c_str(),"and")) {
                                        continue;
                                }
                                tmpc.attr=cmd[i];
                                if(!strcmp(cmd[i+1].c_str(),"=")) {
                                        tmpc.op=EQUAL;
                                } else if(!strcmp(cmd[i+1].c_str(),">")) {
                                        tmpc.op=LARGER;
                                } else if(!strcmp(cmd[i+1].c_str(),"<")) {
                                        tmpc.op=SMALLER;
                                } else if(!strcmp(cmd[i+1].c_str(),">=")) {
                                        tmpc.op=NO_SMALLER;
                                } else if(!strcmp(cmd[i+1].c_str(),"<=")) {
                                        tmpc.op=NO_LARGER;
                                } else if(!strcmp(cmd[i+1].c_str(),"<>")) {
                                        tmpc.op=NO_EQUAL;
                                } else {
                                }
								if(i + 2 >= cmd.size()){
									cout << "error at the last of the command!" << endl;
									exit(0);
								}
                                tmpc.value=cmd[i+2];
                                cond.push_back(tmpc);
                                i=i+2;
                        }
			return;
                }
                if(!strcmp(cmd[0].c_str(),"create")&&!strcmp(cmd[1].c_str(),"index")) {
                        opt=CREATE_INDEX;
                        if(cmd.size() != 6){
							cout << "too many words in create index" << endl;
							exit(0);
						}
						if(cmd[3] != "on"){
							cout << "error nearby index name" << endl;
							exit(0);
						}

                        index_info.name = cmd[2];
						index_info.table = cmd[4];
						index_info.attr = cmd[5];
                        //printf("%s %s\n",tmpi.attr.c_str(),tmpi.name.c_str());
			return;
                }
                if(!strcmp(cmd[0].c_str(),"delete")) {
                        table_name=cmd[2];
                        int flag=0;
                        for(int i=0; i<cmd.size(); i++) {
                                if(!strcmp(cmd[i].c_str(),"where")) {
					opt=DELETE_WHERE;
                                        flag=1;
                                        break;
                                }
				if(i+1==cmd.size()){
					opt=DELETE_NOWHERE;
				}
                        }
                        if(flag==0) {

                        } else {
                                int beg=0;
                                for(int k=0; k<cmd.size(); k++) {
                                        if(!strcmp(cmd[k].c_str(),"where")) {
                                                beg=k;
                                                break;
                                        }
                                }
                                Condition tmpc;
                                for(int i=beg+1; i<cmd.size(); i++) {
                                        if(!strcmp(cmd[i].c_str(),"and")) {
                                                continue;
                                        }
                                        tmpc.attr=cmd[i];
                                        if(!strcmp(cmd[i+1].c_str(),"=")) {
                                                tmpc.op=EQUAL;
                                        } else if(!strcmp(cmd[i+1].c_str(),">")) {
                                                tmpc.op=LARGER;
                                        } else if(!strcmp(cmd[i+1].c_str(),"<")) {
                                                tmpc.op=SMALLER;
                                        } else if(!strcmp(cmd[i+1].c_str(),">=")) {
                                                tmpc.op=NO_SMALLER;
                                        } else if(!strcmp(cmd[i+1].c_str(),"<=")) {
                                                tmpc.op=NO_LARGER;
                                        } else if(!strcmp(cmd[i+1].c_str(),"<>")) {
                                                tmpc.op=NO_EQUAL;
                                        } else {
                                        }

										if(i + 2 >= cmd.size()){
											cout << "error at the last of the command!" << endl;
											exit(0);
										}

                                        tmpc.value=cmd[i+2];
                                        cond.push_back(tmpc);
                                        i=i+2;
                                }
                        }
                        //for(int i=0; i<cond.size(); i++) {
                        //        printf("%s %d %s\n",cond[i].attr.c_str(),cond[i].op,cond[i].value.c_str());
                        //}
			return;
                }
                if(!strcmp(cmd[0].c_str(),"drop")) {
                        if(!strcmp(cmd[1].c_str(),"table")) {
                                opt=DROP_TABLE;
                                table_name=cmd[2];
                        } else {
                                if(!strcmp(cmd[1].c_str(),"index")) {
                                        opt=DROP_INDEX;
                                        index_info.name=cmd[2];
                                } else {
                                }
                        }
			return;
                }
                if(!strcmp(cmd[0].c_str(),"quit")) {
                        opt=QUIT;
			return;
                }
		opt=ERROR;
		return;
        }

};


#endif
