#include <memory>
#include <fstream>
#include <iostream>
#include "IndexManager.h"
#include "BufferManager.h"
#include "CatalogManager.h"
#include "RecordManager.h"

using namespace std;

extern BufferManager bm;

IndexNode::IndexNode(char *bufferData){
	data = bufferData;
	memberInit();
}

IndexNode::IndexNode(const IndexNode& copyNode){
	data = copyNode.data;
	recordNum = copyNode.recordNum;
	type = copyNode.type;
	length = copyNode.length;
	totalNum = copyNode.totalNum;
}

void IndexNode::changeNode(char *copyData, int size){
	data = copyData;
	memberInit();
}

void IndexNode::memberInit(){
	recordNum = *(short *)(data + 2 * sizeof(short));
	type = *(short *)(data + 3 * sizeof(short));
	if(type == INDEX_INT){
		length = sizeof(int);
	}
	else if(type == INDEX_FLOAT){
		length = sizeof(float);
	}
	else if(type > 0){
		length = type;
	}

	totalNum = (PAGE_SIZE - 6 * sizeof(short)) / (2 * sizeof(short) + length);
}

bool IndexNode::isLeaf(){
	return *(short *)(data + 5 * sizeof(short)) != -1;
}

bool IndexNode::isEmpty(){
	return recordNum == 0;
}

bool IndexNode::isFull(){
	return recordNum == totalNum;
}

bool IndexNode::isHalf(){
	return recordNum < totalNum / 2;
}

int IndexNode::getType(){
	return type;
}

int IndexNode::getLength(){
	return length;
}

short IndexNode::getRecordNum(){
	return recordNum;
}

short IndexNode::getTotalNum(){
	return totalNum;
}

char * IndexNode::getData(){
	return data;
}

short IndexNode::getPreBlock(){
	return *(short *)(data);
}

short IndexNode::getNextBlock(){
	return *(short *)(data + 4 * sizeof(short) + recordNum * (2 * sizeof(short) + length));
}

short IndexNode::getParent(){
	return *(short *)(data + sizeof(short));
}

short IndexNode::getChildBlock(int index){
	return *(short *)(data + 4 * sizeof(short) + (2 * sizeof(short) + length) * index);
}

int IndexNode::compareGT(char *selectValue, char *keyValue){
	if(type == INDEX_INT){
		if(*(int *)selectValue > *(int *)keyValue)
			return LARGER;
		else if(*(int *)selectValue == *(int *)keyValue)
			return EQUAL;
		else
			return SMALLER;
	}
	else if(type == INDEX_FLOAT){
		if(*(float *)selectValue > *(float *)keyValue)
			return LARGER;
		else if(*(float *)selectValue == *(float *)keyValue)
			return EQUAL;
		else
			return SMALLER;
	}
	else{
		int i = 0;
		//�ַ����Ƚ�
		do {
			if(selectValue[i] - keyValue[i] > 0)
				return LARGER;
			else if(selectValue[i] - keyValue[i] < 0)
				return SMALLER;
			else
				++ i;
		} while (i < length);
		if(i == length)
			return EQUAL;
	}

}

bool IndexNode::compareEqual(char *selectValue, char *keyValue){
	if(type == INDEX_INT)
		return *(int *)selectValue == *(int *)keyValue;
	else if(type == INDEX_FLOAT)
		return *(float *)selectValue == *(float *)keyValue;
	else{
		int i = 0;
		//�ַ����Ƚ�
		do {
			if(selectValue[i] == 0 && keyValue[i] == 0)
				return true;
			if(selectValue[i] - keyValue[i] != 0)
				return false;
			else
				++ i;
		} while (i < length);
		if(i == length)
			return true;
	}
}

BlockAndIndex IndexNode::selectPosition(char * selectValue){
	BlockAndIndex position = {-1, -1, -1};
	int curPoint = sizeof(short) * 6;
	int result = 0;

	int comResult;

	//selectValue�Ǵ��ͽ����Ƚϵ����ݵ�ַ��
	//�ҵ���һ��<=selectValueָ���ֵ������ֵ����������block�е���š�
	for(int i = 0; i < recordNum; ++i){
		comResult = compareGT(selectValue, data + curPoint);
		if(comResult == LARGER){
			curPoint += length + sizeof(short) * 2;
			result ++;
		}
		else if(comResult == EQUAL){
			curPoint += length + sizeof(short) * 2;
			break;
		}
		else
			break;
	}

	curPoint -= sizeof(short) * 2;
	position.blockNum = *(short *)(data + curPoint);
	position.index = *(short *) (data + curPoint + sizeof(short));
	position.queryNum = result;

	return position;
	
}

void IndexNode::splitLeaf(int nextBlock){
	*(short *)(data + 2 * sizeof(short)) = recordNum / 2;
	*(short *)(data + 4 * sizeof(short) + (2 * sizeof(short) + length) * recordNum / 2) = nextBlock;
	recordNum = recordNum / 2;
}

void IndexNode::splitParent(){
	*(short *)(data + 2 * sizeof(short)) = recordNum / 2;
	recordNum = recordNum / 2;
}

void IndexNode::addRecordNum(short count){
	recordNum += count;
	*(short *)(data + 2 * sizeof(short)) = recordNum;
}

void IndexNode::subRecordNum(short count){
	recordNum -= count;
	*(short *)(data + 2 * sizeof(short)) =  recordNum;
}

BlockAndIndex IndexNode::queryEqual(char * queryValue){
	BlockAndIndex result = {-1, -1, -1};
	int curPoint = sizeof(short) * 6;
	for(int i = 0; i < recordNum; ++i){
		if(compareEqual(queryValue, data + curPoint)){
			result.blockNum = *(short *)(data + curPoint - 2 * sizeof(short));
			result.index = *(short *)(data + curPoint - sizeof(short));
			result.queryNum = i;
			break;
		}
		curPoint += (2 * sizeof(short) + length);
	}

	return result;
}

//#pragma mark -
//#pragma mark IndexManager

IndexManager* IndexManager::s_indexManager = NULL;

IndexManager * IndexManager::shared(){
	static bool s_firstUseIndex = true;
	if(s_firstUseIndex == true){
		s_firstUseIndex = false;
		s_indexManager = new IndexManager();
	}

	return s_indexManager;
}

IndexManager::IndexManager(){
	char *tmp = new char[PAGE_SIZE];
	memset(tmp, 0, PAGE_SIZE);
	root = new IndexNode(tmp);
	queryNode = new IndexNode(tmp);
	file = "";
	//type = length = 0;
}

void IndexManager::writeRoot(){
	string writeFile = "dbfile/" + file;
	fstream dataFile(writeFile.c_str(), ios::in | ios::out);
	dataFile.write(root->getData(), PAGE_SIZE);
}

void IndexManager::changeRoot(const string& fileName){
	string tmpFile;
	if(file != ""){
		//����ͷ��㣬��ͷ�������д�ش���
		tmpFile = "dbfile/" + file;
		fstream rootFile(tmpFile.c_str(), ios::in | ios::out);
		rootFile.write(root->getData(), PAGE_SIZE);
		rootFile.close();
	}
	file = fileName;
	tmpFile = "dbfile/" + file;
	fstream dataFile(tmpFile.c_str(), ios::in);
	char *indexData = root->getData();
	dataFile.read(indexData, PAGE_SIZE);
	root->memberInit();
	
	//type = root->getType();
	//length = root->getLength();
	dataFile.seekg(0, ios::end);
	int end = dataFile.tellg();

	if(end % PAGE_SIZE == 0)
		blockNum =  end / PAGE_SIZE - 1;
	else
		blockNum = end / PAGE_SIZE;
	dataFile.close();
}

IndexNode * IndexManager::getRoot(){
	return root;
}

char *IndexManager::splitLeafNode(short splitNum){
	short splitBlock = ++ blockNum; //��index�ļ�������һ��block
	//�޸���һ��block�ļ���ǰһ��blockָ����������block
	//���һ��block���ø�
	if(queryNode->getNextBlock() != -1){
		char *splitNext = bm.buffer_read(file, queryNode->getNextBlock());
		*(short *)(splitNext) = splitBlock;
	}

	//������ѳ�����block��ͷ�ֽ�
	char *splitData = bm.buffer_read(file, splitBlock);
	*(short *)(splitData) = splitNum;
	*(short *)(splitData + sizeof(short)) = queryNode->getParent();
	*(short *)(splitData + 2 * sizeof(short)) = queryNode->getRecordNum() - queryNode->getRecordNum() / 2;
	*(short *)(splitData + 3 * sizeof(short)) = queryNode->getType();
	//����ԭ�ڵ�
	int copyStart = sizeof(short) * 4 + (2 * sizeof(short) + queryNode->getLength()) * queryNode->getRecordNum() / 2;
	int copyLength = (2 * sizeof(short) + queryNode->getLength()) *(queryNode->getRecordNum() - queryNode->getRecordNum() / 2) + 2 * sizeof(short);
	memcpy(splitData + 4 * sizeof(short), queryNode->getData() + copyStart, copyLength);
	queryNode->splitLeaf(splitBlock);

	return splitData;
}

void IndexManager::childChangeParent(char *splitData, short fatherNum){
	int recordNum = *(short *)(splitData + 2 * sizeof(short));
	int recordLength = root->getLength();
	int curPoint = 4 * sizeof(short);
	int childBlock;
	char *childTmp;
	for(int i = 0; i < recordNum + 1; ++ i){
		childBlock = *(short *)(splitData + curPoint);
		curPoint += 2 * sizeof(short) + recordLength;
		childTmp = bm.buffer_read(file, childBlock);
		*(short *)(childTmp + sizeof(short)) = fatherNum;
	}

}

void IndexManager::insertKey(string tableName, string attrName, char *keyValue, const BlockAndIndex& valuePosition){
	if(getLeafBlock(tableName, attrName, keyValue)== -1)
		return;

	insertInParent(keyValue, valuePosition);

}

void IndexManager::insertKey(const Table& tableInfo, const BlockAndIndex& valuePosition, Row& insertValue){
	int offset = 0;
	char *value = RecordManager::shared()->serialize(tableInfo, insertValue);
	for(vector<Attribute>::const_iterator iter = tableInfo.attrs.begin(); iter != tableInfo.attrs.end(); ++ iter){
		//����������unique��������������ļ��Ļ��������в�����ֵ��
		if(iter->primary || iter->unique){
			string indexName = CatalogManager::shared()->search_index(tableInfo.name, iter->name);
			if(indexName != "\0"){
				insertKey(tableInfo.name, iter->name, value + 1 + offset, valuePosition);
			}
		}
		if(iter->type == INT)
			offset += sizeof(int);
		else if(iter->type == FLOAT)
			offset += sizeof(float);
		else
			offset += iter->length;
	}

	delete[] value;
}

void IndexManager::insertInParent(char *keyValue, const BlockAndIndex& valuePosition){
	BlockAndIndex result = queryNode->selectPosition(keyValue);
	char *queryData = queryNode->getData();
	int copyLength = sizeof(short) * 2 + queryNode->getLength();
	int curPoint = sizeof(short) * 4 + copyLength * queryNode->getRecordNum();
	queryNode->addRecordNum(1);
	if(queryNode->isLeaf()){
		//�Ƚ��������short�����ƣ�Ȼ�����м�¼һ��һ�������ơ�
		memcpy(queryData + curPoint + copyLength, queryData + curPoint, 2 * sizeof(short));
		for(int i = result.queryNum; i < queryNode->getRecordNum() - 1; ++ i){
			curPoint -= copyLength;
			memcpy(queryData + curPoint  + copyLength, queryData + curPoint, copyLength);
		}
		if(curPoint < sizeof(short) * 4)
			curPoint = sizeof(short) * 4;
		//curPoint = sizeof(short) * 4 + copyLength * queryNode->getRecordNum();
		//�������ؼ��ֲ����
		*(short *)(queryData + curPoint) = valuePosition.blockNum;
		*(short *)(queryData + curPoint + sizeof(short)) = valuePosition.index;
		memcpy(queryData + curPoint + 2 * sizeof(short), keyValue, queryNode->getLength());
	}
	else{
		curPoint += 2 * sizeof(short);
		for(int i = result.queryNum; i < queryNode->getRecordNum() - 1; ++ i){
			curPoint -= copyLength;
			memcpy(queryData + curPoint + copyLength, queryData + curPoint, copyLength);
			
		}
		memcpy(queryData + curPoint, keyValue, queryNode->getLength());
		curPoint += queryNode->getLength();
		*(short *)(queryData + curPoint) = valuePosition.blockNum;
		*(short *)(queryData + curPoint + sizeof(short)) = -1;
	}

	//�ڵ��ڲ���֮������
	if(queryNode->isFull()){
		char *newInsertValue;
		blockNum ++;
		char *splitData = bm.buffer_read(file, blockNum);
		//�����ѳ����Ľڵ㴦��ͷ�ֽڣ�Ȼ�󿽱�����
		*(short *)(splitData) = -1;
		
		*(short *)(splitData + 3 * sizeof(short)) = queryNode->getType();

		//����Ҷ�ӽڵ㡣��¼�԰��
		if(queryNode->isLeaf()){
			*(short *)(splitData + 2 * sizeof(short)) = queryNode->getRecordNum() - queryNode->getRecordNum() / 2;
			curPoint = 4 * sizeof(short) + (2 * sizeof(short) + queryNode->getLength()) * (queryNode->getRecordNum() / 2);
			copyLength = (2 * sizeof(short) + queryNode->getLength()) * (queryNode->getRecordNum() - queryNode->getRecordNum()/2) + 2 * sizeof(short);
			memcpy(splitData + 4 * sizeof(short), queryNode->getData() + curPoint, copyLength);
			//newInsertValue = queryNode->getData() + curPoint + 2 * sizeof(short);
			newInsertValue = splitData + 6 * sizeof(short);
			queryNode->splitLeaf(blockNum);
		}
		//���ѷ�Ҷ�ӽڵ㣬�����һ�룬�������Ĺؼ������ϲ壬�ұߵ����ݷ��ѵ���һ���顣
		else{
			*(short *)(splitData + 2 * sizeof(short)) = queryNode->getRecordNum() - queryNode->getRecordNum() / 2 - 1;
			curPoint = 4 * sizeof(short) + (2 * sizeof(short) + queryNode->getLength()) * (queryNode->getRecordNum() / 2 + 1);
			copyLength = (2 * sizeof(short) + queryNode->getLength()) * (queryNode->getRecordNum() - queryNode ->getRecordNum()/2 - 1) + 2 * sizeof(short);
			memcpy(splitData + 4 * sizeof(short), queryNode->getData() + curPoint, copyLength);
			queryNode->splitParent();
			newInsertValue = queryNode->getData() + curPoint - queryNode->getLength();
			childChangeParent(splitData, blockNum);
		}
		//
		
		if(queryNode->getData() == root->getData()){
			*(short *)(splitData + sizeof(short)) = 0;
			//���ÿ����ݿ��������Ȼ�����´���root�ڵ�
			char *lastBlcok = bm.buffer_read(file, ++ blockNum);
			memcpy(lastBlcok, queryNode->getData(), PAGE_SIZE);
			*(short *)(lastBlcok + sizeof(short)) = 0;
			//����root�ڵ����ݣ���block 0 �ڵ����ݡ�
			char *rootData = root->getData();
			*(short *)(rootData) = -1;
			*(short *)(rootData + sizeof(short)) = -1;
			*(short *)(rootData + 2 * sizeof(short)) = 1;
			*(short *)(rootData + 4 * sizeof(short)) = blockNum;
			//��������Ҷ�ڵ�
			*(short *)(rootData + 5 * sizeof(short)) = -1;
			memcpy(rootData + sizeof(short) * 6, newInsertValue, root->getLength());
			*(short *)(rootData + sizeof(short) * 6 + root->getLength()) = blockNum - 1;
			queryNode->changeNode(lastBlcok);
			if(! queryNode->isLeaf())
				childChangeParent(lastBlcok, blockNum);
		}
		else{
			*(short *)(splitData + sizeof(short)) = queryNode->getParent();
			BlockAndIndex newBlock = {blockNum, -1, -1};
			if(queryNode->getParent() != 0)
				queryNode->changeNode(bm.buffer_read(file, queryNode->getParent()));
			else
				queryNode->changeNode(root->getData());
			insertInParent(newInsertValue, newBlock);
		}

	}


}

BlockAndIndex IndexManager::deleteKey(string tableName, string attrName, char * keyValue){
	short thisBlock = getLeafBlock(tableName, attrName, keyValue);
	BlockAndIndex result = {-1, -1, -1};
	if(thisBlock == -1)
		return result;
	result = deleteInParent(keyValue, thisBlock);
	if(result.blockNum == -1)
		cout << "cannot find the delete key" << endl;
	return result;
}

BlockAndIndex IndexManager::deleteInParent(char *keyValue, short thisBlock){
	BlockAndIndex result = queryNode->queryEqual(keyValue);
	if(result.blockNum == -1){
		//cout << "cannot find the delete key" << endl;
		return result;
	}

	queryNode->subRecordNum(1);	//�ڵ�ļ�¼��Ŀ��1��
	char *queryData = queryNode->getData();
	int curPoint = sizeof(short) * 4 + (sizeof(short) * 2 + queryNode->getLength()) * result.queryNum;
	int copyLength = 2 * sizeof(short) + queryNode->getLength();
	//��Ϊ֮ǰrecordNum-1�����������Ҫ+1
	if(queryNode->isLeaf()){
		for(int i = result.queryNum; i < queryNode->getRecordNum(); ++i){
			memcpy(queryData+curPoint, queryData + curPoint + copyLength, copyLength);
			curPoint += copyLength;
		}
		memcpy(queryData + curPoint, queryData + curPoint + copyLength, 2 * sizeof(short));
	}
	else{
		curPoint += 2 * sizeof(short);
		for(int i = result.queryNum; i < queryNode->getRecordNum(); ++i){
			memcpy(queryData+curPoint, queryData + curPoint + copyLength, copyLength);
			curPoint += copyLength;
		}
	}


	//���ڵ�ֻʣ��һ������
	if(queryNode->getData() == root->getData() && queryNode->getRecordNum() == 0){
		short childBlock = queryNode->getChildBlock(0);
		char *childData = bm.buffer_read(file, childBlock);
		memcpy(root->getData(), childData, PAGE_SIZE);
		childChangeParent(root->getData(), 0);
	}

	//��ֵɾ���󣬽ڵ��ֵ����С��һ�롣
	else if(queryNode->getData() != root->getData() && queryNode->isHalf()){
		//��Ҷ����ȡ�ϲ��ڵ�����Ҫ����һ���ڵ㡣���һ���ڵ���ǰһ���ڵ�ϲ�������ڵ㶼����һ���ڵ�ϲ���
		short curBlock, mergeBlock, parentBlock;
		parentBlock = queryNode->getParent();
		if(parentBlock == 0)
			queryNode->changeNode(root->getData());
		else
			queryNode->changeNode(bm.buffer_read(file, parentBlock));
		//�ڸ��ڵ��в��ұ�ɾ���Ľڵ�λ�á�
		BlockAndIndex tmp = queryNode->selectPosition(keyValue);
		char *parentValue = new char[queryNode->getLength()];
		memcpy(parentValue, queryNode->getData() + 6 * sizeof(short) + (2 * sizeof(short) + queryNode->getLength()) * tmp.queryNum, queryNode->getLength());
		//��ɾ����ֵ���ڽڵ������ϵ��ĵ�һ�����ӣ�����һ�����Ӻϲ�,���������ǰһ�����Ӻϲ���ֱ�Ӳ��������
		if(tmp.queryNum == 0){
			//curBlock = queryNode->getNextBlock();
			mergeBlock = thisBlock;
			curBlock = queryNode->getChildBlock(tmp.queryNum + 1);
		}
		else{
			mergeBlock = queryNode->getChildBlock(tmp.queryNum - 1);
			curBlock = thisBlock;
		}
		queryNode->changeNode(bm.buffer_read(file, mergeBlock));
		//�����ڵ���Ժϲ���
		//������Ҷ�ӽڵ�ϲ���ʱ���½ڵ�ļ�¼����ǰ����֮�͡�
		//��������Ҷ�ӽڵ�ϲ���ʱ���½ڵ�ļ�¼������֮��+1����ΪҪ�Ѹ��ڵ�ļ�ֵ�����
		int newCapacity;
		if(queryNode->isLeaf())
			newCapacity = queryNode->getTotalNum();
		else
			newCapacity = queryNode->getTotalNum() - 1;

		char *curData = bm.buffer_read(file, curBlock);
		char *merData = queryNode->getData();
		short curRecordNum = *(short *)(curData + 2 * sizeof(short));
		//���Էŵ�һ���ڵ���ȥ
		if(curRecordNum + queryNode->getRecordNum() < newCapacity){	
			copyLength = (2 * sizeof(short) + queryNode->getLength()) * curRecordNum + 2 * sizeof(short);
			//Ҫ�ϲ���������Ҷ�ڵ�
			if(queryNode->isLeaf()){
				int copyStart = 4 * sizeof(short) + (2 * sizeof(short) + queryNode->getLength()) * queryNode->getRecordNum();
				memcpy(merData + copyStart, curData + 4 * sizeof(short), copyLength);
				queryNode->addRecordNum(curRecordNum);
			}
			else{
				int copyStart = 6 * sizeof(short) + (2 * sizeof(short) + queryNode->getLength()) * queryNode->getRecordNum();
				memcpy(merData+copyStart, parentValue, queryNode->getLength());
				memcpy(merData + copyStart + queryNode->getLength(), curData + 4 * sizeof(short), copyLength);
				queryNode->addRecordNum(curRecordNum + 1);
			}
			parentBlock = queryNode->getParent();
			if(parentBlock == 0)
				queryNode->changeNode(root->getData());
			else
				queryNode->changeNode(bm.buffer_read(file, parentBlock));
			deleteInParent(parentValue, parentBlock);
			
		}
		/*һ���ڵ㲻���ŵ�ʱ��
		����Ҷ�ӽڵ㡣
		ֱ�ӽ�merBlock�����һ����ֵ����ָ����ΪcurBlock�еĵ�һ��
		�����ü�ֵ���ϻ���parentValue��
		������Ҷ�ӽڵ�
		��parentValue�Լ�merBlock���һ��ָ����ΪcurBlock�ĵ�һ��
		����merBlock�����һ����ֵ�滻�������е�parentValue
		queryNode��ʱָ��merBlock
		*/
		else{
			int copyStart = 4 * sizeof(short) + (2 * sizeof(short) + queryNode->getLength()) * (queryNode->getRecordNum() - 1);
			//������ֵ���ϻ����滻���ڵ�ļ�ֵ��
			char *newKey = queryNode->getData() + copyStart + 2 * sizeof(short);
			char *parentData = bm.buffer_read(file, queryNode->getParent());
			int copyDes = 6 * sizeof(short) + (2 * sizeof(short) + queryNode->getLength()) * (tmp.queryNum - 1);
			memcpy(parentData + copyDes, newKey, queryNode->getLength());
			queryNode->subRecordNum(1);
			copyLength = 2 * sizeof(short) + queryNode->getLength();
			char * lastKey = new char[copyLength];

			if(queryNode->isLeaf()){
				memcpy(lastKey, queryNode->getData() + copyStart, copyLength);
				//�����һ��ָ�룬��ָ����һ���ֵܵ�ָ����ǰ��������¼����-1
				memcpy(queryNode->getData() + copyStart, newKey + queryNode->getLength(), 2 * sizeof(short));
			}
			//��Ҷ�ӽڵ�
			else{
				memcpy(lastKey, queryNode->getData() + queryNode->getLength(), 2 * sizeof(short));
				memcpy(lastKey + 2 * sizeof(short), parentValue, copyLength);
				//�޸�merBlockĩβָ��ָ���ĸ��ף����curBlock��
				short nextTmp = *(short *)lastKey;
				char *childBlock = bm.buffer_read(file, nextTmp);
				*(short *)(childBlock + sizeof(short)) = curBlock;
			}

			queryNode->changeNode(bm.buffer_read(file, curBlock));
			//��curBlock����ָ�����ơ�
			copyLength = 2 * sizeof(short) + queryNode->getLength();
			curPoint = 4 * sizeof(short) + copyLength * queryNode->getRecordNum();
			queryData = queryNode->getData();
			memcpy(queryData + curPoint, queryData + curPoint + copyLength, 2 * sizeof(short));
			curPoint -= copyLength;
			for(int i = 0; i < queryNode->getRecordNum(); ++i){
				memcpy(queryData + curPoint, queryData + curPoint + copyLength, copyLength);
				curPoint -= copyLength;
			}
			memcpy(queryData + curPoint, lastKey, copyLength);

			delete[] lastKey;
		}

		delete[] parentValue;
	}

	return result;

}

BlockAndIndex IndexManager::queryKey(string tableName, string attrName, char *queryValue){
	//��ѯ��queryNode��ΪqueryValue���ڵ�block���ݽڵ㡣
	if(getLeafBlock(tableName, attrName, queryValue) == -1){
		BlockAndIndex tmp = {-1, -1, -1};
		return tmp;
	}

	BlockAndIndex result = queryNode->queryEqual(queryValue);
	return result;
}

short IndexManager::getLeafBlock(const string tableName, const string attrName, char *queryValue){
	string indexName = CatalogManager::shared()->search_index(tableName, attrName);
	if(indexName == "\0"){
		return -1;
	}
	string fileName = indexName + ".index";
	if(file != fileName)
		changeRoot(fileName);
	//queryNode = root;
	queryNode->changeNode(root->getData());
	short nextQueryBlock = 0;

	while(! queryNode->isLeaf()){
		nextQueryBlock = queryNode->selectPosition(queryValue).blockNum;
		queryNode->changeNode(bm.buffer_read(file, nextQueryBlock));
	}

	return nextQueryBlock;
}

void IndexManager::createIndex(const Table &tableInfo, const string& attrName, const string& indexName){
	string fileName;
	int type, length = 0;
	int skipByteNum = 1;
	for(vector<Attribute>::const_iterator iter = tableInfo.attrs.begin(); iter != tableInfo.attrs.end(); ++iter){
		type = iter->type;
		if(type == INT || type == FLOAT)
			length = 4;
		else
			length = iter->length;
		if(iter->name == attrName){
			fileName = "dbfile/" + indexName + ".index";
			file = indexName + ".index";
			blockNum = 0;
			break;
		}
		skipByteNum += length;
	}

	if(length == 0)
		cout << "error index name, not exist!" << endl;

	fstream indexFile(fileName.c_str(), ios::out);
	//indexFile.close();

	//changeRoot(fileName);
	char *rootData = root->getData();
	*(short *)(rootData) = -1;
	*(short *)(rootData + sizeof(short)) = -1;
	*(short *)(rootData + 2 * sizeof(short)) = 0;
	if(type == INT)
		*(short *)(rootData + 3 * sizeof(short)) = -1;
	else if(type == FLOAT)
		*(short *)(rootData + 3 * sizeof(short)) = -2;
	else
		*(short *)(rootData + 3 * sizeof(short)) = length;
	*(short *)(rootData + 5 * sizeof(short)) = 0;
	*(short *)(rootData + 4 * sizeof(short)) = -1;
	root->memberInit();
	indexFile.write(root->getData(), PAGE_SIZE);
	indexFile.close();

	string recordFileName = tableInfo.name + ".record";
	fstream recordFile(recordFileName.c_str(), ios::in);
	int recordLength = tableInfo.length + 1;
	int recordCount = PAGE_SIZE / recordLength;
	
	int offset = 0;
	char *recordKey = NULL;
	BlockAndIndex recordPosition = {-1, -1, -1};
	for(int i = 0; i < tableInfo.block_cnt; ++ i){
		recordKey = bm.buffer_read(recordFileName, i);
		for(int j = 0; j < recordCount; ++ j){
			offset = recordLength * j;
			if(recordKey[offset] != NIL){
				recordPosition.blockNum = i;
				recordPosition.index = j;
				insertKey(tableInfo.name, attrName, recordKey + offset + skipByteNum, recordPosition);
			}
		}
	}

}

