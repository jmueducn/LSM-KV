#include "kvstore.h"
#include <string>
#include<iostream>
#include <fstream>
#include <sstream>
#include"utils.h"
void clearDirectory(const std::string& directoryPath) {
    std::vector<std::string> entries;
	
    if (utils::scanDir(directoryPath, entries) > 0) {
        for (const std::string& entry : entries) {
            std::string fullPath = directoryPath + "/" + entry;
            if (entry.find(".sst") == std::string::npos) {
                clearDirectory(fullPath);  // 递归清除子目录
                utils::rmdir(fullPath.c_str());  // 删除空目录
            } else {
                utils::rmfile(fullPath.c_str());  // 删除文件
            }
        }
    }
}
void KVStore::getCachefromdir(std::string& dir, int level) {
    std::vector<std::string> entries;
    if (utils::scanDir(dir, entries) > 0) {
        for (const std::string& entry : entries) {
            std::string fullPath = dir + "/" + entry;
            if (entry.find(".sst") != std::string::npos) {
                Cache caches;
                caches.getCacheFromFile(fullPath);  
                if(currTimeTable<caches.getHeader().get_tT()>currTimeTable)currTimeTable=caches.getHeader().get_tT();
                cacheList.push_back(caches);
				//std::cout<<caches.fileName<<std::endl;
            }  if (level > maxLevel) {
                maxLevel = level; // 更新maxLevel
            }
			
        }
    }
}

void KVStore::getAllfile() {
    std::vector<std::string> entries;
    if (utils::scanDir(directory, entries) > 0) {
        for (const std::string& entry : entries) {
            std::string fullPath = directory + "/" + entry;
            if (entry.find(".sst") == std::string::npos) {
                int level = std::stoi(entry.substr(6, 1)); // 解析出目录层级
                getCachefromdir(fullPath, level);
            }
        }
    } else {
        maxLevel = 0;
    }
}

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
	memtable.buildList(0.5,32768);
	directory=dir;
	getAllfile();
	currTimeTable++;
	file_num=currTimeTable;
}

KVStore::~KVStore()
{	
	reset(); 
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{	
	currentSizeOfMemtable+=(s.size()+sizeof(uint32_t)+sizeof(uint64_t)+32);
	if(currentSizeOfMemtable>(2*1000*1000)){
		bool exist;
		std::string dir=directory+"/level-0";
		exist=utils::dirExists(dir);
		char newdir[100];
		strcpy(newdir,dir.c_str());
		if(!exist)utils::_mkdir(newdir);
		buildSStable();
		addCache(dir,0);
	
		sstable.toFile(dir,0,file_num);
		file_num++;
		memtable.clear();
		
		memtable.buildList(0.5,32768);
		currTimeTable++;
		currentSizeOfMemtable=10272;
		/*std::cout<<"before:"<<std::endl;
		for(auto& cache:cacheList){
			std::cout<<cache.fileName<<std::endl;
			std::cout<<cache.getHeader().get_min()<<" "<<cache.getHeader().get_max()<<std::endl;
		}
		std::cout<<std::endl;*/
		compaction(0);
		/*std::cout<<"after:"<<std::endl;
		for(auto& cache:cacheList){
			std::cout<<cache.fileName<<std::endl;
			std::cout<<cache.getHeader().get_min()<<" "<<cache.getHeader().get_max()<<std::endl;
		}
		std::cout<<std::endl;*/
	}
	memtable.insert(key,s);
	
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	std::string s="";

	s=memtable.search(key);
	
	if(s=="~DELETED~")return("");
	
	else if(s==""&&currTimeTable>1){
		int level=0;
		
		while(level<=maxLevel){
			
		uint64_t maxtT=0;
		std::string stt="level-"+std::to_string(level);
		for ( auto& cache : cacheList) {
		
			
		if(cache.fileName.find(stt) != std::string::npos){
			
        if(cache.getHeader().get_max()>=key&&cache.getHeader().get_min()<=key&&cache.getBlf().exist(key)){
			uint64_t tT=cache.getHeader().get_tT();
		uint32_t mid=cache.getIndex().get_offset(key,cache.getHeader().get_n());
		if(mid==-1){continue;}
		int length;

		uint32_t start=cache.getIndex().offsets_[mid];
		if(mid>0)length=cache.getIndex().offsets_[mid]-cache.getIndex().offsets_[mid-1];
		else length=start;
		start-=length;
		std::ifstream in(cache.fileName);
		in.seekg(10272 + (cache.getHeader().get_n() ) * 12 + start, std::ifstream::beg);
		std::string str;
    	str.resize(length);
    	in.read(&str[0], length);
		//if(key==7687)std::cout<<str<<cache.fileName<<std::endl;
		if(tT>=maxtT){
		
		s=str;maxtT=tT;
		}
		
		}}}
		
		if(s=="~DELETED~")return("");
		if(s!="")return s;
		level++;
    }
		
		
		
		
		
		
		
		
	}if(s=="~DELETED~")return("");//std::cout<<s.size()<<std::endl;
	return s;
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{	
	std::string s=get(key);
	
	if (s=="")return false;
	else {
		std::string deleteString="~DELETED~";
		put(key,deleteString);
		return true;
	}
	return false;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
	maxLevel=0;
	memtable.clear();
	memtable.buildList(0.5,32768);
	currentSizeOfMemtable=10272;
	currTimeTable=1;
	cacheList.clear();
	clearDirectory(directory);
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{	
}
void BloomFilter::buildBloomFilter(MemTable memtable){
	node *tmp=memtable.head;
	while(tmp->height>1)tmp=tmp->down;
	uint32_t hash[4]={0};
	tmp=tmp->right;
	while(tmp->right!=nullptr){
		MurmurHash3_x64_128(&tmp->key,sizeof(tmp->key),1,hash);
		bits[hash[0]%(10240*8)]=1;
		bits[hash[1]%(10240*8)]=1;
		bits[hash[2]%(10240*8)]=1;
		bits[hash[3]%(10240*8)]=1;
		tmp=tmp->right;
	}


}
void BloomFilter::addkey(uint64_t key){
	uint32_t hash[4]={0};
	MurmurHash3_x64_128(&key,sizeof(key),1,hash);
		bits[hash[0]%(10240*8)]=1;
		bits[hash[1]%(10240*8)]=1;
		bits[hash[2]%(10240*8)]=1;
		bits[hash[3]%(10240*8)]=1;
}
bool BloomFilter::exist(u_int64_t key){
	uint32_t hash[4]={0};
	MurmurHash3_x64_128(&key,sizeof(key),1,hash);
	return bits[hash[0]%(10240*8)]==1&&
		bits[hash[1]%(10240*8)]==1&&
		bits[hash[2]%(10240*8)]==1&&
		bits[hash[3]%(10240*8)]==1;
}
uint32_t Index::get_offset (uint64_t key ,uint32_t n) const{
	uint32_t left = 0;
    uint32_t right = n - 1;
    while (left <= right) {
        uint32_t mid = (left + right) / 2;
        uint64_t mid_key = keys_[mid];

        if (mid_key < key) {
            left = mid + 1;
        } else if (mid_key > key) {
            right = mid - 1;
        } else {
            // 找到了对应的键，返回对应的偏移量
            return mid;
        }
    }

    return -1;
}
void KVStore::buildSStable(){
	sstable.data_="";
	node *tmp=memtable.head;
	while(tmp->height>1)tmp=tmp->down;
	tmp=tmp->right;
	uint64_t min=tmp->key;
	sstable.data_+=tmp->value;
	Index index;
	index.add_entry(tmp->key,tmp->value.size());
	uint64_t cnt=0;
	uint32_t ss=tmp->value.size();
	while(tmp->right->right!=nullptr){
		cnt++;
		tmp=tmp->right;
		ss+=tmp->value.size();
		index.add_entry(tmp->key,ss);
		sstable.data_.append(tmp->value);
	}cnt++;
	uint64_t max=tmp->key;
	Header header(currTimeTable,cnt,min,max);
	BloomFilter blf;
	blf.buildBloomFilter(memtable);
	sstable.bloomfilter=blf;
	sstable.header=header;
	sstable.index=index;
}
void Index::add_entry(uint64_t key,uint32_t offset){
	keys_.push_back(key);
	offsets_.push_back(offset);
}
void SSTable::toFile(std::string directory,int level,int file_num) {
	
    // 打开文件并进行写入操作

	std::vector<std::string> all_files;
	int num = utils::scanDir(directory,all_files);
	std::string sst=".sst";
	std::string begg="/"+std::to_string(level)+"_";
	std::string fileName=directory+begg+std::to_string(file_num)+sst;
    std::ofstream file(fileName, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Error: Failed to open file!" << std::endl;
        return;
    }
    // 写入 Header 数据
    file.write(reinterpret_cast<const char*>(&header), sizeof(header));

    // 写入 BloomFilter 数据
    file.write(reinterpret_cast<const char*>(&bloomfilter), sizeof(bloomfilter));

    // 写入 Index 数据
    
    for (int i = 0; i < header.get_n(); ++i) {
        file.write(reinterpret_cast<const char*>(&index.keys_[i]), sizeof(index.keys_[i]));
        file.write(reinterpret_cast<const char*>(&index.offsets_[i]), sizeof(index.offsets_[i]));
    }

    // 写入 Data 数据
    file.write(data_.c_str(), data_.size());
	
    // 关闭文件
    file.close();
	clear();
}
SSTable SSTable::readfromFile(std::string fileName) {
    // 打开文件并进行读取操作
    std::ifstream file(fileName, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Error: Failed to open file!" << std::endl;
        return SSTable();
    }

    // 读取 Header 数据
    Header header;
    file.read(reinterpret_cast<char*>(&header), sizeof(header));

    // 读取 BloomFilter 数据
    BloomFilter bloomfilter;
    file.read(reinterpret_cast<char*>(&bloomfilter), sizeof(bloomfilter));

    // 读取 Index 数据
    Index index;
    for (int i = 0; i < header.get_n(); ++i) {
        uint64_t key;
        uint32_t offset;
        file.read(reinterpret_cast<char*>(&key), sizeof(key));
        file.read(reinterpret_cast<char*>(&offset), sizeof(offset));
        index.add_entry(key, offset);
    }

    // 读取 Data 数据
    std::string data((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());

    // 关闭文件
    file.close();

    // 返回读取的 SSTable 对象
    return SSTable(header, bloomfilter, index, data);
}
void KVStore::addCache(std::string directory,int level){
	std::vector<std::string> all_files;
	int num = utils::scanDir(directory,all_files);
	std::string sst=".sst";
	std::string begg="/"+std::to_string(level)+"_";
	std::string fileName=directory+begg+std::to_string(file_num)+sst;
	Cache newchace(sstable.getHeader(),sstable.getBlf(),sstable.getIndex(),fileName);
	cacheList.push_back(newchace);
}
int pow2(int exponent) {
    int result = 1;
    int absExponent = std::abs(exponent);

    for (int i = 0; i < absExponent; ++i) {
        result *= 2;
    }

    if (exponent < 0) {
        result = 1 / result;
    }

    return result;
}
std::vector<SSTable> KVStore::split(SSTable &merged,uint64_t maxtT){
	std::vector<SSTable> sstables;
	sstables.clear();
	Index index=merged.index;
	std::string all_data=merged.data_;
	Index nindex;
	int currSize=10272;
	int i=0;
	uint64_t tT,min,max,n;
	n=0;max=0;min=18446744073709551615;tT=maxtT;
	uint32_t newoffset=0;
	std::string data;
	BloomFilter blf;
	while(i<index.keys_.size()){
		
		uint64_t key=index.keys_[i];
		 uint32_t offset = index.offsets_[i];
			uint32_t length=0;
			if(i>0)length=index.offsets_[i]-index.offsets_[i-1];
		else length=offset;
			offset-=length;
			newoffset+=length;
			std::string s=all_data.substr(offset,length);
		currSize+=(s.size()+sizeof(uint32_t)+sizeof(uint64_t)+32);
		if(currSize>2*1000*1000){
			Header header(tT,n,min,max);
			SSTable a(header,blf,nindex,data);
			sstables.push_back(a);
			currSize=10272;
			n=0;max=0;min=18446744073709551615;newoffset=length;data.clear();nindex.keys_.clear();nindex.offsets_.clear();blf.clear();
		}
		nindex.add_entry(key,newoffset);
		data+=s;
		blf.addkey(key);
		if(key<=min)min=key;
		if(key>=max)max=key;
		n++;
		i++;
	}	
		Header header(tT,n,min,max);
		SSTable a(header,blf,nindex,data);
		sstables.push_back(a);
		
	return sstables;
}
void KVStore::compaction(int level){
	std::string dir=directory+"/level-"+std::to_string(level);
	std::vector<std::string> all_files;
	std::vector<std::string> picked_files;
	std::vector<Cache>all_files_cache;
	int num = utils::scanDir(dir,all_files);
	int max_tt=0;
	if(num<=pow2(level+1))return;
	if(level==0){picked_files=all_files;
	for(auto& file:all_files){
		file=dir+"/"+file;
		for (auto it = cacheList.begin(); it != cacheList.end(); ) {
    if (it->fileName == file) {
        all_files_cache.push_back(*it);++it; // 删除元素并更新迭代器
    } else {
        ++it;  // 继续遍历下一个元素
    }
}
}std::partial_sort_copy(all_files_cache.begin(), all_files_cache.end(),
                               all_files_cache.begin(), all_files_cache.begin() + num,
                               []( Cache& cache1,  Cache& cache2) {
                                   if(cache1.getHeader().get_tT() != cache2.getHeader().get_tT()) return cache1.getHeader().get_tT() < cache2.getHeader().get_tT();
								   else return cache1.getHeader().get_min()<cache2.getHeader().get_min();
                               });all_files_cache.resize(num);}
	else {
		
		int pick_num=num-pow2(level+1);
		for(auto& file:all_files){
			file=dir+"/"+file;
		for (auto it = cacheList.begin(); it != cacheList.end(); ) {
    if (it->fileName == file) {
        all_files_cache.push_back(*it);
        it ++; // 删除元素并更新迭代器
    } else {
        ++it;  // 继续遍历下一个元素
    }
}
}
		 std::partial_sort_copy(all_files_cache.begin(), all_files_cache.end(),
                               all_files_cache.begin(), all_files_cache.begin() + pick_num,
                               []( Cache& cache1,  Cache& cache2) {
                                   if(cache1.getHeader().get_tT() != cache2.getHeader().get_tT()) return cache1.getHeader().get_tT() < cache2.getHeader().get_tT();
								   else return cache1.getHeader().get_min()<cache2.getHeader().get_min();
                               });

        // Resize all_files_cache to keep only the first "pick_num" Cache objects
        all_files_cache.resize(pick_num);
	
	}
	uint64_t all_Min=18446744073709551615,all_Max=0;
	for(auto& cache:all_files_cache){
		uint64_t min=cache.getHeader().get_min(),max=cache.getHeader().get_max();
		if(min<all_Min)all_Min=min;
		if(max>all_Max)all_Max=max;
	}
	int next_level=level+1;
	std::string dir_next=directory+"/level-"+std::to_string(next_level);
	std::vector<Cache>next_cache;
	if(next_level<=maxLevel){
	
	std::vector<std::string> next_files;
	int num_next=utils::scanDir(dir_next,next_files);
		for(auto& file:next_files){
			file=dir_next+"/"+file;
		for ( auto& cache : cacheList) {
			if(cache.fileName==file&&((cache.getHeader().get_min()<=all_Max&&cache.getHeader().get_max()>=all_Min)))next_cache.push_back(cache);
			
		}}
	for(auto& cache:next_cache){
		uint64_t tT=cache.getHeader().get_tT();
		if(tT>max_tt)max_tt=tT;
	}}	
	for(auto& cache:all_files_cache){
		uint64_t tT=cache.getHeader().get_tT();
		if(tT>max_tt)max_tt=tT;
	}
	
	
	if(next_level>maxLevel){maxLevel=next_level;bool exist;
		std::string dir=directory+"/level-"+std::to_string(next_level);
		exist=utils::dirExists(dir);
		char newdir[100];
		strcpy(newdir,dir.c_str());
		if(!exist)utils::_mkdir(newdir);}
		for(auto& cache:all_files_cache){
			for (auto it = cacheList.begin(); it != cacheList.end(); ) {
    if (it->fileName == cache.fileName) {
        it=cacheList.erase(it); // 删除元素并更新迭代器
    } else {
        ++it;  // 继续遍历下一个元素
    }
}
		}
		for(auto& cache:next_cache){
			for (auto it = cacheList.begin(); it != cacheList.end(); ) {
    if (it->fileName == cache.fileName) {
        it=cacheList.erase(it); // 删除元素并更新迭代器
    } else {
        ++it;  // 继续遍历下一个元素
    }
}
		}
	std::vector<SSTable>sstables;
	std::vector<SSTable>nsstables;
	SSTable newsstable;
		for ( auto& cache : all_files_cache){
			newsstable=newsstable.readfromFile(cache.fileName);
			sstables.push_back(newsstable);
			newsstable.clear();
			utils::rmfile(cache.fileName.c_str());
		}
		for ( auto& cache : next_cache){
			newsstable=newsstable.readfromFile(cache.fileName);
			sstables.push_back(newsstable);
			newsstable.clear();
			utils::rmfile(cache.fileName.c_str());
		}
	SSTable merged_sstable;
	merged_sstable.clear();
	Header header(0,0,0,0);
	merged_sstable.header=header;
		for(auto& sstable:sstables){
			
			merged_sstable=merged_sstable.mergeSSTables(sstable);
			
		}

		for(auto& sstable:nsstables){
			
			merged_sstable=merged_sstable.mergeSSTables(sstable);
			
		}
	sstables.clear();
	sstables=split(merged_sstable,max_tt);
	for(auto& thissstable:sstables){
		sstable.clear();
		sstable=thissstable;
		addCache(dir_next,next_level);
		thissstable.toFile(dir_next,next_level,file_num);
		file_num++;
	}	compaction(next_level);
	}
SSTable SSTable::mergeSSTables( SSTable& sstable1) {
     Index index1 = sstable1.getIndex();
     Index index2 = index;
     std::string data1 = sstable1.getData();
     std::string data2 = data_;
	 uint64_t tT=sstable1.getHeader().get_tT();
	 bool a;
	 if(tT>header.get_tT())a=1;
	 else {a=0;}
	
    // 创建新的SSTable对象用于存储归并排序后的结果
   SSTable merge;
    if(tT>header.get_tT()){Header headers(tT,0,0,0);merge.header=headers;}
   else merge.header=header;
   
	uint32_t newoffset=0;
    // 初始化索引指针
    int i = 0;
    int j = 0;
    // 归并排序
    while (i < index1.keys_.size() && j < index2.keys_.size()) {
        uint64_t key1 = index1.keys_[i];
        uint64_t key2 = index2.keys_[j];
		
        if (key1 < key2) {
            uint32_t offset = index1.offsets_[i];
			
			uint32_t length=0;
			if(i>0)length=index1.offsets_[i]-index1.offsets_[i-1];
		else length=offset;
			offset-=length;
			newoffset+=length;
            merge.index.add_entry(key1, newoffset);
            merge.data_ += data1.substr(offset,length);
            ++i;
        } else if(key1>key2){
            uint32_t offset = index2.offsets_[j];
				uint32_t length=0;
			if(j>0)length=index2.offsets_[j]-index2.offsets_[j-1];
		else length=offset;

			offset-=length;
			newoffset+=length;
            merge.index.add_entry(key2, newoffset);
            merge.data_ += data2.substr(offset,length);
            ++j;
        }
			else if(a){
				
				uint32_t offset = index1.offsets_[i];
			
			uint32_t length=0;
			if(i>0)length=index1.offsets_[i]-index1.offsets_[i-1];
		else length=offset;
			offset-=length;
			newoffset+=length;
            merge.index.add_entry(key1, newoffset);
			//std::cout<<data1.substr(offset,length)<<std::endl;
            merge.data_ += data1.substr(offset,length);
			++i;++j;
			}
			else{//std::cout<<"???";
			
				uint32_t offset = index2.offsets_[j];
				uint32_t length=0;
			if(j>0)length=index2.offsets_[j]-index2.offsets_[j-1];
		else length=offset;

			offset-=length;
			newoffset+=length;
            merge.index.add_entry(key2, newoffset);
			
			
            merge.data_ += data2.substr(offset,length);
			++i;++j;
			}
		
    }

    // 将剩余未遍历完的键值对复制到新的SSTable对象中
    for (; i < index1.keys_.size(); ++i) {
        uint64_t key = index1.keys_[i];
        uint32_t offset = index1.offsets_[i];
		uint32_t length=0;
			if(i>0)length=index1.offsets_[i]-index1.offsets_[i-1];
		else length=offset;

			offset-=length;
			newoffset+=length;
        merge.index.add_entry(key, newoffset);
        merge.data_ += data1.substr(offset,length);
    }

    for (; j < index2.keys_.size(); ++j) {
        uint64_t key = index2.keys_[j];
        uint32_t offset = index2.offsets_[j];
		uint32_t length=0;
			if(j>0)length=index2.offsets_[j]-index2.offsets_[j-1];
		else length=offset;

			offset-=length;
			newoffset+=length;
        merge.index.add_entry(key, newoffset);
        merge.data_ += data2.substr(offset,length);
    }
	
    return merge;
}
void Cache::getCacheFromFile(std::string files){
	fileName=files;
	std::ifstream file(fileName, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Error: Failed to open file!" << std::endl;
        return ;
    }

    // 读取 Header 数据
    file.read(reinterpret_cast<char*>(&header), sizeof(header));

    // 读取 BloomFilter 数据
    file.read(reinterpret_cast<char*>(&bloomfilter), sizeof(bloomfilter));

    // 读取 Index 数据
    for (int i = 0; i < header.get_n(); ++i) {
        uint64_t key;
        uint32_t offset;
        file.read(reinterpret_cast<char*>(&key), sizeof(key));
        file.read(reinterpret_cast<char*>(&offset), sizeof(offset));
        index.add_entry(key, offset);
    }

	
}
