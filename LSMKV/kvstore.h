#pragma once
#include<algorithm>
#include<cmath>
#include "kvstore_api.h"
#include<iostream>
#include"MurmurHash3.h"
#include<bitset>
#include<vector>
#define INT_MAX (uint64_t)18446744073709551615
#define INT_MIN 0
class node{
public:
    uint64_t key;
    int height;
	std::string value;
    node * right;
    node * down;
    node(uint64_t x,std::string s){
        key=x;
		value=s;
        height=1;
        right=NULL;
        down=NULL;
    }
};
class MemTable{
public:
    node *head = NULL,*tail=NULL;
    double prob;
    int length;
    int maxheight;
    void buildList(double p,int n){
        srand(time(NULL));
        prob=p;
        length =n;
        maxheight = -log(n)/log(p);
        if(maxheight<1)maxheight=1;
        head=new node(INT_MIN,"");
        node * tmp=head;

        for(int i=maxheight;i>0;i--){
            tmp->height=i;
            tmp->key=INT_MIN;
            tmp->down=new node(INT_MIN,"");
            tmp=tmp->down;

        }

        tmp=head;
        tail=new node(INT_MAX,"");
        node *tmps=tail;
        for(int i=maxheight;i>0;i--){
            tmps->height=i;
            tmps->key=INT_MAX;
            tmps->down=new node(INT_MAX,"");
            tmps=tmps->down;
        }
        tmps=tail;
        for(int i=maxheight;i>0;i--){
            tmp->right=tmps;
            tmps=tmps->down;
            tmp=tmp->down;

        }tmp->right=tmps;
    }
    void clear(){
        
        node *tmp=head;
        node *p=tmp;
        node *w=tmp;
       
        while (tmp->down!=nullptr)
        {
           w=tmp->down;
       
        
       while (tmp->right!=nullptr)
       {
        p=tmp->right;
        tmp->right=nullptr;
        tmp->down=nullptr;
        delete tmp;
        tmp=nullptr;
        tmp=p;
        
       }
       
       tmp->right=nullptr;
        tmp->down=nullptr;
       delete tmp;
       tmp=nullptr;
        tmp=w;
    
       }
      while (tmp->right!=nullptr)
       {
        p=tmp->right;
        tmp->right=nullptr;
        tmp->down=nullptr;
        delete tmp;
        tmp=nullptr;
        tmp=p;
        
       }
       
       tmp->right=nullptr;
        tmp->down=nullptr;
       delete tmp;
       tmp=nullptr;
       head=nullptr;tail=nullptr;
    }
    

    void insert(int x,std::string v){
        std::string s=search(x);
	
	if (s==""){
        node *update[maxheight+1];
        node *p=head;
        while(p->height!=1) {
            while (p->right->key < x)p = p->right;
            update[p->height]=p;
            p=p->down;
        }
        while (p->right->key < x)p = p->right;
        node *q=new node(x,v);
        q->right=p->right;
        p->right=q;
        int i;
        for( i=1;i<=maxheight;i++){
            int p=rand()%1000;
            bool ok=0;
            if(p<prob*1000)ok=1;
            else ok=0;
            if(!ok)break;
        }
        if(i>maxheight)i=maxheight;
        int layer=i;//cout<<layer;
        node *tmp[layer+1];
        tmp[1]=q;
        for(int i=2;i<=layer;i++){
            tmp[i]=new node(x,v);
            tmp[i]->height=i;
            tmp[i]->down=tmp[i-1];
            tmp[i]->key=tmp[i-1]->key;
            tmp[i]->right=update[i]->right;
            update[i]->right=tmp[i];
        }}
        else{
            node *p=head;
        bool found =0;
        std::string emptystr("");
        while(p->height>1) {
            while (p->right->key < x){p = p->right;}
            if(p->right->key==x){
             
                p=p->right;found=1;break;
            }
            p=p->down;
        }
        if(found){
            while(p->height>1){
                p->value=v;
                p=p->down;
            } p->value=v;
        }
        else {
            while (p->right->key < x){p = p->right;}
            if(p->right->key==x){
                p=p->right;found=1;
                    
            }
            if(found){
            while(p->height>1){
                p->value=v;
                p=p->down;
            } p->value=v;
        }
            
        
    }

        }

    }
    std::string search(uint64_t x){
        node *p=head;
        bool found =0;
        std::string emptystr("");
        while(p->height>1) {
            while (p->right->key < x){p = p->right;}
            if(p->right->key==x){
             
                p=p->right;found=1;break;
            }
            p=p->down;
        }
        if(found)return p->value;
        else {
            while (p->right->key < x){p = p->right;}
            if(p->right->key==x){
                p=p->right;found=1;
                    
            }
            if(found)return p->value;
            else return emptystr;
        }
    }
};
class Header{
public:Header(uint64_t ti,uint64_t n,uint64_t min,uint64_t max): timeTable(ti), num(n), keyMin(min), keyMax(max)
    {
    };   Header(){};
        uint64_t get_tT(){return timeTable;}
        uint64_t get_n(){return num;}
        uint64_t get_min(){return keyMin;}
        uint64_t get_max(){return keyMax;}
       
private:
uint64_t timeTable;
uint64_t num;
uint64_t keyMin;
uint64_t keyMax;
};

class BloomFilter{
private:
std::bitset<10240*8> bits;
public:
void buildBloomFilter(MemTable memtable);
//~BloomFilter();
bool exist(uint64_t key);
void addkey(uint64_t key);
void clear(){bits.reset();}
};
class Index{
public:
    Index(){keys_.clear();offsets_.clear();};

    void add_entry(uint64_t key, uint32_t offset);
    uint32_t get_offset(uint64_t key,uint32_t) const;


    std::vector<uint64_t> keys_;
    std::vector<uint32_t> offsets_;
};
class Cache{
private:
    Header header;
    BloomFilter bloomfilter;
    Index index;
public:
    std::string fileName;
    bool is_cached;
    bool Bl_exist(uint64_t x){return bloomfilter.exist(x);};
    Cache(Header header,BloomFilter bloomfilter,Index index,std::string& fileName): header(header),bloomfilter(bloomfilter),index(index),fileName(fileName){is_cached=0;};
    Cache() : is_cached(false) {};
    Header getHeader(){
        return header;
    }
    Index getIndex(){
        return index;
    }
    BloomFilter getBlf(){
        return bloomfilter;
    }
    void getCacheFromFile(std::string fileName);
};
class SSTable{
    
private:   
    Header header;
    BloomFilter bloomfilter;
    Index index;
    std::string data_;
public:
    SSTable( Header& header,  BloomFilter& bloomfilter,
             Index& index,  std::string& data)
        : header(header), bloomfilter(bloomfilter),
          index(index), data_(data)
    {
    };
    SSTable(){};
    void toFile(std::string dir,int level,int filenum);
    void clear() {
    header = Header();
    bloomfilter = BloomFilter();
    index = Index();
    data_.clear();
}   SSTable readfromFile(std::string fileName);
    Header getHeader(){
        return header;
    }
    Index getIndex(){
        return index;
    }
    std::string getData(){
        return data_;
    }
    BloomFilter getBlf(){
        return bloomfilter;
    }
    friend class KVStore;
    SSTable mergeSSTables(SSTable& sstable1);
};
class KVStore : public KVStoreAPI {
	// You can add your implementation here
private:
    int file_num;
	MemTable memtable;
    std::string directory;
    int currentSizeOfMemtable;
    uint64_t currTimeTable=0;
    SSTable sstable;
    std::vector<Cache> cacheList;
    int maxLevel;
    std::vector<SSTable> split(SSTable &merged,uint64_t maxtT);
public:
	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;
    void buildSStable();
    void addCache(std::string dir,int level);
    void compaction(int level);
    void getAllfile();
    void getCachefromdir( std::string &dir,int level);
};
