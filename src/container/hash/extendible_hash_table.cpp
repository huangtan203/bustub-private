//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"


namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  auto dir_page=reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&directory_page_id_,nullptr)->GetData());
  dir_page->SetPageId(directory_page_id_);
  page_id_t page_id=INVALID_PAGE_ID;
  buffer_pool_manager_->NewPage(&page_id);
  dir_page->SetBucketPageId(0,page_id);
  buffer_pool_manager_->UnpinPage(directory_page_id_,true);
  buffer_pool_manager_->UnpinPage(page_id,true);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t hash_=this->Hash(key);
  uint32_t global_mask=dir_page->GetGlobalDepthMask();
  return hash_&global_mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
page_id_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t bucket_index=this->KeyToDirectoryIndex(key,dir_page);
  return dir_page->GetBucketPageId(bucket_index);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  // 
  Page*page_=buffer_pool_manager_->FetchPage(directory_page_id_);
  auto dir_page=reinterpret_cast<HashTableDirectoryPage *>(page_->GetData());
  return dir_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  Page*page=buffer_pool_manager_->FetchPage(bucket_page_id);
  auto bucket_page=reinterpret_cast<HashTableBucketPage<KeyType,ValueType,KeyComparator> *>(page->GetData());
  return bucket_page;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  auto dir_page=this->FetchDirectoryPage();
  page_id_t bucket_page_id=this->KeyToPageId(key,dir_page);
  auto bucket_page=this->FetchBucketPage(bucket_page_id);
  bool flag=bucket_page->GetValue(key,comparator_, result);
  buffer_pool_manager_->UnpinPage(bucket_page_id,false);
  buffer_pool_manager_->UnpinPage(directory_page_id_,false);
  table_latch_.RUnlock();
  return flag;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  //加锁
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page=this->FetchDirectoryPage();
  page_id_t page_id=KeyToPageId(key,dir_page);
  HASH_TABLE_BUCKET_TYPE * bucket_page=this->FetchBucketPage(page_id);
  bool is_dirty=bucket_page->Insert(key,value,comparator_);
  table_latch_.WUnlock();
  //这里几个脏位需要注意,直接设为true理论上没有错误
  if(is_dirty){
    //解锁
    buffer_pool_manager_->UnpinPage(page_id,true);
    buffer_pool_manager_->UnpinPage(directory_page_id_,false);
    return true;
  }
  if(bucket_page->IsExist(key,value,comparator_)){
    //解锁
    buffer_pool_manager_->UnpinPage(page_id,false);
    buffer_pool_manager_->UnpinPage(directory_page_id_,false);
    return false;
  }
  is_dirty=SplitInsert(transaction,key,value);
  buffer_pool_manager_->UnpinPage(page_id,is_dirty);
  buffer_pool_manager_->UnpinPage(directory_page_id_,false);
  return is_dirty;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page=this->FetchDirectoryPage();
  page_id_t page_id=KeyToPageId(key,dir_page);
  HASH_TABLE_BUCKET_TYPE * bucket_page=this->FetchBucketPage(page_id);
  uint32_t bucket_index=KeyToDirectoryIndex(key,dir_page);
  uint32_t local_depth=dir_page->GetLocalDepth(bucket_index);
  if(dir_page->GetGlobalDepth()==local_depth){
    if(dir_page->Size()<<1!=DIRECTORY_ARRAY_SIZE){
      dir_page->IncrGlobalDepth();
    }else{
      table_latch_.WUnlock();
      return false;
    }
    
  }
  page_id_t page_id_new=INVALID_PAGE_ID;
  Page*page=buffer_pool_manager_->NewPage(&page_id_new);
  HASH_TABLE_BUCKET_TYPE*bucket_page_image=reinterpret_cast<HashTableBucketPage<KeyType,ValueType,KeyComparator> *>(page->GetData());
  size_t common_bit=(bucket_index)%(1<<local_depth);
  for(size_t bucket_idx=common_bit;bucket_idx<dir_page->Size();bucket_idx+=(1<<local_depth)){
    if(((bucket_idx>>local_depth)&1)!=((bucket_index>>local_depth)&1)){
      dir_page->SetBucketPageId(bucket_idx,page_id_new);
    }else{
      //也可不要
      dir_page->SetBucketPageId(bucket_idx,page_id);
    }
    dir_page->IncrLocalDepth(bucket_idx);
  }
  for(size_t bucket_idx=0;bucket_idx<BUCKET_ARRAY_SIZE;bucket_idx++){
    bool readable=bucket_page->IsReadable(bucket_idx);
    auto data_key=bucket_page->KeyAt(bucket_idx);
    auto data_value=bucket_page->ValueAt(bucket_idx);   
    if(readable){
      page_id_t page_id_insert=static_cast<page_id_t>(this->KeyToPageId(data_key,dir_page));
      if(page_id_insert==page_id_new){
        bucket_page_image->InsertAt(bucket_idx,data_key,data_value);
        bucket_page->SetUnOccupied(bucket_idx);
        bucket_page->SetUnReadable(bucket_idx);
      }     
    }
  }
  buffer_pool_manager_->UnpinPage(page_id_new,true);
  buffer_pool_manager_->UnpinPage(page_id,true);
  buffer_pool_manager_->UnpinPage(directory_page_id_,true);
  table_latch_.WUnlock();
  bool is_inserted=this->Insert(transaction,key,value);
  return is_inserted;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page=this->FetchDirectoryPage();
  table_latch_.WLock();
  page_id_t page_id=KeyToPageId(key,dir_page);
  HASH_TABLE_BUCKET_TYPE * bucket_page=this->FetchBucketPage(page_id);
  bool is_dirty=bucket_page->Remove(key,value,comparator_);
  if(is_dirty&&bucket_page->IsEmpty()){
    buffer_pool_manager_->UnpinPage(page_id,is_dirty);
    buffer_pool_manager_->UnpinPage(directory_page_id_,false);
    Merge(transaction,key,value);
  }
  else{
    buffer_pool_manager_->UnpinPage(page_id,is_dirty);
    buffer_pool_manager_->UnpinPage(directory_page_id_,false);
  }
  table_latch_.WUnlock();
  return is_dirty;
}
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::PrintBit(){
  // HashTableDirectoryPage *dir_page=this->FetchDirectoryPage();
  // page_id_t page_id=KeyToPageId(key,dir_page);
  // HASH_TABLE_BUCKET_TYPE * bucket_page=this->FetchBucketPage(page_id);
  // bucket_page->PrintBucketBit();
  // buffer_pool_manager_->UnpinPage(page_id,false);
  // buffer_pool_manager_->UnpinPage(directory_page_id_,false);
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value){
  HashTableDirectoryPage *dir_page=this->FetchDirectoryPage();
  page_id_t page_id=KeyToPageId(key,dir_page);
  uint32_t bucket_index=KeyToDirectoryIndex(key,dir_page);
  MergeMain(page_id,bucket_index);
  buffer_pool_manager_->UnpinPage(directory_page_id_,false);//改为true试试？
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::MergeMain(page_id_t page_id,uint32_t bucket_index){
  HashTableDirectoryPage *dir_page=this->FetchDirectoryPage();
  //page_id_t page_id=KeyToPageId(key,dir_page);
  //HASH_TABLE_BUCKET_TYPE * bucket_page=this->FetchBucketPage(page_id);
  //uint32_t bucket_index=KeyToDirectoryIndex(key,dir_page);
  uint32_t local_depth=dir_page->GetLocalDepth(bucket_index);
  uint32_t bucket_index_image=dir_page->GetSplitImageIndex(bucket_index);

  if(local_depth!=0&&dir_page->GetLocalDepth(bucket_index)==dir_page->GetLocalDepth(bucket_index_image)
  &&dir_page->GetBucketPageId(bucket_index)!=dir_page->GetBucketPageId(bucket_index_image)){
    size_t common_bit=(bucket_index)%(1<<(local_depth-1));
    for(size_t bucket_idx=common_bit;bucket_idx<dir_page->Size();bucket_idx+=(1<<local_depth)){
      int bucket_bit=(bucket_index>>local_depth)&1;
      int bucket_image_bit=(bucket_idx>>local_depth)&1;
      if(bucket_bit==bucket_image_bit){
        dir_page->SetBucketPageId(bucket_idx,dir_page->GetBucketPageId(bucket_index_image));
      }
      dir_page->DecrLocalDepth(bucket_idx);
    }
    buffer_pool_manager_->DeletePage(bucket_index);
    bool flag=false;
    for(size_t i=0;i<dir_page->Size();i++){
      if(dir_page->GetLocalDepth(i)==dir_page->GetGlobalDepth()){
        flag=true;
        break;
      }
    }
    if(!flag){
      dir_page->DecrGlobalDepth();
      for(size_t i=0;i<dir_page->Size();i++){
        page_id_t bucket_index_next=dir_page->GetBucketPageId(i);
        auto bucket_page_next=reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_index_next)->GetData());
        if(bucket_page_next->IsEmpty()){
          buffer_pool_manager_->UnpinPage(bucket_index_next,false);
          MergeMain(bucket_index_next,i);
        }
        else{
          buffer_pool_manager_->UnpinPage(bucket_index_next,false);
        }
      }     
    }
    buffer_pool_manager_->UnpinPage(directory_page_id_,true);
    return true;
  }
  buffer_pool_manager_->UnpinPage(page_id,false);
  buffer_pool_manager_->UnpinPage(directory_page_id_,false);
  return false;
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
