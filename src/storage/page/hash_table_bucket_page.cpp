//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"
namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  bool flag=false;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++){
    if(IsReadable(bucket_idx)){
      if(cmp(this->array_[bucket_idx].first,key)==0){
        //printf("%ld\n",bucket_idx);
        result->template emplace_back(this->array_[bucket_idx].second);
        flag=true;
      }
    }
  }
  return flag;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsExist(KeyType key, ValueType value, KeyComparator cmp){
  for(size_t bucket_idx=0; bucket_idx<BUCKET_ARRAY_SIZE; bucket_idx++){
    bool is_occupied=IsOccupied(bucket_idx);
    bool is_readable=IsReadable(bucket_idx);
    if(is_occupied&&is_readable&&(cmp(this->array_[bucket_idx].first,key)==0)
      &&value==this->array_[bucket_idx].second){
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  //先检查是否存在,因为重复利用了不可读的位置,特殊情况是前面有不可读的位置,但是后面存在相同的key-value
  if(IsExist(key,value,cmp)){
    return false;
  }
  //insert
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++){;
    bool is_occupied=IsOccupied(bucket_idx);
    bool is_readable=IsReadable(bucket_idx);
    if(!is_occupied||(!is_readable)){
      // printf("begin======\n");
      // PrintBucketBit();
      // printf("after======\n");
      this->InsertAt(bucket_idx,key,value); 
      // PrintBucketBit();
      // printf("end======\n");
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
int HASH_TABLE_BUCKET_TYPE::GetOccupiedBit(size_t bucket_idx){
  uint32_t num_index=bucket_idx/(sizeof(char)*8);
  uint32_t bit_index=bucket_idx%(sizeof(char)*8);
  return (this->occupied_[num_index]>>(bit_index))&1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
int HASH_TABLE_BUCKET_TYPE::GetReadableBit(size_t bucket_idx){
  uint32_t num_index=bucket_idx/(sizeof(char)*8);
  uint32_t bit_index=bucket_idx%(sizeof(char)*8);
  return (this->readable_[num_index]>>(bit_index))&1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::InsertAt(size_t bucket_idx,KeyType key, ValueType value){
  this->array_[bucket_idx]=MappingType(key,value);
  SetReadable(bucket_idx);
  SetOccupied(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++){
    bool is_readable=IsReadable(bucket_idx);
    if(is_readable&&cmp(this->array_[bucket_idx].first,key)==0&&(value==this->array_[bucket_idx].second)){
      this->RemoveAt(bucket_idx);
      //printf("remove:%ld\n",bucket_idx);
      //PrintBucketBit();
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return this->array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return this->array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  uint32_t num_index=bucket_idx/(sizeof(char)*8);
  uint32_t bit_index=bucket_idx%(sizeof(char)*8);
  readable_[num_index]&=(~(1<<bit_index));

}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  uint32_t num_index=bucket_idx/(sizeof(char)*8);
  uint32_t bit_index=bucket_idx%(sizeof(char)*8);
  return (this->readable_[num_index]>>(bit_index))&1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  uint32_t num_index=bucket_idx/(sizeof(char)*8);
  uint32_t bit_index=bucket_idx%(sizeof(char)*8);
  this->occupied_[num_index]|=(1<<bit_index);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  uint32_t num_index=bucket_idx/(sizeof(char)*8);
  uint32_t bit_index=bucket_idx%(sizeof(char)*8);
  return (this->occupied_[num_index]>>(bit_index))&1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  uint32_t num_index=bucket_idx/(sizeof(char)*8);
  uint32_t bit_index=bucket_idx%(sizeof(char)*8);
  this->readable_[num_index]|=(1<<bit_index);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetUnOccupied(uint32_t bucket_idx){
  uint32_t num_index=bucket_idx/(sizeof(char)*8);
  uint32_t bit_index=bucket_idx%(sizeof(char)*8);
  this->occupied_[num_index]&=(~(1<<bit_index));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetUnReadable(uint32_t bucket_idx){
  uint32_t num_index=bucket_idx/(sizeof(char)*8);
  uint32_t bit_index=bucket_idx%(sizeof(char)*8);
  this->readable_[num_index]&=(~(1<<bit_index));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  return this->NumReadable()==BUCKET_ARRAY_SIZE;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  uint32_t res=0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++){
    if(!IsOccupied(bucket_idx)){
      break;
    }
    if(IsReadable(bucket_idx)){
      res++;
    }
  }
  return res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  return this->NumReadable()==0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucketBit(){
  for(size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++){
    if(this->IsOccupied(bucket_idx)){
      printf("1 ");
    }else{
      printf("0 ");
    }
  }
  printf("\n");
  for(size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++){
    if(this->IsReadable(bucket_idx)){
      printf("1 ");
    }else{
      printf("0 ");
    }
  }
  printf("\n");
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
