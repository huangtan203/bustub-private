//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) {
  // Allocate and create individual BufferPoolManagerInstances
  this->nums_instance_=num_instances;
  this->pool_size_=pool_size;
  this->next_index_=0;
  for(size_t i=0;i<nums_instance_;i++){
    bpms_.emplace_back(new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager)); 
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager(){
  for(size_t i=0;i<nums_instance_;i++){
    delete bpms_[i];
  }
  bpms_.clear();
  std::vector<BufferPoolManager*>().swap(bpms_);
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return pool_size_*nums_instance_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  
  return bpms_[(static_cast<int>(page_id))%(static_cast<int>(nums_instance_))];
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolManager(page_id)->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  return dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id))->UnpinPgImp(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  return dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id))->FlushPgImp(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  latch_.lock();
  for(size_t i=0;i<nums_instance_;i++){
    Page*page=(this->bpms_[(i+next_index_)%nums_instance_])->NewPage(page_id);
    if(page!=nullptr){
      next_index_=(next_index_+1)%nums_instance_;
      latch_.unlock();
      return page;
    }
  }
  latch_.unlock();
  return nullptr;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  return  dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id))->DeletePgImp(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for(size_t i=0;i<nums_instance_;i++){
    dynamic_cast<BufferPoolManagerInstance *>(bpms_[i])->FlushAllPgsImp();
  }
}

}  // namespace bustub
