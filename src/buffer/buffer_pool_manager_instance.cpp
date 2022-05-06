//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  if(page_id==INVALID_PAGE_ID||!page_table_.count(page_id)) return false;
  latch_.lock();
  frame_id_t frame_id=page_table_[page_id];
  Page*page_=&pages_[frame_id];
  if(page_->pin_count_){
    latch_.unlock();
    return false;
  }
  if(page_->is_dirty_){
    disk_manager_->WritePage(page_id,page_->data_);
    page_->is_dirty_=false;
  }
  latch_.unlock();
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  latch_.lock();
  for(auto [a,b]:page_table_){
    if(pages_[b].pin_count_==0&&pages_[b].is_dirty_){
      disk_manager_->WritePage(a,pages_[b].data_);
    }
  }
  latch_.unlock();
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  latch_.lock();
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  frame_id_t frame_id;
  Page*page_;
  // if(free_list_.size()==0&&replacer_->Victim(&frame_id)==false){
  //   latch_.unlock();
  //   return nullptr;
  // }
  bool is_all_pin=true;
  for(frame_id_t i=0;i<static_cast<frame_id_t>(pool_size_);i++){
    if(pages_[i].pin_count_==0){
      is_all_pin=false;
      break;
    }
  }
  if(is_all_pin){
    latch_.unlock();
    return nullptr;
  }
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  if(free_list_.size()){
    frame_id=free_list_.front();
    free_list_.pop_front();   
  }else{
    if(replacer_->Victim(&frame_id)==false){
      latch_.unlock();
      return nullptr;
    }
  } 
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  page_=&pages_[frame_id];
  page_id_t page_id_new=page_->page_id_;
  if(page_->is_dirty_){
    disk_manager_->WritePage(page_id_new,page_->data_);
  }
  page_table_.erase(page_id_new);
  page_id_new=AllocatePage();
  page_table_[page_id_new]=frame_id;
  page_->ResetMemory();
  page_->page_id_=page_id_new;
  page_->pin_count_++;
  page_->is_dirty_=false;
  replacer_->Pin(frame_id);
  *page_id=page_id_new;
  latch_.unlock();
  // 4.   Set the page ID output parameter. Return a pointer to P.
  return page_;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  frame_id_t frame_id=-1;
  latch_.lock();
  Page*page_;
  if(page_table_.count(page_id)){
    frame_id=page_table_[page_id];
    replacer_->Pin(frame_id);
    page_=&pages_[frame_id];
    page_->pin_count_++;
    latch_.unlock();
    return page_;
  }else{
    if(free_list_.size()){
      frame_id=free_list_.front();
      free_list_.pop_front();
      page_=&pages_[frame_id];
    }else {
      if(replacer_->Victim(&frame_id)==false){
        latch_.unlock();
        return nullptr;
      }
      page_=&pages_[frame_id];
      page_id_t page_id_old=page_->page_id_;
      if(page_->is_dirty_){   
        disk_manager_->WritePage(page_id_old,page_->data_);
      }
      page_table_.erase(page_id_old);
    }    
  }
  page_table_[page_id]=frame_id;
  page_->page_id_=page_id;
  page_->pin_count_++;
  replacer_->Pin(frame_id);
  disk_manager_->ReadPage(page_id,page_->data_);
  // 2.     If R is dirty, write it back to the disk.  
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  latch_.unlock();
  return page_;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  latch_.lock();
  
  // 1.   Search the page table for the requested page (P).
  frame_id_t frame_id;
  Page*page_;
  if(!page_table_.count(page_id)){
    latch_.unlock();
    return true;
  }
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  frame_id=page_table_[page_id];
  page_=&pages_[frame_id];
  if(page_->pin_count_){
    latch_.unlock();
    return false;
  }
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  if(page_->is_dirty_){
    FlushPgImp(page_id);
  }
  // DeallocatePage(page_id);
  this->DeallocatePage(page_id);
  page_table_.erase(page_id);
  free_list_.push_back(frame_id);
  page_->ResetMemory();
  page_->pin_count_=0;
  page_->is_dirty_=false;
  page_->page_id_=INVALID_PAGE_ID;
  latch_.unlock();
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) { 
  if(!page_table_.count(page_id)) return false;
  latch_.lock();
  frame_id_t frame_id;
  Page*page_;
  frame_id=page_table_[page_id];
  page_=&pages_[frame_id];
  if(page_->pin_count_<=0){
    latch_.unlock();
    return false;
  }
  page_->pin_count_--;
  page_->is_dirty_=is_dirty;
  if(page_->pin_count_==0){
    replacer_->Unpin(frame_id);
  }
  latch_.unlock();
  return true; }

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
