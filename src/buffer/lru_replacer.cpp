//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
    capacity_=num_pages;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) { 
    latch_.lock();
    if(replace_list_.empty()) {
        frame_id=nullptr;
        latch_.unlock();     
        return false;
    }
    *frame_id=replace_list_.back();
    replace_list_.pop_back();
    exist_rep_.erase(*frame_id);
    latch_.unlock();
    return true; }

void LRUReplacer::Pin(frame_id_t frame_id) {
    latch_.lock();
    if(exist_rep_.count(frame_id)==0U){
        latch_.unlock();
        return;
    }  
    replace_list_.erase(exist_rep_[frame_id]);
    exist_rep_.erase(frame_id);
    latch_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    latch_.lock();
    if(exist_rep_.count(frame_id)!=0U) {
        latch_.unlock();
        return ;
    }
    if(replace_list_.size()==capacity_){
        frame_id_t *victim=nullptr;
        if(!Victim(victim)){
            latch_.unlock();
            return;
        }
    }
    replace_list_.push_front(frame_id);
    exist_rep_[frame_id]=++replace_list_.begin();
    latch_.unlock();
}

size_t LRUReplacer::Size() {
    return static_cast<size_t>(replace_list_.size());
}
}  // namespace bustub
