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
    capacity=num_pages;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) { 
    mtx.lock();
    if(replace_list.size()==0) {
        mtx.unlock();
        return false;
    }
    *frame_id=replace_list.back();
    replace_list.pop_back();
    exist_rep.erase(*frame_id);
    mtx.unlock();
    return true; }

void LRUReplacer::Pin(frame_id_t frame_id) {
    if(!exist_rep.count(frame_id)) return;
    mtx.lock();
    replace_list.remove(frame_id);
    exist_rep.erase(frame_id);
    mtx.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    if(exist_rep.count(frame_id)) return ;
    mtx.lock();
    if(replace_list.size()==capacity){
        frame_id_t back=replace_list.back();
        replace_list.pop_back();
        exist_rep.erase(back);
    }
    replace_list.push_front(frame_id);
    exist_rep[frame_id]=1;
    mtx.unlock();
}

size_t LRUReplacer::Size() {
    return (size_t)replace_list.size();
}
}  // namespace bustub
