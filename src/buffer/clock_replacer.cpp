//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
    ref_flg.resize(num_pages,false);
    capacity=num_pages;
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) { 
    if(exist_rep.size()==0) return false;
    mtx.lock();
    while(true){
        if(!exist_rep.count(placeholder)) placeholder++;
        else{
            if(ref_flg[placeholder]) ref_flg[placeholder++]=false;
            else{
                *frame_id=placeholder;
                exist_rep.erase(*frame_id);
                ref_flg[*frame_id]=false;
                break;
            }
        }
        if(placeholder==capacity) placeholder=0;
    }   
    mtx.unlock();
    return true;
 }

void ClockReplacer::Pin(frame_id_t frame_id) {
    if(!exist_rep.count(frame_id)) return ;
    mtx.lock();
    exist_rep.erase(frame_id);
    ref_flg[frame_id]=false;
    mtx.unlock();
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
    if(exist_rep.count(frame_id)) return;
    mtx.lock();
    if(exist_rep.size()==capacity){
        frame_id_t *frame_id=0;
        Victim(frame_id);
    }
    exist_rep[frame_id]=true;
    ref_flg[frame_id]=true;
    mtx.unlock();
}

size_t ClockReplacer::Size() { return exist_rep.size(); }

}  // namespace bustub
