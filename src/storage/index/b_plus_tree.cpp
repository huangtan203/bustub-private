//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { 
  //root_page_id是否有效/page是否能找到,否的话返回false
  //找到root_page_id对应的页,判断其size是否大于1,非叶子节点的根节点其数量最小值为1,内部节点的
  //根节点其大小为0
  if(root_page_id_==INVALID_PAGE_ID){
    return true;
  }
  Page*page=buffer_pool_manager_->FetchPage(this->root_page_id_,nullptr);
  if(page==nullptr){
    buffer_pool_manager_->UnpinPage(this->root_page_id_,false);
    return true;
  }
  auto root_page=reinterpret_cast<InternalPage*>(page->GetData());
  if((root_page->IsLeafPage()&&root_page->GetSize()==0)||(!root_page->IsLeafPage()&&root_page->GetSize()==1)){
    buffer_pool_manager_->UnpinPage(this->root_page_id_,false);
    return true;
  }
  buffer_pool_manager_->UnpinPage(this->root_page_id_,false);
  return false;
 }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  page_id_t page_id=root_page_id_;
  Page*page=buffer_pool_manager_->FetchPage(page_id,nullptr);
  auto root_page=reinterpret_cast<InternalPage*>(page->GetData());
  while(!root_page->IsLeafPage()){
    int index=root_page->Lookup(key,comparator_);   
    //强制转为page_id
    page_id=root_page->ValueAt(index);
    buffer_pool_manager_->UnpinPage(page_id,false);
    page=buffer_pool_manager_->FetchPage(page_id,nullptr);
    root_page=reinterpret_cast<InternalPage*>(page->GetData());
  }
  //root_page为叶子节点,强转为叶子节点
  auto leaf_page=reinterpret_cast<LeafPage*>(page->GetData());
  int find_idx=leaf_page->KeyIndex(key,comparator_);
  bool ans=false; 
  if(comparator_(key,leaf_page->GetItem(find_idx).first)==0){
    while(find_idx<leaf_page->GetSize()&&comparator_(key,leaf_page->GetItem(find_idx).first)==0){
      result->emplace_back(leaf_page->GetItem(find_idx++).second);
      ans=true;
    }
  }
  buffer_pool_manager_->UnpinPage(page_id,false);
  return ans;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) { 
  if(IsEmpty()){
    StartNewTree(key,value);
    return true;
  }
  if(InsertIntoLeaf(key,value,transaction)){
    return true;
  }
  return false; }
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t page_id;
  Page*page=buffer_pool_manager_->NewPage(&page_id,nullptr);
  if(page==nullptr){
    throw "out of memory";
  }
  LeafPage* leaf_page=reinterpret_cast<LeafPage*>(page->GetData());
  leaf_page->Init(page_id, INVALID_PAGE_ID, this->leaf_max_size_);
  leaf_page->Insert(key,value,comparator_);
  this->root_page_id_=page_id;
  UpdateRootPageId(1);
  buffer_pool_manager_->UnpinPage(page_id,true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if(IsEmpty()){
    StartNewTree(key,value);
    return true;
  }
  Page*page=FindLeafPage(key,false);
  LeafPage* leaf_page=reinterpret_cast<LeafPage*>(page->GetData());
  if(leaf_page->Lookup(key,nullptr,comparator_)){
    //已经存在
    buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
    return false;
  }
  leaf_page->Insert(key,value,comparator_);
  if(leaf_page->GetSize()==leaf_page->GetMaxSize()){
    //fenlie
    LeafPage*split_page=reinterpret_cast<LeafPage*>(Split(leaf_page));
    this->InsertIntoParent(leaf_page,split_page->KeyAt(0),split_page,transaction);
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t page_id_new;
  Page*page=buffer_pool_manager_->NewPage(&page_id_new,nullptr);
  if(page==nullptr){
    throw "out of memory";
  }
  N* split_node;
  if(node->IsLeafPage()){
   node=reinterpret_cast<LeafPage*>(node);
   split_node=reinterpret_cast<LeafPage*>(page->GetData());
   split_node->Init(page_id_new, node->GetParentPageId(), this->leaf_max_size_);
   node->MoveHalfTo(split_node);
   split_node->SetNextPageId(node->GetNextPageId());
   node->SetNextPageId(split_node->GetPageId());
   split_node=reinterpret_cast<N*>(split_node);
  }else{
    auto node_new=reinterpret_cast<InternalPage*>(node);
    split_node=reinterpret_cast<InternalPage*>(page->GetData());
    split_node->Init(page_id_new, node_new->GetParentPageId(), this->internal_max_size_);
    node_new->MoveHalfTo(split_node,buffer_pool_manager_);
    split_node=reinterpret_cast<N*>(split_node);
  }
  //buffer_pool_manager_->UnpinPage(page_id_new,true);
  //需要返回,暂时不能unpin
  return split_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
  Transaction *transaction) {
  if(old_node->IsRootPage()){
    Page*new_root_page=buffer_pool_manager_->NewPage(&root_page_id_);
    auto new_root=reinterpret_cast<InternalPage*>(new_root_page->GetData());
    new_root->Init(root_page_id_,INVALID_PAGE_ID,this->internal_max_size_);
    UpdateRootPageId(0);
    new_root->PopulateNewRoot(old_node->GetPageId(),key,new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    buffer_pool_manager_->UnpinPage(root_page_id_,true);
    return ;  
  }
  page_id_t parent_page_id=old_node->GetParentPageId();
  InternalPage *parent_page=reinterpret_cast<InternalPage*>(buffer_pool_manager_->FetchPage(parent_page_id,nullptr)->GetData());
  parent_page->InsertNodeAfter(old_node->GetPageId(),key,new_node->GetPageId());
  if(parent_page->GetSize()==parent_page->GetMaxSize()){
    auto split_page=Split(parent_page);
    InsertIntoParent(parent_page,split_page->KeyAt(0),split_page);//之所以是keyat(0)是因为需要提取一个中间的key放到parent上
    buffer_pool_manager_->UnpinPage(split_page->GetPageId(),true);
  }
  buffer_pool_manager_->UnpinPage(parent_page_id,true);
  }

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if(this->IsEmpty()){
    return ;
  }
  Page*page=this->FindLeafPage(key,false);
  if(page==nullptr){
    buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
    return ;
  }
  LeafPage* leaf_page=reinterpret_cast<LeafPage*>(page->GetData());
  leaf_page->RemoveAndDeleteRecord(key,comparator_);
  if(leaf_page->GetSize()>=leaf_page->GetMinSize()){
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),true);
    return ;
  }
  bool flag=CoalesceOrRedistribute(leaf_page,transaction);
  if(flag){
    buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
    buffer_pool_manager_->DeletePage(page->GetPageId());
  }else{
    buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if(node->IsRootPage()){
    return AdjustRoot(node);
  }
  page_id_t page_id=node->GetParentPageId();
  page_id_t node_page_id=node->GetPageId();
  Page*page=buffer_pool_manager_->FetchPage(page_id,nullptr);
  InternalPage*parent_page=reinterpret_cast<InternalPage*>(page->GetData());
  int index=parent_page->ValueIndex(node_page_id);
  page_id_t sli_page_id=static_cast<page_id_t>(parent_page->ValueAt(index==0?1:index-1));
  Page*sli_page=buffer_pool_manager_->FetchPage(sli_page_id,nullptr);
  N *sli_plus_page=reinterpret_cast<N*>(sli_page->GetData());
  if(sli_plus_page->GetSize()+node->GetSize()<=node->GetMaxSize()){
    bool res=Coalesce(&sli_plus_page,&node,&parent_page,index,transaction);
    buffer_pool_manager_->UnpinPage(page_id,true);
    buffer_pool_manager_->UnpinPage(sli_page_id,true);
    if(res){
      buffer_pool_manager_->DeletePage(page_id);
    }
    if(index==0){
      buffer_pool_manager_->DeletePage(sli_page_id);
    }
    return index>0;
  }else{
    Redistribute(sli_plus_page,node,index);
    buffer_pool_manager_->UnpinPage(page_id,true);
    buffer_pool_manager_->UnpinPage(sli_page_id,true);
  }
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
//merge操作
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
  Transaction *transaction) {
    if(index==0){
      std::swap(neighbor_node,node);
      index=1;
    }
    if((*node)->IsLeafPage()){
      LeafPage* node_new=reinterpret_cast<LeafPage*>(*node);
      LeafPage* neighbor_node_new=reinterpret_cast<LeafPage*>(*neighbor_node);
      node_new->MoveAllTo(neighbor_node);
      neighbor_node->SetNextPageId(node_new->GetNextPageId());
    }else{
      InternalPage*node_new=reinterpret_cast<InternalPage*>(*node);
      InternalPage*neighbor_node_new=reinterpret_cast<InternalPage*>(*neighbor_node);
      auto mid_key=(*parent)->KeyAt(index);
      node_new->MoveAllTo(neighbor_node_new,mid_key,buffer_pool_manager_);
    }
    (*parent)->Remove(index);
    //buffer_pool_manager_->UnpinPage();
  return CoalesceOrRedistribute(*parent,transaction);
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
//借位操作
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  page_id_t page_id=node->GetParentPageId();
  Page*page=buffer_pool_manager_->FetchPage(page_id,nullptr);
  auto parent_page=reinterpret_cast<InternalPage*>(page->GetData());
  if(index==0){
    if(node->IsLeafPage()){
      node=reinterpret_cast<LeafPage*>(node);
      neighbor_node=reinterpret_cast<LeafPage*>(neighbor_node);
      neighbor_node->MoveFirstToEndOf(node);
      parent_page->SetKeyAt(1,neighbor_node->KeyAt(0));
      //
    }else{
      node=reinterpret_cast<InternalPage*>(node);
      neighbor_node=reinterpret_cast<InternalPage*>(neighbor_node);
      int idx=parent_page->ValueIndex(neighbor_node->GetPageId());
      auto key=parent_page->KeyAt(idx+1);
      neighbor_node->MoveFirstToEndOf(node,key,buffer_pool_manager_);
      parent_page->SetKeyAt(1,neighbor_node->KeyAt(0));
    }
  }else{
    //有左节点
    if(node->IsLeafPage()){
      node=reinterpret_cast<LeafPage*>(node);
      neighbor_node=reinterpret_cast<LeafPage*>(neighbor_node);
      node->MoveFirstToEndOf(neighbor_node);
      parent_page->SetKeyAt(index,node->KeyAt(0));
    }else{
      node=reinterpret_cast<InternalPage*>(node);
      neighbor_node=reinterpret_cast<InternalPage*>(neighbor_node);
      int idx=parent_page->ValueIndex(node->GetPageId());
      auto key=parent_page->KeyAt(idx);
      node->MoveFirstToEndOf(neighbor_node,key,buffer_pool_manager_);
      parent_page->SetKeyAt(index,node->KeyAt(0));
    }
  }
  buffer_pool_manager_->UnpinPage(page_id,true); 
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) { 
  //根节点只有一个左儿子时应该删除
  if(old_root_node->GetSize()==1&&old_root_node->IsRootPage()){
    auto old_root_node_new=reinterpret_cast<InternalPage*>(old_root_node);
    auto page_id=old_root_node_new->RemoveAndReturnOnlyChild();
    root_page_id_=static_cast<page_id_t>(page_id);
    UpdateRootPageId(0);
    Page*page=buffer_pool_manager_->FetchPage(page_id,nullptr);
    InternalPage*page_new=reinterpret_cast<InternalPage*>(page->GetData());
    page_new->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(page_id,true);
    return true;
  }
  //所有节点都被删除
  if(old_root_node->IsRootPage()&&old_root_node->GetSize()==0){
    root_page_id_=INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return true;
  }
  return false; }

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() { return INDEXITERATOR_TYPE(); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");
  if(root_page_id_==INVALID_PAGE_ID){
    throw "not a tree";
  }
  //page_id_t page_id=root_page_id_;
  Page*page=buffer_pool_manager_->FetchPage(root_page_id_,nullptr);
  auto root_page=reinterpret_cast<InternalPage*>(page->GetData());//这里不同,没有getdata应该是错的
  while(!root_page->IsLeafPage()){ 
    int index=leftMost?root_page->ValueAt(0):root_page->Lookup(key,comparator_);   
    //强制转为page_id
    page_id_t next_page_id=root_page->ValueAt(index);
    buffer_pool_manager_->UnpinPage(root_page->GetPageId(),false);   
    page=buffer_pool_manager_->FetchPage(next_page_id,nullptr);
    root_page=reinterpret_cast<InternalPage*>(page->GetData());
    //page_id=next_page_id;
  }
  return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
