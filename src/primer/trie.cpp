#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  std::shared_ptr<const TrieNode> x = this->root_;
  //  std::map<char, std::shared_ptr<const TrieNode>> children_;
  for (char ch : key) {
    if (x == nullptr) {
      return nullptr;
    }
    auto it = x->children_.find(ch);
    if (it == x->children_.end())  //找不到
    {
      return nullptr;
    } else {
      x = it->second;
    }
  }
  if (x->is_value_node_ == true) {
    // 如果是值节点，尝试将当前节点转换为 TrieNodeWithValue<T> 指针
    auto newnode = dynamic_cast<const TrieNodeWithValue<T> *>(x.get());
    if (newnode != nullptr) {
      // 如果转换成功，返回值的普通指针
      return newnode->value_.get();  //!!!
    } else {
      // 如果转换失败，说明类型不匹配，返回 nullptr
      return nullptr;
    }
  } else {
    return nullptr;
  }
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");
  std::unique_ptr<TrieNode> root1 = root_->Clone();
  std::shared_ptr<TrieNode> curnode = std::move(root1);
  std::shared_ptr<TrieNode> parent(curnode);

  //  std::map<char, std::shared_ptr<const TrieNode>> children_;
  if (curnode == nullptr)  //如果是空树的话
  {
    std::shared_ptr<TrieNode> newroot = std::make_shared<TrieNode>();
    curnode = newroot;
    parent = newroot;
  }
  char lastch = 'a';
  for (char ch : key) {
    auto it = curnode->children_.find(ch);
    if (it == curnode->children_.end())  //找不到
    {
      std::shared_ptr<TrieNode> newnode = std::make_shared<TrieNode>();
      // curnode->children_.emplace(std::make_pair(ch, newnode));
      curnode->children_.insert({ch, newnode});
      parent = curnode;
      curnode = newnode;
      lastch = ch;
    } else {
      parent = curnode;
      curnode = std::const_pointer_cast<bustub::TrieNode>(it->second);
      lastch = ch;
    }
  }
  if (curnode->is_value_node_ == false) {
    //  std::map<char, std::shared_ptr<const TrieNode>> children_;
    // std::shared_ptr<TrieNodeWithValue<T>> newnode = std::make_shared<TrieNodeWithValue<T>>(std::move(value));
    std::shared_ptr<T> value_ptr = std::make_shared<T>(value);
    TrieNodeWithValue newnode(value_ptr);
    std::shared_ptr<TrieNodeWithValue<T>> temp = std::make_shared<TrieNodeWithValue<T>>(newnode);
    std::shared_ptr<TrieNode> newnode1 = std::dynamic_pointer_cast<TrieNodeWithValue<T>>(temp);
    // std::shared_ptr<TrieNodeWithValue> newnode1 = std::make_shared<const TrieNodeWithValue>(newnode);
    // std::shared_ptr<TrieNode> base_node_ptr = std::make_shared<TrieNode>(newnode);
    // std::shared_ptr<const TrieNode> newnode1 = std::static_pointer_cast<const TrieNode>(base_node_ptr);
    // std::shared_ptr<const TrieNode> newnode1=newnode;
    // parent->children_.erase(lastch);
    // parent->children_.emplace(std::make_pair(lastch, newnode));
    // parent->children_[char(lastch)]=newnode1;
    parent->children_.emplace(char(lastch), newnode1);

    return Trie(std::move(root1));
  } else {
    return Trie(std::move(root1));
  }
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");
  std::unique_ptr<TrieNode> root1 = root_->Clone();
  std::shared_ptr<TrieNode> curnode = std::move(root1);
  std::shared_ptr<TrieNode> parent(curnode);
  if (curnode == nullptr) {
    return Trie(std::move(root1));
  }
  for (char ch : key) {
    auto it = curnode->children_.find(ch);
    if (it == curnode->children_.end()) {
      return Trie(std::move(root1));
    } else {
      parent = curnode;
      curnode = std::const_pointer_cast<bustub::TrieNode>(it->second);
    }
  }
  if (curnode->is_value_node_ == false) {
    return Trie(std::move(root1));
  } else {
    curnode->is_value_node_ = false;
    return Trie(std::move(root1));
  }

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
