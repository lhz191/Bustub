#include "primer/orset.h"
#include <algorithm>
#include <string>
#include <vector>
#include "common/exception.h"
#include "fmt/format.h"

namespace bustub {

template <typename T>
auto ORSet<T>::Contains(const T &elem) const -> bool {
  // TODO(student): Implement this
  auto it = std::find_if(e.begin(), e.end(), [&](const auto &p) { return p.first == elem; });
  return it != e.end();
}

template <typename T>
void ORSet<T>::Add(const T &elem, uid_t uid) {
  // // TODO(student): Impleme nt this
  std::pair<T, uid_t> p1 = std::make_pair(elem, uid);
  this->e.push_back(p1);
}

template <typename T>
void ORSet<T>::Remove(const T &elem) {
  // TODO(student): Implement this
  auto it = e.begin();
  while (it != e.end()) {
    if (it->first == elem) {
      t.push_back(*it);
      it = e.erase(it);
    } else {
      it++;
    }
  }
}

template <typename T>
void ORSet<T>::Merge(const ORSet<T> &other) {
  for (auto it = other.t.begin(); it != other.t.end(); it++) {
    T value = (*it).first;
    for (auto it2 = this->e.begin(); it2 != this->e.end();) {
      if ((*it2).first == value && (*it2).second == (*it).second) {
        it2 = this->e.erase(it2);
      } else {
        it2++;
      }
    }
  }
  // auto temp=copy(other);
  // auto it3=this->t.begin();
  // while(it3!=this->t.end())
  // {
  //   T value2=*it3.first;
  //   auto it4=temp.e.begin();
  //   while(it4!=temp.e.end())
  //   {
  //     if(*it4.first==value2)
  //     {
  //       it4=this->e.erase(it4);
  //     }
  //   }
  // }
  auto temp = other.e;
  for (auto it3 = this->t.begin(); it3 != this->t.end(); it3++) {
    T value2 = (*it3).first;
    for (auto it4 = temp.begin(); it4 != temp.end();) {
      if ((*it4).first == value2 && (*it4).second == (*it3).second) {
        it4 = temp.erase(it4);
      } else {
        it4++;
      }
    }
  }
  // auto it5=temp.e.begin();
  // while(it5!=temp.e.end())
  // {
  //   if(this->e.find(*it5)==0)
  //   {
  //     this->e.push_back(*it5);
  //   }
  // }
  auto it5 = temp.begin();
  while (it5 != temp.end()) {
    if (std::find_if(e.begin(), e.end(),
                     [&](const auto &p) { return p.first == it5->first && p.second == it5->second; }) == e.end()) {
      e.push_back(*it5);
    }
    it5++;
  }
  auto temp2 = other.t;
  auto it6 = temp2.begin();
  while (it6 != temp2.end()) {
    if (std::find_if(t.begin(), t.end(),
                     [&](const auto &p) { return p.first == it6->first && p.second == it6->second; }) == t.end()) {
      t.push_back(*it6);
    }
    it6++;
  }
}

template <typename T>
auto ORSet<T>::Elements() const -> std::vector<T> {
  // TODO(student): Implement this
  std::vector<T> temp;
  auto it = this->e.begin();
  for (; it != this->e.end(); it++) {
    // if(temp.find(*it.first)==0)
    // {
    //   temp.push_back(*it.first);
    // }
    if (std::find(temp.begin(), temp.end(), (*it).first) == temp.end()) {
      temp.push_back((*it).first);
    }
  }
  return temp;
}
// template <typename T>
// auto ORSet<T>::Elements() const -> std::vector<T> {
//     std::vector<T> temp;
//     for (const auto &p : e) {
//         // 使用 std::find 查找元素是否已经存在于 temp 向量中
//         if (std::find(temp.begin(), temp.end(), p.first) == temp.end()) {
//             temp.push_back(p.first);
//         }
//     }
//     return temp;
// }

template <typename T>
auto ORSet<T>::ToString() const -> std::string {
  auto elements = Elements();
  std::sort(elements.begin(), elements.end());
  return fmt::format("{{{}}}", fmt::join(elements, ", "));
}

template class ORSet<int>;
template class ORSet<std::string>;

}  // namespace bustub
