#pragma once

#include <string>
#include <vector>

namespace bustub {

/** @brief Unique ID type. */
using uid_t = int64_t;

/** @brief The observed remove set datatype. */
template <typename T>
class ORSet {
 public:
  ORSet()=default;

  /**
   * @brief Checks if an element is in the set.
   *
   * @param elem the element to check
   * @return true if the element is in the set, and false otherwise.
   */
  auto Contains(const T &elem) const -> bool;

  /**
   * @brief Adds an element to the set.
   *
   * @param elem the element to add
   * @param uid unique token associated with the add operation.
   */
  void Add(const T &elem, uid_t uid);

  /**
   * @brief Removes an element from the set if it exists.
   *
   * @param elem the element to remove.
   */
  void Remove(const T &elem);

  /**
   * @brief Merge changes from another ORSet.
   *
   * @param other another ORSet
   */
  void Merge(const ORSet<T> &other);

  /**
   * @brief Gets all the elements in the set.
   *
   * @return all the elements in the set.
   */
  auto Elements() const -> std::vector<T>;

  /**
   * @brief Gets a string representation of the set.
   *
   * @return a string representation of the set.
   */
  auto ToString() const -> std::string;

 private:
  // TODO(student): Add your private memeber variables to represent ORSet.
  std::vector<std::pair<T, uid_t>> e;
  std::vector<std::pair<T, uid_t>> t;

  inline auto GenerateUid() -> uid_t { return next_uid_++; }
  uid_t next_uid_= 0;

};

}  // namespace bustub
