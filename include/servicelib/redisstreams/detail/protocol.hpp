#pragma once

#include <cstddef>
#include <hiredis/hiredis.h>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace servicelib::redisstreams {
// Redis fields are an ordered sequence, not a map. Names may repeat and both
// names and values are binary-safe.
using Fields = std::vector<std::pair<std::string, std::string>>;
struct Entry {
  std::string id;
  Fields fields;
};

namespace detail {
struct Reply {
  int type{};
  long long integer{};
  std::string text;
  std::vector<Reply> elements;

  static Reply copy(const redisReply *raw) {
    std::size_t remaining = 8 * 1024 * 1024;
    return copyBounded(raw, remaining, 0);
  }

private:
  static Reply copyBounded(const redisReply *raw, std::size_t &remaining,
                           unsigned depth) {
    if (!raw)
      throw std::runtime_error("Redis disconnected before reply");
    // Bound our retained copy. hiredis has already parsed the native response;
    // this is not a bound on its parser's transient allocation.
    if (depth > 8 || remaining < sizeof(Reply))
      throw std::runtime_error("Redis reply exceeds adapter limits");
    remaining -= sizeof(Reply);
    Reply out{raw->type, raw->integer, {}, {}};
    if (raw->str) {
      if (raw->len > remaining)
        throw std::runtime_error("Redis reply exceeds adapter byte limit");
      remaining -= raw->len;
      out.text.assign(raw->str, raw->len);
    }
    if (raw->elements > remaining / sizeof(Reply))
      throw std::runtime_error("Redis reply exceeds adapter element limit");
    out.elements.reserve(raw->elements);
    for (std::size_t i = 0; i < raw->elements; ++i)
      out.elements.push_back(
          copyBounded(raw->element[i], remaining, depth + 1));
    return out;
  }
};

inline const std::vector<Reply> &Array(const Reply &reply) {
  if (reply.type != REDIS_REPLY_ARRAY)
    throw std::runtime_error("Expected Redis array reply");
  return reply.elements;
}
inline const std::string &String(const Reply &reply) {
  if (reply.type != REDIS_REPLY_STRING)
    throw std::runtime_error("Expected Redis bulk string reply");
  return reply.text;
}
inline std::vector<Entry> Entries(const Reply &reply) {
  std::vector<Entry> entries;
  for (const auto &item : Array(reply)) {
    const auto &row = Array(item);
    if (row.size() != 2)
      throw std::runtime_error("Invalid Redis stream entry");
    const auto &fields = Array(row[1]);
    if (fields.size() % 2)
      throw std::runtime_error("Unpaired Redis stream field");
    Entry entry{String(row[0]), {}};
    entry.fields.reserve(fields.size() / 2);
    for (std::size_t i = 0; i < fields.size(); i += 2)
      entry.fields.emplace_back(String(fields[i]), String(fields[i + 1]));
    entries.push_back(std::move(entry));
  }
  return entries;
}
inline std::vector<Entry> ReadEntries(const Reply &reply,
                                      const std::string &key) {
  if (reply.type == REDIS_REPLY_NIL)
    return {};
  const auto &streams = Array(reply);
  if (streams.size() != 1)
    throw std::runtime_error("Unexpected Redis stream count");
  const auto &stream = Array(streams.front());
  if (stream.size() != 2 || String(stream[0]) != key)
    throw std::runtime_error("Unexpected Redis stream identity");
  return Entries(stream[1]);
}
struct ClaimedEntries {
  std::string next;
  std::vector<Entry> entries;
  std::vector<std::string> deleted;
};
inline ClaimedEntries ParseClaim(const Reply &reply) {
  const auto &values = Array(reply);
  // Redis 7+ also returns IDs removed from PEL because the stream entry was
  // trimmed/deleted. Never mistake an empty page for the end of its scan.
  if (values.size() != 3)
    throw std::runtime_error("Expected Redis 7 XAUTOCLAIM reply");
  ClaimedEntries out{String(values[0]), Entries(values[1]), {}};
  for (const auto &id : Array(values[2]))
    out.deleted.push_back(String(id));
  return out;
}
inline std::vector<std::string> AddCommand(const std::string &key,
                                           const Fields &fields, bool create) {
  if (fields.empty())
    throw std::invalid_argument("Redis entry requires fields");
  std::vector<std::string> args{"XADD", key};
  if (!create)
    args.push_back("NOMKSTREAM");
  args.push_back("*");
  for (const auto &[name, value] : fields) {
    args.push_back(name);
    args.push_back(value);
  }
  return args;
}
} // namespace detail
} // namespace servicelib::redisstreams
