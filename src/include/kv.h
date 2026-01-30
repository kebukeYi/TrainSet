//
// Created by 19327 on 2025/12/31/星期三.
//

#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include <mutex>
#include <optional>

#pragma once

namespace train_set {

    struct StringRecord {
        std::string value;
        int64_t expire = -1; // -1 代表永不过期;
    };

    struct HashRecord {
        std::unordered_map<std::string, std::string> field_vals;
        int64_t expire = -1;
    };

    struct skipListNode;

    struct skipList {
        skipList();

        ~skipList();

        bool insert(double score, const std::string &member);

        bool erase(double score, const std::string &member);

        void rangeByRank(int64_t start, int64_t end, std::vector<std::string> &out);

        void allValue(std::vector<std::pair<double, std::string>> &out);

        size_t size() const;

    private:
        int randomLevel();

        int maxLevel = 32;
        skipListNode *head;
        float probability = 0.25;
        int level;
        size_t length;//  无符号long类型;

    };

    struct ZSetRecord {
        std::vector<std::pair<double, std::string>> items;
        bool useSkipList;
        std::unique_ptr<skipList> sl;
        std::unordered_map<std::string, double> member_to_score;
        int64_t expire = -1;
    };

    class KVStorage {
    private:
        std::unordered_map<std::string, StringRecord> string_records;
        std::unordered_map<std::string, HashRecord> hash_records;
        std::unordered_map<std::string, ZSetRecord> zset_records;
        std::unordered_map<std::string, int64_t> expires; // <key, 最大存活时间点,不是段>
        std::mutex mtx;

    public:
        bool set(const std::string &key, const std::string &value, std::optional<int64_t> expire = std::nullopt);

        bool setWithExpire(const std::string &key, const std::string &value, int64_t expire);

        std::optional<std::string> get(const std::string &key);

        int del(const std::vector<std::string> &keys);

        bool exists(const std::string &key);

        bool expire(const std::string &key, int64_t expire);

        int64_t ttl(const std::string &key);

        size_t stringKeySize() {
            return string_records.size();
        }

        //  expire 扫描
        int expireScanStep(int max_step);

        std::vector<std::pair<std::string, StringRecord>> stringSnapshot();

        std::vector<std::pair<std::string, HashRecord>> hashSnapshot();

        struct ZSetFlat {
            std::string key;
            std::vector<std::pair<double, std::string>> items;
            int64_t expire;
        };

        std::vector<ZSetFlat> zSetSnapshot();

        //  list all keys; union of string/hash/zset keys (unique)
        std::vector<std::string> listKeys();

        // Hash APIs
        // returns 1 if new field created, 0 if overwritten
        // HSET myhash field1 "foo"
        int hset(const std::string &key, const std::string &field, const std::string &value);

        bool hsetWithExpire(const std::string &key, int64_t expire);

        std::optional<std::string> hget(const std::string &key, const std::string &field);

        int hdel(const std::string &key, const std::vector<std::string> &fields);

        bool hexists(const std::string &key, const std::string &field);

        // return flatten [field, value, field, value, ...]; 扁平化值返回;
        std::vector<std::string> hgetAllByKey(const std::string &key);

        int hlen(const std::string &key);

        // ZSet APIs
        // returns number of new elements added
        // zadd user:rank   kk 90 kk1 89 kk2 88
        // zadd order:rank  mm 90 qw1 89 we6 88
        int zadd(const std::string &key, double score, const std::string &member);

        bool zaddWithExpire(const std::string &key, int64_t expire);

        // zrange user:rank 0 -1
        std::vector<std::string> zrange(const std::string &key, int64_t start, int64_t end);

        std::optional<double> zscore(const std::string &key, const std::string &member);

        int zremove(const std::string &key, const std::vector<std::string> &members);

    private:
        size_t ZSetVectorThreshold = 128;

        static int64_t nowMs();

        static bool isExpiredOfString(const StringRecord &record, int64_t now_ms);

        static bool isExpiredOfHash(const HashRecord &record, int64_t now_ms);

        static bool isExpiredOfZSet(const ZSetRecord &record, int64_t now_ms);

        void clearIfExpiredOfString(const std::string &key, int64_t now_ms);

        void clearIfExpiredOfHash(const std::string &key, int64_t now_ms);

        void clearIfExpiredOfZSet(const std::string &key, int64_t now_ms);
    };
}