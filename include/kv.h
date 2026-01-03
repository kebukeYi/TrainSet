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
        int64_t expire;
    };

    struct HashRecord {
        std::unordered_map<std::string, std::string> field_vals;
        int64_t expire;
    };

    struct skipListNode;

    struct skipList {
        skipList();

        ~skipList();

        bool insert(double score, std::string &member);

        bool erase(double score, std::string &member);

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
        bool set(std::string& key, std::string& value, std::optional<int64_t> expire = std::nullopt);

        bool setWithExpire(std::string& key, std::string& value, int64_t expire);

        std::optional<std::string> get(std::string& key);

        int del(std::vector<std::string> &keys);

        bool exists(std::string &key);

        bool expire(std::string &key, int64_t expire);

        int64_t getExpire(std::string &key);

        size_t stringKeySize() { return string_records.size(); }

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
        int hset(std::string& key, std::string& field, std::string& value);

        bool hsetWithExpire(std::string& key, int64_t expire);

        std::optional<std::string> hget(std::string& key, std::string& field);

        int hdel(std::string& key, std::vector<std::string> &fields);

        bool hexists(std::string& key, std::string& field);

        // return flatten [field, value, field, value, ...]; 扁平化值返回;
        std::vector<std::string> hgetAllByKey(std::string& key);

        int hlen(std::string& key);

        // ZSet APIs
        // returns number of new elements added
        // zadd user:rank   kk 90 kk1 89 kk2 88
        // zadd order:rank  mm 90 qw1 89 we6 88
        int zadd(std::string &key, double score, std::string &member);

        bool zaddWithExpire(std::string &key, int64_t expire);

        // zrange user:rank 0 -1
        std::vector<std::string> zrange(std::string& key, int64_t start, int64_t end);

        std::optional<double> zscore(std::string &key, std::string &member);

        int zremove(std::string &key, std::vector<std::string> &members);

    private:
        size_t ZSetVectorThreshold = 128;

        static int64_t nowMs();

        static bool isExpiredOfString(StringRecord &record, int64_t now_ms);

        static bool isExpiredOfHash(HashRecord &record, int64_t now_ms);

        static bool isExpiredOfZSet(ZSetRecord &record, int64_t now_ms);

        void clearIfExpiredOfString(std::string& key, int64_t now_ms);

        void clearIfExpiredOfHash(std::string& key, int64_t now_ms);

        void clearIfExpiredOfZSet(std::string& key, int64_t now_ms);
    };
}