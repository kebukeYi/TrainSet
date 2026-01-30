//
// Created by 19327 on 2025/12/31/星期三.
//

#pragma once

#include <string>
#include <vector>
#include <optional>

namespace train_set {
    enum class RespType {
        SimpleString,// +OK\r\n
        Error,// -ERR message\r\n
        Integer,// :123\r\n
        BulkString,// $6\r\nfoobar\r\n
        Array,// *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
        Null
    };
    struct RespValue {
        RespType type=RespType::Null;
        std::string bulk_string;
        std::vector<RespValue> array;
    };

    class RespParser {
    private:
        std::string m_buffer;
    private:
        bool parse_line(size_t &pos, std::string &out_line);

        bool parse_simple_string(size_t &pos, RespType type, RespValue &value);

        bool parse_bulk_string(size_t &pos, RespValue &value);

        bool parse_array(size_t &pos, RespValue &value);

        bool parse_integer(size_t &pos, int64_t &value);

    public:
        void append(std::string_view data);

        void clear();

        std::optional<RespValue> tryParseOne();

        std::optional<std::pair<RespValue, std::string>> tryParseOneWithRaw();
    };

    std::string respSimpleString(std::string_view s);

    std::string respError(std::string_view s);

    std::string respBulk(std::string_view s);

    std::string respNullBulk();

    std::string respInteger(int64_t v);
}