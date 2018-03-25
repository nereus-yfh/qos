#ifndef INCLUDE_QOS_KERNAL_H
#define INCLUDE_QOS_KERNAL_H

#include <atomic>
#include <string>
#include <thread>
#include <unordered_map>

#include "Configure.h"

namespace mixer {
namespace qos {

enum EnumQosStatus {
    QOS_E_SUCC = 0,
    QOS_E_ERR = -1,
};

enum EnumQosTokenType {
    QOS_TOKEN_ERR = -1,
    QOS_TOKEN_GREEN = 1,
    QOS_TOKEN_YELLOW,
    QOS_TOKEN_ORANGE,
    QOS_TOKEN_RED,
};

class QosKernal;
class Qos {
public:
    Qos();
    ~Qos() = default;
public:
    int init(const comcfg::Configure &config);
    int init(const std::string path, const std::string file_name);
    int get_token(const std::string &user_name, const std::string &product, int64_t token_num);
    std::string dump();

private:
    std::shared_ptr<QosKernal> _qos_kernal;
};

class QosKernal : public std::enable_shared_from_this<QosKernal> {
public:
    QosKernal();
    ~QosKernal();

public:
    int init(const comcfg::Configure &config);
    int get_token(const std::string &user_name, const std::string &product, int64_t token_num);
    static int add_token(std::weak_ptr<QosKernal> wp);
    static int adjust_idle_token(std::weak_ptr<QosKernal> wp);
    int thread_run();
    std::string dump();

private:
    inline std::string make_key(const std::string &user_name, const std::string &product) {
        return user_name + "\t" + product;
    }
private:
    struct UserNode {
        std::string user_name;
        std::string product;

        int64_t qos_limit;
        std::atomic<int64_t> c_bucket;
        std::atomic<int64_t> e_bucket;
        std::atomic<bool> running;
    };

    int64_t _total_qos;
    std::unordered_map<std::string, std::unique_ptr<UserNode >> _user_map;
    bool _add_token_thread_running;
    std::thread _add_token_thread;

    bool _use_idle_flag;
    bool _idle_adjust_thread_running;
    std::thread _idle_adjust_thread;
    std::atomic<int64_t> _idle_bucket;
    std::atomic<int64_t> _idle_qos_limit;

    std::weak_ptr<QosKernal> _wp;
};

} // namespace qos
} // namespace mixer










#endif  //__QOS_KERNAL_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
