#include <algorithm>
#include <chrono>
#include <sstream>

#include "bvar/bvar.h"
#include "qos_kernal.h"

namespace mixer {
namespace qos {

QosKernal::QosKernal() : _total_qos(0), _add_token_thread_running(false), 
    _use_idle_flag(false), _idle_adjust_thread_running(false),
    _idle_bucket(0), _idle_qos_limit(0)
{
}

QosKernal::~QosKernal() {

    if (_add_token_thread.joinable()) {
        _add_token_thread.detach();
    }
    if (_idle_adjust_thread.joinable()) {
        _idle_adjust_thread.detach();
    }
}

int QosKernal::init(const comcfg::Configure &conf) {
    try {
        const comcfg::ConfigUnit &global_conf = conf["qos"]["global"];
        _total_qos = global_conf["total_qos"].to_int64();
        _use_idle_flag = global_conf["use_idle"].to_int32();
        for (size_t i = 0; i < conf["qos"]["users"].size(); i++) {
            const comcfg::ConfigUnit &user_conf = conf["qos"]["users"][i];
            std::unique_ptr<UserNode> user_node(new UserNode);
            user_node->user_name = user_conf["user_name"].to_cstr();
            user_node->product = user_conf["product"].to_cstr();
            user_node->qos_limit = user_conf["qos_limit"].to_int64();
            user_node->c_bucket.store(user_node->qos_limit);
            user_node->e_bucket.store(user_node->qos_limit);
            user_node->running.store(false);
            
            std::string key = make_key(user_node->user_name, user_node->product);
            _user_map.emplace(key, std::move(user_node));
        }
    } catch (const comcfg::ConfigException& e) {
        LOG(ERROR) << "load conf error, msg[" << e.what() << "]";
        return QOS_E_ERR;
    }

    thread_run();

    return QOS_E_SUCC;
}

int QosKernal::get_token(const std::string &user_name, 
        const std::string &product, int64_t token_num) {
    auto it = _user_map.find(make_key(user_name, product));
    if (it == _user_map.end()) {
        return QOS_TOKEN_ERR;
    }
    UserNode &user_node = *(it->second);
    int64_t token = user_node.c_bucket.fetch_sub(token_num) - token_num;
    int32_t ret = QOS_TOKEN_ERR;
    if (token >= 0) {
        ret = QOS_TOKEN_GREEN;
    } else {
        user_node.c_bucket.fetch_add(token_num);
        token = user_node.e_bucket.fetch_sub(token_num) - token_num;
        if (token >= 0) {
            ret = QOS_TOKEN_YELLOW;
        } else {
            user_node.e_bucket.fetch_add(token_num);
            ret = QOS_TOKEN_RED;
        }
    }
    user_node.running.store(true);
    
    if (_use_idle_flag && ret == QOS_TOKEN_RED) {
        token = _idle_bucket.fetch_sub(token_num) - token_num;
        if (token >= 0) {
            ret = QOS_TOKEN_ORANGE;
        } else {
            _idle_bucket.fetch_add(token_num);
        }
    }
    return ret;
}

std::string QosKernal::dump() {
    std::ostringstream ostr;
    ostr << "qos dump status: " 
        << "total_qos[" << _total_qos << "] "
        << "use_idle[" << _use_idle_flag << "] "
        << "idle_bucket[" << _idle_bucket.load() << "] "
        << "idle_qos[" << _idle_qos_limit << "] ";
    ostr << "users: ";
    for (auto& obj : _user_map) {
        UserNode &user_node = *(obj.second);
        ostr << "(" << user_node.user_name << ","
            << user_node.product << "|" 
            << user_node.qos_limit << "`" 
            << user_node.c_bucket << "`" 
            << user_node.e_bucket << "`" 
            << user_node.running << ")"; 
    }
    return ostr.str();
}

int QosKernal::add_token(std::weak_ptr<QosKernal> wp) {
    while (true) {
        std::shared_ptr<QosKernal> qos_kernal = wp.lock();
        if (!qos_kernal) {
            return QOS_E_SUCC;
        }
        for (auto& obj : qos_kernal->_user_map) {
            UserNode &user_node = *(obj.second);
            int64_t c_bucket_left = user_node.c_bucket.load(std::memory_order_relaxed);
            user_node.c_bucket.store(user_node.qos_limit, std::memory_order_relaxed);
            int64_t e_bucket_left = user_node.e_bucket.load(std::memory_order_relaxed);
            c_bucket_left = c_bucket_left > 0 ? c_bucket_left : 0;
            e_bucket_left = e_bucket_left > 0 ? e_bucket_left : 0;
            user_node.e_bucket.store(std::min(user_node.qos_limit, c_bucket_left + e_bucket_left),
                    std::memory_order_relaxed);
        }
        if (qos_kernal->_use_idle_flag) {
            qos_kernal->_idle_bucket.store(
                    qos_kernal->_idle_qos_limit.load(std::memory_order_relaxed), 
                    std::memory_order_relaxed);
        }
        std::chrono::milliseconds dura(1000);
        std::this_thread::sleep_for(dura);
    }
    return QOS_E_SUCC;
}

int QosKernal::adjust_idle_token(std::weak_ptr<QosKernal> wp) {
    while (true) {
        std::shared_ptr<QosKernal> qos_kernal = wp.lock();
        if (!qos_kernal) {
            return QOS_E_SUCC;
        }
        int64_t qos_using = 0;
        for (auto& obj : qos_kernal->_user_map) {
            UserNode &user_node = *(obj.second);
            if (user_node.running) {
                qos_using += user_node.qos_limit;
            }
            user_node.running = false;
        }
        qos_kernal->_idle_qos_limit.store(std::max(qos_kernal->_total_qos - qos_using, int64_t(0)), 
                std::memory_order_relaxed);
        std::chrono::seconds dura(30);
        std::this_thread::sleep_for(dura);
    }
    return QOS_E_SUCC;
}

int QosKernal::thread_run() {
    std::weak_ptr<QosKernal> wp = shared_from_this();
    if (!_add_token_thread_running) {
        _add_token_thread = std::thread([wp]() {
                add_token(wp); 
                });
    }
    if (_use_idle_flag && !_idle_adjust_thread_running) {
        _idle_adjust_thread = std::thread([wp]() { 
                adjust_idle_token(wp);
                });
    }
    return QOS_E_SUCC;
}

Qos::Qos() {
    _qos_kernal = std::make_shared<QosKernal>();
}

int Qos::init(const comcfg::Configure &config) {
    return _qos_kernal->init(config);
}

int Qos::init(const std::string path, const std::string file_name) {
    comcfg::Configure conf;
    int ret = conf.load(path.c_str(), file_name.c_str());
    
    if (ret != 0) {
        LOG(ERROR) << "load conf error file_name[" << file_name << "]";
        return QOS_E_ERR;
    }
    return _qos_kernal->init(conf);
}

int Qos::get_token(const std::string &user_name, const std::string &product, int64_t token_num) {
    return _qos_kernal->get_token(user_name, product, token_num);
}

std::string Qos::dump() {
    return _qos_kernal->dump();
}

} // namespace qos
} // namespace mixer













/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
