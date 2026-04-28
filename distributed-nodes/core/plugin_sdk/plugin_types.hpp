#pragma once


#include <caf/type_id.hpp>

// 消息类型定义必须在主程序和插件都可见的头文件中，且不使用虚函数
struct PluginInfo {
    std::string name;
    std::string version;
    std::string author;
    std::string description;
};

struct Ping {
    std::string payload;
};
struct Pong {
    std::string reply;
};

struct PluginMessage {
    int id; // 消息ID
    std::string payload; // 消息内容, CAF 知道如何序列化 std::string
};

// 类型起始ID，应该在某个地方统一规划，避免与其他类型冲突
CAF_BEGIN_TYPE_ID_BLOCK(plugin_types, 1000)
    CAF_ADD_TYPE_ID(plugin_types, PluginInfo)
    CAF_ADD_TYPE_ID(plugin_types, PluginMessage)
    CAF_ADD_TYPE_ID(plugin_types, Ping)
    CAF_ADD_TYPE_ID(plugin_types, Pong)
    
    CAF_ADD_ATOM(plugin_types, shutdown_actor_atom)
    
CAF_END_TYPE_ID_BLOCK(plugin_types)

// 示例
// self->send(other_actor, PluginMessage{42, "Hello from plugin!"});
