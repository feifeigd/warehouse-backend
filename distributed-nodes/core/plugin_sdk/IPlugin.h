
#pragma once


#include <caf/actor.hpp> // 引入 CAF actor 句柄定义
#include <caf/actor_system.hpp> // 引入 CAF actor_system 定义

// 标准库
#include <string>
#include <memory>

// 动态库导出C接口，插件必须实现这些函数
extern "C" {
    caf::
}

// 插件生命周期接口，所有插件必须实现
class IPlugin {
public:
    virtual ~IPlugin() = default;

    // 插件名称
    virtual std::string name() const = 0;

    // 插件初始化，传入CAF actor_system指针
    // system 是进程全局变量，插件可以通过它创建actor或访问系统资源
    virtual bool initialize(caf::actor_system* system) = 0;

    // 插件卸载前回调，做资源清理
    virtual void shutdown() = 0;

    // 获取插件的主actor句柄（用于actor间通信）
    virtual caf::actor main_actor() = 0;

    // 热重载时通知插件（可选实现）
    virtual void on_reload() {}
};
