#pragma once

class graceful_base_state {
    bool shutting_down = false;
public:
    // 自动兜底行为生成器，在每条消息处理后检查是否需要退出，适用于需要优雅关闭的actor
    // 用法：self->become(graceful_behavior(self, shutting_down, ...handlers...)); // 更换行为时调用
    template <class... Handlers>
    behavior graceful_behavior(event_based_actor* self, Handlers&&... handlers) {
        return behavior{
            ([=](auto&&... args) {
                handlers(args...);
                if (shutting_down && self->mailbox().empty()) self->quit();
            })...
        };
    }
};

/*
// 示例：一个继承自 graceful_base_state 的 actor 状态类，可以在其中添加一些公共状态变量和消息处理逻辑
class graceful_actor_state : public graceful_base_state {
    // 这里可以添加一些公共状态变量
    event_based_actor* self_;
    int some_valut_ = 0;
public:
    graceful_actor_state(event_based_actor* self, int some_valut) : self_(self), some_valut_(some_valut) {}
    
    behavior make_behavior() {
        // 这里可以添加一些公共消息处理逻辑
        return graceful_behavior(self_, [](int x) {
            // 这里是公共消息处理器，可以处理一些通用消息
            // 具体消息处理器由子类实现
        }     
        );
    }
};
*/
