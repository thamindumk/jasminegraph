#ifndef STATUS_BUFFER_H
#define STATUS_BUFFER_H

#include <queue>
#include <mutex>
#include <condition_variable>
#include "StatusMessage.h"
#include <unistd.h>
class InstanceHandler;
class StatusBuffer {
 public:
    std::queue<StatusMessage> buffer;
    std::mutex mtx;
    std::condition_variable cv;
    void push(const StatusMessage& msg);
    StatusMessage pop();
    bool isEmpty();
    void listenStatusNotification(int connFd);
    void sendStatusNotification(int connFd, bool *loop_exit_p, InstanceHandler *instanceHandler);
};

#endif // STATUS_BUFFER_H
