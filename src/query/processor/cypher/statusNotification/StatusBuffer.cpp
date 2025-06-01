//
// Created by kumarawansha on 4/27/25.
//

#include "StatusBuffer.h"
#include "../../../../util/logger/Logger.h"
#include "../runtime/InstanceHandler.h"

Logger notificationLogger;
void StatusBuffer::push(const StatusMessage &msg) {
    std::unique_lock<std::mutex> lock(mtx);
    buffer.push(msg);
    cv.notify_one();
}

StatusMessage StatusBuffer::pop() {
    std::unique_lock<std::mutex> lock(mtx);
    cv.wait(lock, [this]() { return !buffer.empty(); });
    StatusMessage msg = buffer.front();
    buffer.pop();
    return msg;
}

bool StatusBuffer::isEmpty() {
    std::unique_lock<std::mutex> lock(mtx);
    return buffer.empty();
}

void StatusBuffer::listenStatusNotification(int connFd){
    while (true) {
        std::string data;
        if (!this->isEmpty()){
            StatusMessage msg = this->pop();
            data = msg.toString();
            if (msg.message == "-1") {
                break;
            }
            notificationLogger.info("Received status message: " + data);
        }
    }
    return;
}

void StatusBuffer::sendStatusNotification(int connFd, bool *loop_exit_p, InstanceHandler *instanceHandler) {
    while (true) {
        StatusMessage notification = this->pop();
        std::string raw = notification.toString();
        std::string message = notification.message;
        notificationLogger.info("Sending status message: " + message);
        if(message == "-1"){
            instanceHandler->dataPublishToMaster(connFd, loop_exit_p, raw);
            break;
        }
        instanceHandler->dataPublishToMaster(connFd, loop_exit_p, raw);
    }
}