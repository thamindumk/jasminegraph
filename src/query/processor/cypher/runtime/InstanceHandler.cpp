//
// Created by kumarawansha on 12/13/24.
//

#include "InstanceHandler.h"
#include "../../../../server/JasmineGraphInstanceProtocol.h"
static Logger instanceHandlerLogger;
std::unordered_map<int, std::mutex> InstanceHandler::connectionLocks;
std::mutex InstanceHandler::mapMutex;

InstanceHandler::InstanceHandler(std::map<std::string,
        JasmineGraphIncrementalLocalStore*>& incrementalLocalStoreMap)
        : incrementalLocalStoreMap(incrementalLocalStoreMap) { };


void InstanceHandler::handleRequest(int connFd, bool *loop_exit_p,
                                    GraphConfig gc, string masterIP,
                                    std::string queryJson){
    OperatorExecutor operatorExecutor(gc, queryJson, masterIP);
    operatorExecutor.initializeMethodMap();
    SharedBuffer sharedBuffer(5);
    StatusBuffer statusBuffer;
    auto method = OperatorExecutor::methodMap[operatorExecutor.query["Operator"]];
    std::thread result(method, std::ref(operatorExecutor), std::ref(sharedBuffer),
                       std::string(operatorExecutor.queryPlan), gc, std::ref(statusBuffer));
    std::thread notification([&statusBuffer, connFd, loop_exit_p, this]() {
        statusBuffer.sendStatusNotification(connFd, loop_exit_p, this);
    });
    while(true) {
        string raw = sharedBuffer.get();
        if(raw == "-1"){
            this->dataPublishToMaster(connFd, loop_exit_p, raw);
            result.join();
            notification.join();
            break;
        }
        this->dataPublishToMaster(connFd, loop_exit_p, raw);
    }
}

void InstanceHandler::dataPublishToMaster(int connFd, bool *loop_exit_p, std::string message) {
    std::mutex* connMutex;
    {
        std::lock_guard<std::mutex> lock(mapMutex);
        connMutex = &connectionLocks[connFd]; // creates mutex if not exist
    }

    std::lock_guard<std::mutex> connLock(*connMutex);

    if (!Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::QUERY_DATA_START)) {
        *loop_exit_p = true;
        return;
    }

    std::string start_ack(JasmineGraphInstanceProtocol::QUERY_DATA_ACK.length(), 0);
    int return_status = recv(connFd, &start_ack[0], JasmineGraphInstanceProtocol::QUERY_DATA_ACK.length(), 0);
    if (return_status > 0) {
        instanceHandlerLogger.info("Received data start ack: "+start_ack);
    } else {
        instanceHandlerLogger.info("Error while reading start ack");
        *loop_exit_p = true;
        return;
    }

    int message_length = message.length();
    int converted_number = htonl(message_length);
    instanceHandlerLogger.info("Sending content length"+to_string(converted_number));
    if(!Utils::send_int_wrapper(connFd, &converted_number, sizeof(converted_number))){
        *loop_exit_p = true;
        return;
    }

    std::string length_ack(JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK.length(), 0);
    return_status = recv(connFd, &length_ack[0], JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK.length(), 0);
    if (return_status > 0) {
        instanceHandlerLogger.info("Received content length ack: "+length_ack);
    } else {
        instanceHandlerLogger.info("Error while reading content length ack");
        *loop_exit_p = true;
        return;
    }

    if (!Utils::send_str_wrapper(connFd, message)) {
        *loop_exit_p = true;
        return;
    }

    std::string success_ack(JasmineGraphInstanceProtocol::GRAPH_DATA_SUCCESS.length(), 0);
    return_status = recv(connFd, &success_ack[0], JasmineGraphInstanceProtocol::GRAPH_DATA_SUCCESS.length(), 0);
    if (return_status > 0) {
        instanceHandlerLogger.info("Received success ack: "+ success_ack);
    } else {
        instanceHandlerLogger.info("Error while reading content length ack");
        *loop_exit_p = true;
        return;
    }

}
