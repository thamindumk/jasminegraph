//
// Created by kumarawansha on 4/27/25.
//

#include "StatusMessage.h"
#include <sstream>
#include <stdexcept>

std::string StatusMessage::toString() const {
    std::ostringstream oss;
    oss << workerId << "|" << messageTypeToString(type) << "|" << message;
    return oss.str();
}

StatusMessage StatusMessage::fromString(const std::string& str) {
    StatusMessage statusMessage;
    std::istringstream iss(str);
    std::string typeStr;

    std::string workerIdStr;
    std::getline(iss, workerIdStr, '|');
    std::getline(iss, typeStr, '|');
    std::getline(iss, statusMessage.message);

    statusMessage.workerId = std::stoi(workerIdStr);
    statusMessage.type = stringToMessageType(typeStr);

    return statusMessage;
}

std::string StatusMessage::messageTypeToString(MessageType type) {
    switch (type) {
        case MessageType::PROGRESS: return "PROGRESS";
        case MessageType::SUCCESS: return "SUCCESS";
        case MessageType::ERROR: return "ERROR";
        default: return "UNKNOWN";
    }
}

MessageType StatusMessage::stringToMessageType(const std::string& str) {
    if (str == "PROGRESS") return MessageType::PROGRESS;
    if (str == "SUCCESS") return MessageType::SUCCESS;
    if (str == "ERROR") return MessageType::ERROR;
    throw std::invalid_argument("Invalid MessageType string: " + str);
}