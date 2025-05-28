

#ifndef STATUS_MESSAGE_H
#define STATUS_MESSAGE_H

#include <string>

enum class MessageType {
    PROGRESS,
    SUCCESS,
    ERROR
};

struct StatusMessage {
    int workerId;
    MessageType type;
    std::string message;

    // Convert to a string
    std::string toString() const;

    // Build from a string
    static StatusMessage fromString(const std::string& str);

private:
    static std::string messageTypeToString(MessageType type);
    static MessageType stringToMessageType(const std::string& str);
};


#endif // STATUS_MESSAGE_H
