/*
 * Copyright 2021 JasminGraph Team
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "./DataPublisher.h"

#include <pthread.h>

#include "../server/JasmineGraphInstanceProtocol.h"
#include "../util/Utils.h"
#include "../util/logger/Logger.h"
# include <thread>
Logger data_publisher_logger;

DataPublisher::DataPublisher(int worker_port, std::string worker_address) {
    this->worker_port = worker_port;
    this->worker_address = worker_address;
    struct hostent *server;

    server = gethostbyname(worker_address.c_str());
    if (server == NULL) {
        data_publisher_logger.error("ERROR, no host named " + worker_address);
        pthread_exit(NULL);
    }

    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(worker_port);
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        data_publisher_logger.error("Socket creation error!");
    }
    if (Utils::connect_wrapper(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        data_publisher_logger.error("Connection Failed!");
    }
}

DataPublisher::~DataPublisher() {
    Utils::send_str_wrapper(sock, JasmineGraphInstanceProtocol::CLOSE);
    close(sock);
}

void DataPublisher::publish(std::string message) {
    char receiver_buffer[MAX_STREAMING_DATA_LENGTH] = {0};

    send(this->sock, JasmineGraphInstanceProtocol::GRAPH_STREAM_START.c_str(),
         JasmineGraphInstanceProtocol::GRAPH_STREAM_START.length(), 0);

    char start_ack[ACK_MESSAGE_SIZE] = {0};
    auto ack_return_status = recv(this->sock, &start_ack, sizeof(start_ack), 0);
    std::string ack(start_ack);
    if (JasmineGraphInstanceProtocol::GRAPH_STREAM_START_ACK != ack) {
        data_publisher_logger.error("Error while receiving start command ack\n");
    }

    int message_length = message.length();
    int converted_number = htonl(message_length);
    data_publisher_logger.debug("Sending content length\n");
    send(this->sock, &converted_number, sizeof(converted_number), 0);

    int received_int = 0;
    data_publisher_logger.debug("Waiting for content length ack\n");
    auto return_status = recv(this->sock, &received_int, sizeof(received_int), 0);

    if (return_status > 0) {
        data_publisher_logger.debug("Received int =" + std::to_string(ntohl(received_int)));
    } else {
        data_publisher_logger.error("Error while receiving content length ack\n");
    }
    send(this->sock, message.c_str(), message.length(), 0);
    data_publisher_logger.debug("Edge data sent\n");
    char CRLF;
    do {
        auto return_status = recv(this->sock, &CRLF, sizeof(CRLF), 0);
        if (return_status < 1) {
            return;
        }
        if (CRLF == '\r') {
            auto return_status = recv(this->sock, &CRLF, sizeof(CRLF), 0);
            if (return_status < 1) {
                return;
            }
            if (CRLF == '\n') {
                break;
            }
        }
    } while (true);
}


void DataPublisher::queryPublish(std::string graphId, std::string partitionId, std::string message) {
    char receiver_buffer[MAX_STREAMING_DATA_LENGTH] = {0};

    send(this->sock, JasmineGraphInstanceProtocol::QUERY_START.c_str(),
         JasmineGraphInstanceProtocol::QUERY_START.length(), 0);

    char start_ack[ACK_MESSAGE_SIZE] = {0};
    auto ack_return_status = recv(this->sock, &start_ack, sizeof(start_ack), 0);
    std::string ack(start_ack);
    if (JasmineGraphInstanceProtocol::QUERY_START_ACK != ack) {
        data_publisher_logger.error("Error while receiving start command ack");
    }

    int message_length = graphId.length();
    int converted_number = htonl(message_length);
    data_publisher_logger.info("Sending content length: "+to_string(converted_number));
    send(this->sock, &converted_number, sizeof(converted_number), 0);

    int received_int = 0;
    data_publisher_logger.info("Waiting for content length ack");
    auto return_status = recv(this->sock, &received_int, sizeof(received_int), 0);

    if (return_status > 0) {
        data_publisher_logger.info("Received int =" + std::to_string(ntohl(received_int)));
    } else {
        data_publisher_logger.error("Error while receiving content length ack");
    }

    send(this->sock, graphId.c_str(), graphId.length(), 0);

    message_length = partitionId.length();
    converted_number = htonl(message_length);
    data_publisher_logger.info("Sending content length"+to_string(converted_number));
    send(this->sock, &converted_number, sizeof(converted_number), 0);

    received_int = 0;
    data_publisher_logger.info("Waiting for content length ack");
    return_status = recv(this->sock, &received_int, sizeof(received_int), 0);

    if (return_status > 0) {
        data_publisher_logger.info("Received int =" + std::to_string(ntohl(received_int)));
    } else {
        data_publisher_logger.error("Error while receiving content length ack");
    }
    send(this->sock, partitionId.c_str(), partitionId.length(), 0);

    message_length = message.length();
    converted_number = htonl(message_length);
    data_publisher_logger.info("Sending content length "+to_string(converted_number));
    send(this->sock, &converted_number, sizeof(converted_number), 0);

    received_int = 0;
    data_publisher_logger.info("Waiting for content length ack");
    return_status = recv(this->sock, &received_int, sizeof(received_int), 0);

    if (return_status > 0) {
        data_publisher_logger.info("Received int =" + std::to_string(ntohl(received_int)));
    } else {
        data_publisher_logger.error("Error while receiving content length ack");
    }

    send(this->sock, message.c_str(), message.length(), 0);
    data_publisher_logger.info("Query sent successfully: " + message);

    queryDataReciev();

    char CRLF;
    do {
        auto return_status = recv(this->sock, &CRLF, sizeof(CRLF), 0);
        if (return_status < 1) {
            return;
        }
        if (CRLF == '\r') {
            auto return_status = recv(this->sock, &CRLF, sizeof(CRLF), 0);
            if (return_status < 1) {
                return;
            }
            if (CRLF == '\n') {
                break;
            }
        }
    } while (true);
}

void DataPublisher::queryDataReciev() {
    for(int i=0; i<200;i++) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        char start[ACK_MESSAGE_SIZE] = {0};
        recv(this->sock, &start, sizeof(start), 0);
        std::string start_msg(start);
        if (JasmineGraphInstanceProtocol::QUERY_DATA_START != start_msg) {
            data_publisher_logger.error("Error while receiving start command ack : "+ start_msg);
            continue;
        }
        data_publisher_logger.info("YYYYYYYY");
        data_publisher_logger.info(start);
        std::string ack(JasmineGraphInstanceProtocol::QUERY_DATA_ACK);
        send(this->sock, ack.c_str(),
             JasmineGraphInstanceProtocol::QUERY_DATA_ACK.length(), 0);

        int content_length;
        ssize_t return_status = recv(this->sock, &content_length, sizeof(int), 0);
        if (return_status > 0) {
            content_length = ntohl(content_length);
            data_publisher_logger.info("Received int =" + std::to_string(content_length));
        } else {
            data_publisher_logger.error("Error while receiving content length");
            return;
        }
        data_publisher_logger.info("LENGTH : "+to_string(content_length));
        send(this->sock, ack.c_str(),
             JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK.length(), 0);

        std::string data(content_length, 0);
        return_status = recv(this->sock, &data[0], content_length, 0);
        if (return_status > 0) {
            data_publisher_logger.info("Received graph data."+ data);
            send(this->sock, ack.c_str(),
                 JasmineGraphInstanceProtocol::GRAPH_DATA_SUCCESS.length(), 0);
        } else {
            data_publisher_logger.info("Error while reading graph data");
            return;
        }
        if(data != "-1"){
         break;
        }
    }
}
