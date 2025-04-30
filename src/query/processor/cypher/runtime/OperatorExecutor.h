//
// Created by kumarawansha on 1/2/25.
//

#ifndef JASMINEGRAPH_OPERATOREXECUTOR_H
#define JASMINEGRAPH_OPERATOREXECUTOR_H
#include "../../../../nativestore/NodeManager.h"
#include "InstanceHandler.h"
#include "../util/SharedBuffer.h"
#include <string>
#include <vector>
class StatusBuffer;
using namespace  std;

class OperatorExecutor {
 public:
    OperatorExecutor(GraphConfig gc, string queryPlan, string masterIP);
    void AllNodeScan(SharedBuffer &buffer, string jsonPlan, GraphConfig gc, StatusBuffer &statusBuffer);
    void ProduceResult(SharedBuffer &buffer, string jsonPlan, GraphConfig gc, StatusBuffer &statusBuffer);
    void Filter(SharedBuffer &buffer, string jsonPlan, GraphConfig gc, StatusBuffer &statusBuffer);
    void ExpandAll(SharedBuffer &buffer, string jsonPlan, GraphConfig gc, StatusBuffer &statusBuffer);
    void UndirectedRelationshipTypeScan(SharedBuffer &buffer, string jsonPlan, GraphConfig gc, StatusBuffer &statusBuffer);
    void UndirectedAllRelationshipScan(SharedBuffer &buffer, string jsonPlan, GraphConfig gc, StatusBuffer &statusBuffer);
    void NodeByIdSeek(SharedBuffer &buffer, string jsonPlan, GraphConfig gc, StatusBuffer &statusBuffer);
    void EargarAggregation(SharedBuffer &buffer, string jsonPlan, GraphConfig gc, StatusBuffer &statusBuffer);
    void Create(SharedBuffer &buffer, string jsonPlan, GraphConfig gc, StatusBuffer &statusBuffer);
    void CartesianProduct(SharedBuffer &buffer, string jsonPlan, GraphConfig gc, StatusBuffer &statusBuffer);
    void Projection(SharedBuffer &buffer, string jsonPlan, GraphConfig gc, StatusBuffer &statusBuffer);
    void Distinct(SharedBuffer &buffer, string jsonPlan, GraphConfig gc, StatusBuffer &statusBuffer);
    void OrderBy(SharedBuffer &buffer, string jsonPlan, GraphConfig gc, StatusBuffer &statusBuffer);
    string masterIP;
    string  queryPlan;
    GraphConfig gc;
    json query;
    static std::unordered_map<std::string, std::function<void(OperatorExecutor &, SharedBuffer &, std::string,
            GraphConfig, StatusBuffer&)>> methodMap;
    static void initializeMethodMap();
};


#endif //JASMINEGRAPH_OPERATOREXECUTOR_H
