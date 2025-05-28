/**
Copyright 2025 JasmineGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

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
    void NodeScanByLabel(SharedBuffer &buffer, string jsonPlan, GraphConfig gc, StatusBuffer &statusBuffer);
    void ProduceResult(SharedBuffer &buffer, string jsonPlan, GraphConfig gc, StatusBuffer &statusBuffer);
    void Filter(SharedBuffer &buffer, string jsonPlan, GraphConfig gc, StatusBuffer &statusBuffer);
    void ExpandAll(SharedBuffer &buffer, string jsonPlan, GraphConfig gc, StatusBuffer &statusBuffer);
    void UndirectedRelationshipTypeScan(SharedBuffer &buffer, string jsonPlan, GraphConfig gc, StatusBuffer &statusBuffer);
    void UndirectedAllRelationshipScan(SharedBuffer &buffer, string jsonPlan, GraphConfig gc, StatusBuffer &statusBuffer);
    void DirectedRelationshipTypeScan(SharedBuffer &buffer, string jsonPlan, GraphConfig gc, StatusBuffer &statusBuffer);
    void DirectedAllRelationshipScan(SharedBuffer &buffer, string jsonPlan, GraphConfig gc, StatusBuffer &statusBuffer);
    void NodeByIdSeek(SharedBuffer &buffer, string jsonPlan, GraphConfig gc, StatusBuffer &statusBuffer);
    void AggregationFunction(SharedBuffer &buffer, string jsonPlan, GraphConfig gc, StatusBuffer &statusBuffer);
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
    static const int INTER_OPERATOR_BUFFER_SIZE = 5;
};

#endif  // JASMINEGRAPH_OPERATOREXECUTOR_H
