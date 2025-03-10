/**
Copyright 2018 JasmineGraph Team
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

#include "StreamingTriangles.h"
#include <algorithm>
#include <vector>
#include <future>
#include <sstream>

#include "../../../util/logger/Logger.h"

Logger streaming_triangle_logger;
std::map<long, std::unordered_set<long>> StreamingTriangles::localAdjacencyList;
std::map<std::string, std::map<long, std::unordered_set<long>>> StreamingTriangles::centralAdjacencyList;

TriangleResult StreamingTriangles::countTriangles(NodeManager* nodeManager, bool returnTriangles) {
    std::map<long, std::unordered_set<long>> adjacencyList = nodeManager->getAdjacencyList();
    std::map<long, long> distributionMap = nodeManager->getDistributionMap();

    const TriangleResult &result = Triangles::countTriangles(adjacencyList, distributionMap, returnTriangles);

    return result;
}

NativeStoreTriangleResult StreamingTriangles::countLocalStreamingTriangles(
        JasmineGraphIncrementalLocalStore *incrementalLocalStoreInstance) {
    streaming_triangle_logger.info("###STREAMING TRIANGLE### Static Streaming Local Triangle Counting: Started");
    const TriangleResult &result = countTriangles(incrementalLocalStoreInstance->nm, false);
    long triangleCount = result.count;

    NodeManager* nodeManager = incrementalLocalStoreInstance->nm;
    std::string graphID = std::to_string(nodeManager->getGraphID());
    std::string partitionID = std::to_string(nodeManager->getPartitionID());


    const std::string& dbPrefix = nodeManager->getDbPrefix();
    long localRelationCount = nodeManager->dbSize(dbPrefix + "_relations.db") / RelationBlock::BLOCK_SIZE - 1;
    long centralRelationCount = nodeManager->dbSize(dbPrefix +
            "_central_relations.db") / RelationBlock::CENTRAL_BLOCK_SIZE - 1;

    NativeStoreTriangleResult nativeStoreTriangleResult{localRelationCount, centralRelationCount, triangleCount};

    streaming_triangle_logger.info("###STREAMING TRIANGLE### Static Streaming Local Triangle Counting: Completed: " +
                                        std::to_string(triangleCount));
    return nativeStoreTriangleResult;
}

std::string StreamingTriangles::countCentralStoreStreamingTriangles(std::string graphId,
                                                                    std::vector<std::string> partitionIdList) {
    streaming_triangle_logger.info("###STREAMING TRIANGLE### Static Streaming Central Triangle "
                                   "Counting: Started");
    std::map<long, std::unordered_set<long>> adjacencyList;
    std::map<long, long> degreeMap;
    std::vector<std::future<std::map<long, std::unordered_set<long>>>> adjacencyListResponse;

    std::vector<std::string>::iterator partitionIdListIterator;

    for (partitionIdListIterator = partitionIdList.begin(); partitionIdListIterator != partitionIdList.end();
         ++partitionIdListIterator) {
        std::string aggregatePartitionId = *partitionIdListIterator;

        adjacencyListResponse.push_back(std::async(std::launch::async, StreamingTriangles::getCentralAdjacencyList,
                                                   std::stoi(graphId), std::stoi(aggregatePartitionId)));
    }

    for (auto&& futureCall : adjacencyListResponse) {
        // Merge adjacency lists
        const std::map<long, std::unordered_set<long>>& currentAdjacencyList = futureCall.get();
        for (const auto& entry : currentAdjacencyList) {
            adjacencyList[entry.first].insert(entry.second.begin(), entry.second.end());
        }
    }

    for (auto& it : adjacencyList) {
        degreeMap.emplace(it.first, it.second.size());
    }

    const TriangleResult &result = Triangles::countTriangles(adjacencyList, degreeMap, true);
    streaming_triangle_logger.info("###STREAMING TRIANGLE### Static Streaming Central Triangle Counting: "
                                   "Completed");
    return result.triangles;
}

std::map<long, std::unordered_set<long>> StreamingTriangles::getCentralAdjacencyList(unsigned int graphID,
                                                                                     unsigned int partitionID) {
    unsigned long maxLabel = std::stol(Utils::getJasmineGraphProperty("org.jasminegraph.nativestore.max.label.size"));
    GraphConfig gc{maxLabel, graphID, partitionID, "app"};
    std::unique_ptr<NodeManager> nodeManager(new NodeManager(gc));

    return nodeManager->getAdjacencyList(false);
}

std::vector<std::pair<long, long>> StreamingTriangles::getEdges(unsigned int graphID, unsigned int partitionID,
                                                                long previousCentralRelationCount) {
    std::vector<std::pair<long, long>> edges;
    unsigned long maxLabel = std::stol(Utils::getJasmineGraphProperty("org.jasminegraph.nativestore.max.label.size"));
    GraphConfig gc{maxLabel, graphID, partitionID, "app"};
    std::unique_ptr<NodeManager> nodeManager(new NodeManager(gc));

    const std::string& dbPrefix = nodeManager->getDbPrefix();
    int relationBlockSize = RelationBlock::CENTRAL_BLOCK_SIZE;

    long newCentralRelationCount = nodeManager->dbSize(dbPrefix + "_central_relations.db") / relationBlockSize - 1;
    streaming_triangle_logger.debug("Found current central relation count " +
                                    std::to_string(newCentralRelationCount));

    for (int i = previousCentralRelationCount + 1; i <= newCentralRelationCount ; i++) {
        RelationBlock* relationBlock = RelationBlock::getCentralRelation(i*relationBlockSize);
        long source = std::stol(relationBlock->getSource()->id);
        long target = std::stol(relationBlock->getDestination()->id);
        edges.push_back(std::make_pair(source, target));
        edges.push_back(std::make_pair(target, source));
        delete relationBlock;
    }

    return edges;
}

NativeStoreTriangleResult StreamingTriangles::countDynamicLocalTriangles(
        JasmineGraphIncrementalLocalStore *incrementalLocalStoreInstance,
        long oldLocalRelationCount, long oldCentralRelationCount) {
    streaming_triangle_logger.info("###STREAMING TRIANGLE### Dynamic Streaming Local Triangle "
                                  "Counting: Started");
    NodeManager* nodeManager = incrementalLocalStoreInstance->nm;
    std::vector<std::pair<long, long>> edges;
    std::map<long, std::unordered_set<long>> newAdjacencyList;

    streaming_triangle_logger.debug("got previous count " + std::to_string(oldLocalRelationCount) + " " +
                                  std::to_string(oldCentralRelationCount));

    const std::string& dbPrefix = nodeManager->getDbPrefix();
    int relationBlockSize = RelationBlock::BLOCK_SIZE;
    int centralRelationBlockSize = RelationBlock::CENTRAL_BLOCK_SIZE;

    long newLocalRelationCount = nodeManager->dbSize(dbPrefix + "_relations.db") / relationBlockSize - 1;
    long newCentralRelationCount = nodeManager->dbSize(dbPrefix +
            "_central_relations.db") / centralRelationBlockSize - 1;
    streaming_triangle_logger.debug("got relation count " + std::to_string(newLocalRelationCount) + " " +
                                  std::to_string(newCentralRelationCount));

    if ((oldLocalRelationCount == newLocalRelationCount) && (oldCentralRelationCount == newCentralRelationCount)) {
        NativeStoreTriangleResult nativeStoreTriangleResult{newLocalRelationCount,
                                                            newCentralRelationCount, 0};
        streaming_triangle_logger.info("###STREAMING TRIANGLE### Dynamic Streaming Local Triangle "
                                       "Counting: Completed : 0");
        return  nativeStoreTriangleResult;
    }

    for (int i = oldLocalRelationCount + 1; i <= newLocalRelationCount; i++) {
        RelationBlock* relationBlock = RelationBlock::getLocalRelation(i*relationBlockSize);
        long sourceNode = std::stol(relationBlock->getSource()->id);
        long targetNode = std::stol(relationBlock->getDestination()->id);
        edges.push_back(std::make_pair(sourceNode, targetNode));
        edges.push_back(std::make_pair(targetNode, sourceNode));
        newAdjacencyList[sourceNode].insert(targetNode);
        newAdjacencyList[targetNode].insert(sourceNode);
        localAdjacencyList[sourceNode].insert(targetNode);
        localAdjacencyList[targetNode].insert(sourceNode);
        delete relationBlock;
    }

    for (int i = oldCentralRelationCount + 1; i <= newCentralRelationCount ; i++) {
        RelationBlock* relationBlock = RelationBlock::getCentralRelation(i*centralRelationBlockSize);
        long sourceNode = std::stol(relationBlock->getSource()->id);
        long targetNode = std::stol(relationBlock->getDestination()->id);
        edges.push_back(std::make_pair(sourceNode, targetNode));
        edges.push_back(std::make_pair(targetNode, sourceNode));
        newAdjacencyList[sourceNode].insert(targetNode);
        newAdjacencyList[targetNode].insert(sourceNode);
        localAdjacencyList[sourceNode].insert(targetNode);
        localAdjacencyList[targetNode].insert(sourceNode);
        delete relationBlock;
    }

    std::map<long, std::unordered_set<long>> adjacencyList = localAdjacencyList;

    long trianglesValue = totalCount(adjacencyList, newAdjacencyList, edges);

    NativeStoreTriangleResult nativeStoreTriangleResult{newLocalRelationCount, newCentralRelationCount,
                                                        trianglesValue};
    streaming_triangle_logger.info("###STREAMING TRIANGLE### Dynamic Streaming Local Triangle "
                                  "Counting: Completed : " + std::to_string(trianglesValue));
    return  nativeStoreTriangleResult;
}

std::string StreamingTriangles::countDynamicCentralTriangles(
        std::string graphId, std::vector<std::string>& partitionIdList,
        std::vector<std::string>& oldCentralRelationCount) {
    streaming_triangle_logger.info("###STREAMING TRIANGLE### Dynamic Streaming Central Triangle "
                                  "Counting: Started");
    std::string joinedString;
    for (std::string& partitionId : partitionIdList) {
        joinedString += partitionId;
    }

    std::map<long, std::unordered_set<long>> adjacencyList;
    std::vector<std::pair<long, long>> edges;
    int position = 0;
    std::vector<std::future<std::map<long, std::unordered_set<long>>>> adjacencyListResponse;
    std::vector<std::future<std::vector<std::pair<long, long>>>> edgeMapResponse;
    std::vector<std::string>::iterator partitionIdListIterator;

    for (partitionIdListIterator = partitionIdList.begin(); partitionIdListIterator != partitionIdList.end();
         ++partitionIdListIterator) {
        std::string aggregatePartitionId = *partitionIdListIterator;

        long previousCentralRelationCount = std::stol(oldCentralRelationCount[position]);
        position++;
        streaming_triangle_logger.debug("got previous central count " +
                                      std::to_string(previousCentralRelationCount));

        edgeMapResponse.push_back(std::async(std::launch::async, StreamingTriangles::getEdges, std::stoi(graphId),
                                                   std::stoi(aggregatePartitionId), previousCentralRelationCount));
    }

    for (auto &&futureCall : edgeMapResponse) {
        // Merge degree maps
        const std::vector<std::pair<long, long>>& edgeMap = futureCall.get();
        for (const auto& entry : edgeMap) {
            long sourceNode = entry.first;
            long targetNode = entry.second;
            edges.push_back(entry);
            centralAdjacencyList[joinedString][sourceNode].insert(targetNode);
            centralAdjacencyList[joinedString][targetNode].insert(sourceNode);
        }
    }

    adjacencyList = centralAdjacencyList[joinedString];
    std::basic_ostringstream<char> triangleStream;
    long count = 0;

    for (const auto& edge : edges) {
        long u = edge.first;
        long v = edge.second;

        for (auto w : adjacencyList[u]) {
            if (adjacencyList[v].find(w) != adjacencyList[v].end()) {
                    long varOne = u;
                    long varTwo = v;
                    long varThree = w;
                    if (varOne > varTwo) {  // swap
                        varOne ^= varTwo;
                        varTwo ^= varOne;
                        varOne ^= varTwo;
                    }
                    if (varOne > varThree) {  // swap
                        varOne ^= varThree;
                        varThree ^= varOne;
                        varOne ^= varThree;
                    }
                    if (varTwo > varThree) {  // swap
                        varTwo ^= varThree;
                        varThree ^= varTwo;
                        varTwo ^= varThree;
                    }
                    triangleStream << varOne << "," << varTwo << "," << varThree << ":";
                    count++;
            }
        }
    }
    streaming_triangle_logger.info("###STREAMING TRIANGLE### Dynamic Streaming Central Triangle "
                                  "Counting: Finished");
    string triangle = triangleStream.str();
    if (!triangle.empty()) {
        triangle.erase(triangle.size() - 1);
    }
    return triangle;
}

long StreamingTriangles::count(const std::map<long, std::unordered_set<long>>& g1,
                               const std::map<long, std::unordered_set<long>>& g2,
                               const std::vector<std::pair<long, long>>& edges) {
    long total_count = 0;

    for (const auto& edge : edges) {
        long u = edge.first;
        long v = edge.second;
        long count = 0;

        for (long w : g1.at(u)) {
            if (g2.at(v).find(w) != g2.at(v).end()) {
                count++;
            }
        }

        total_count += count;
    }

    return total_count;
}

long StreamingTriangles::totalCount(const std::map<long, std::unordered_set<long>>& g1,
                std::map<long, std::unordered_set<long>>& g2,
                std::vector<std::pair<long, long>>& edges) {
    std::vector<std::future<long>> countResponse;
    countResponse.push_back(std::async(std::launch::async, StreamingTriangles::count,
                                       std::ref(g1), std::ref(g1), std::ref(edges)));
    countResponse.push_back(std::async(std::launch::async, StreamingTriangles::count,
                                       std::ref(g1), std::ref(g2), std::ref(edges)));
    countResponse.push_back(std::async(std::launch::async, StreamingTriangles::count,
                                       std::ref(g2), std::ref(g2), std::ref(edges)));
    long s3 = countResponse[2].get();
    long s2 = countResponse[1].get();
    long s1 = countResponse[0].get();

    return 0.5 * ((s1 - s2) + (s3 / 3));
}
