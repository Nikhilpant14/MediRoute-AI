#include <algorithm>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <limits>
#include <queue>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

using namespace std;

struct NodePoint {
    int id;
    string name;
    double lat;
    double lng;
};

struct Edge {
    int to;
    double distanceKm;
};

struct Ambulance {
    int id;
    int nodeId;
    bool available;
    int busyForMinutes;
};

struct Hospital {
    string name;
    int nodeId;
};

struct DispatchResult {
    bool success;
    string message;
    int ambulanceId;
    string hospitalName;
    double routeToPatientKm;
    double routeToHospitalKm;
    int etaToPatientMinutes;
    int etaToHospitalMinutes;
};

class CityGraph {
public:
    void addNode(const NodePoint& node) {
        nodes[node.id] = node;
    }

    void addBidirectionalEdge(int from, int to, double distanceKm) {
        adjacency[from].push_back({to, distanceKm});
        adjacency[to].push_back({from, distanceKm});
    }

    vector<int> shortestPath(int source, int target, double& totalDistanceKm) const {
        unordered_map<int, double> dist;
        unordered_map<int, int> parent;
        priority_queue<pair<double, int>, vector<pair<double, int>>, greater<>> minHeap;

        for (const auto& entry : nodes) {
            dist[entry.first] = numeric_limits<double>::infinity();
        }

        dist[source] = 0.0;
        minHeap.push({0.0, source});

        while (!minHeap.empty()) {
            auto [currentDistance, currentNode] = minHeap.top();
            minHeap.pop();

            if (currentDistance > dist[currentNode]) {
                continue;
            }

            if (currentNode == target) {
                break;
            }

            auto found = adjacency.find(currentNode);
            if (found == adjacency.end()) {
                continue;
            }

            for (const auto& edge : found->second) {
                double nextDistance = currentDistance + edge.distanceKm;
                if (nextDistance < dist[edge.to]) {
                    dist[edge.to] = nextDistance;
                    parent[edge.to] = currentNode;
                    minHeap.push({nextDistance, edge.to});
                }
            }
        }

        if (!isfinite(dist[target])) {
            totalDistanceKm = numeric_limits<double>::infinity();
            return {};
        }

        totalDistanceKm = dist[target];
        vector<int> path;
        for (int node = target; ; node = parent[node]) {
            path.push_back(node);
            if (node == source) {
                break;
            }
        }
        reverse(path.begin(), path.end());
        return path;
    }

    const NodePoint* getNode(int nodeId) const {
        auto found = nodes.find(nodeId);
        if (found == nodes.end()) {
            return nullptr;
        }
        return &found->second;
    }

private:
    unordered_map<int, NodePoint> nodes;
    unordered_map<int, vector<Edge>> adjacency;
};

class DispatchEngine {
public:
    explicit DispatchEngine(const CityGraph& graph)
        : graph(graph) {}

    void addAmbulance(const Ambulance& ambulance) {
        ambulances.push_back(ambulance);
    }

    void addHospital(const Hospital& hospital) {
        hospitals.push_back(hospital);
    }

    DispatchResult dispatchRequest(int patientNodeId) {
        int bestIndex = -1;
        double bestDistanceKm = numeric_limits<double>::infinity();
        double secondBestDistanceKm = numeric_limits<double>::infinity();

        for (int index = 0; index < static_cast<int>(ambulances.size()); ++index) {
            if (!ambulances[index].available) {
                continue;
            }

            double routeDistanceKm = 0.0;
            graph.shortestPath(ambulances[index].nodeId, patientNodeId, routeDistanceKm);
            if (!isfinite(routeDistanceKm)) {
                continue;
            }

            if (routeDistanceKm < bestDistanceKm) {
                secondBestDistanceKm = bestDistanceKm;
                bestDistanceKm = routeDistanceKm;
                bestIndex = index;
            } else if (routeDistanceKm < secondBestDistanceKm) {
                secondBestDistanceKm = routeDistanceKm;
            }
        }

        if (bestIndex == -1) {
            return {
                false,
                "All ambulances are busy right now.",
                -1,
                "",
                0.0,
                0.0,
                0,
                0
            };
        }

        auto hospitalResult = nearestHospital(patientNodeId);
        ambulances[bestIndex].available = false;
        ambulances[bestIndex].busyForMinutes =
            estimateEtaMinutes(bestDistanceKm) + estimateEtaMinutes(hospitalResult.first);
        ambulances[bestIndex].nodeId = hospitalResult.second;

        string message = "Assigned nearest available ambulance.";
        if (isfinite(secondBestDistanceKm) && secondBestDistanceKm < bestDistanceKm + 0.0001) {
            message = "Assigned one of the equally short ambulances.";
        }

        return {
            true,
            message,
            ambulances[bestIndex].id,
            hospitalNameForNode(hospitalResult.second),
            bestDistanceKm,
            hospitalResult.first,
            estimateEtaMinutes(bestDistanceKm),
            estimateEtaMinutes(hospitalResult.first)
        };
    }

    void tickMinute() {
        for (auto& ambulance : ambulances) {
            if (!ambulance.available && ambulance.busyForMinutes > 0) {
                ambulance.busyForMinutes -= 1;
                if (ambulance.busyForMinutes <= 0) {
                    ambulance.available = true;
                    ambulance.busyForMinutes = 0;
                }
            }
        }
    }

    void printFleet() const {
        cout << "\nCurrent Ambulance Fleet\n";
        cout << "-----------------------\n";
        for (const auto& ambulance : ambulances) {
            cout << "Unit #" << ambulance.id
                 << " | Node " << ambulance.nodeId
                 << " | " << (ambulance.available ? "Available" : "Busy")
                 << " | Remaining busy time: " << ambulance.busyForMinutes << " min\n";
        }
    }

private:
    const CityGraph& graph;
    vector<Ambulance> ambulances;
    vector<Hospital> hospitals;

    static int estimateEtaMinutes(double distanceKm, double speedKmph = 42.0) {
        if (distanceKm <= 0.0) {
            return 1;
        }
        return max(1, static_cast<int>(ceil((distanceKm / speedKmph) * 60.0)));
    }

    pair<double, int> nearestHospital(int patientNodeId) const {
        double bestDistanceKm = numeric_limits<double>::infinity();
        int bestHospitalNode = -1;

        for (const auto& hospital : hospitals) {
            double routeDistanceKm = 0.0;
            graph.shortestPath(patientNodeId, hospital.nodeId, routeDistanceKm);
            if (routeDistanceKm < bestDistanceKm) {
                bestDistanceKm = routeDistanceKm;
                bestHospitalNode = hospital.nodeId;
            }
        }

        return {bestDistanceKm, bestHospitalNode};
    }

    string hospitalNameForNode(int nodeId) const {
        for (const auto& hospital : hospitals) {
            if (hospital.nodeId == nodeId) {
                return hospital.name;
            }
        }
        return "Unknown Hospital";
    }
};

int main() {
    CityGraph graph;

    graph.addNode({1, "Graphic Era University", 30.2679, 78.0146});
    graph.addNode({2, "Velmed Hospital", 30.2706, 78.0106});
    graph.addNode({3, "Balliwala", 30.3167, 78.0328});
    graph.addNode({4, "Doon Hospital", 30.3250, 78.0430});
    graph.addNode({5, "CMI Hospital", 30.3168, 78.0438});
    graph.addNode({6, "ISBT", 30.2878, 78.0219});
    graph.addNode({7, "Rajpur Road", 30.3494, 78.0678});
    graph.addNode({8, "Max Hospital", 30.3737, 78.0747});

    graph.addBidirectionalEdge(1, 2, 1.2);
    graph.addBidirectionalEdge(1, 6, 3.4);
    graph.addBidirectionalEdge(6, 3, 4.1);
    graph.addBidirectionalEdge(3, 4, 2.2);
    graph.addBidirectionalEdge(3, 5, 1.8);
    graph.addBidirectionalEdge(5, 7, 4.4);
    graph.addBidirectionalEdge(7, 8, 3.0);
    graph.addBidirectionalEdge(2, 6, 2.8);
    graph.addBidirectionalEdge(4, 5, 0.8);

    DispatchEngine engine(graph);
    engine.addAmbulance({1, 2, true, 0});
    engine.addAmbulance({2, 3, true, 0});
    engine.addAmbulance({3, 6, true, 0});
    engine.addAmbulance({4, 7, true, 0});

    engine.addHospital({"Velmed Hospital", 2});
    engine.addHospital({"Doon Hospital", 4});
    engine.addHospital({"CMI Hospital", 5});
    engine.addHospital({"Max Hospital", 8});

    engine.printFleet();

    DispatchResult result = engine.dispatchRequest(1);
    cout << "\nDispatch Result\n";
    cout << "---------------\n";
    cout << boolalpha << "Success: " << result.success << "\n";
    cout << "Message: " << result.message << "\n";
    cout << "Assigned Ambulance: " << result.ambulanceId << "\n";
    cout << "Nearest Hospital: " << result.hospitalName << "\n";
    cout << fixed << setprecision(2);
    cout << "Route To Patient: " << result.routeToPatientKm << " km\n";
    cout << "Route To Hospital: " << result.routeToHospitalKm << " km\n";
    cout << "ETA To Patient: " << result.etaToPatientMinutes << " min\n";
    cout << "ETA To Hospital: " << result.etaToHospitalMinutes << " min\n";

    engine.printFleet();
    return 0;
}
