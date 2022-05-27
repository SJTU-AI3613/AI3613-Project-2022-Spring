#pragma once

#include "common/format.h"
#include "common/macros.h"

#include <list>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace naivedb {
/**
 * @brief Graph stores a directed graph.
 *
 * @tparam VID_T type of identifier of each vertex
 */
template <typename VID_T>
class Graph {
    friend struct fmt::formatter<Graph>;
    DISALLOW_COPY(Graph)
  public:
    Graph() = default;

    ~Graph() = default;

    Graph(Graph &&graph)
        : vertex_map_(std::move(graph.vertex_map_))
        , outgoing_neighbors_(std::move(graph.outgoing_neighbors_))
        , incoming_neighbors_(std::move(graph.incoming_neighbors_))
        , free_slots_(std::move(graph.free_slots_)) {}

    Graph &operator=(Graph &&graph) {
        vertex_map_ = std::move(graph.vertex_map_);
        outgoing_neighbors_ = std::move(graph.outgoing_neighbors_);
        incoming_neighbors_ = std::move(graph.incoming_neighbors_);
        free_slots_ = std::move(graph.free_slots_);
        return *this;
    }

    /**
     * @brief Add a vertex to the graph.
     * @warning Make sure the vertex does not exist before invoking this method!
     *
     * @param vertex_id
     * @return Graph&
     */
    Graph &add_vertex(VID_T vertex_id) {
        vertices_.emplace(vertex_id);
        if (!free_slots_.empty()) {
            auto index = free_slots_.front();
            free_slots_.pop_front();
            vertex_map_.emplace(vertex_id, index);
        } else {
            vertex_map_.emplace(vertex_id, outgoing_neighbors_.size());
            outgoing_neighbors_.emplace_back();
            incoming_neighbors_.emplace_back();
        }
        return *this;
    }

    /**
     * @brief Remove a vertex from the graph. This method does nothing if the vertex does not exist.
     *
     * @param vertex_id
     * @return Graph&
     */
    Graph &remove_vertex(VID_T vertex_id) {
        if (auto iter = vertex_map_.find(vertex_id); iter != vertex_map_.end()) {
            auto [_, index] = *iter;
            outgoing_neighbors_[index].clear();
            incoming_neighbors_[index].clear();
            free_slots_.emplace_back(index);
            vertex_map_.erase(iter);
            vertices_.erase(vertex_id);
        }
        return *this;
    }

    /**
     * @brief Check whether the given vertex exists.
     *
     * @param vertex_id
     * @return true
     * @return false
     */
    bool has_vertex(VID_T vertex_id) const { return vertices_.find(vertex_id) != vertices_.end(); }

    /**
     * @brief Get the set of all vertices in the graph.
     *
     * @return const std::unordered_set<VID_T>&
     */
    const std::unordered_set<VID_T> &vertices() const { return vertices_; }

    /**
     * @brief Add an edge to the graph. This method does nothing if the edge exists.
     * @warning Make sure both the source vertex and the destination vertex exist before invoking this method!
     *
     * @param src
     * @param dst
     * @return Graph&
     */
    Graph &add_edge(VID_T src, VID_T dst) {
        auto src_index = vertex_map_.at(src);
        auto dst_index = vertex_map_.at(dst);
        outgoing_neighbors_[src_index].emplace(dst);
        incoming_neighbors_[dst_index].emplace(src);
        return *this;
    }

    /**
     * @brief Remove an edge from the graph. This method does nothing if the edge does not exist.
     * @warning Make sure both the source vertex and the destination vertex exist before invoking this method!
     *
     * @param src
     * @param dst
     * @return Graph&
     */
    Graph &remove_edge(VID_T src, VID_T dst) {
        auto src_index = vertex_map_.at(src);
        auto dst_index = vertex_map_.at(dst);
        if (auto iter = outgoing_neighbors_[src_index].find(dst); iter != outgoing_neighbors_[src_index].end()) {
            outgoing_neighbors_[src_index].erase(iter);
        }
        if (auto iter = incoming_neighbors_[dst_index].find(src); iter != incoming_neighbors_[dst_index].end()) {
            incoming_neighbors_[dst_index].erase(iter);
        }
        return *this;
    }

    /**
     * @brief Check whether the given edge exists.
     * @warning Make sure both the source vertex and the destination vertex exist before invoking this method!
     *
     * @param src
     * @param dst
     * @return true
     * @return false
     */
    bool has_edge(VID_T src, VID_T dst) const {
        auto src_index = vertex_map_.at(src);
        return outgoing_neighbors_[src_index].find(dst) != outgoing_neighbors_[src_index].end();
    }

    /**
     * @brief Get the set of outgoing neighbors of a vertex.
     * @warning Make sure the vertex exists before invoking this method!
     *
     * @param vertex_id
     * @return const std::unordered_set<VID_T>&
     */
    const std::unordered_set<VID_T> &outgoing_neighbors(VID_T vertex_id) const {
        return outgoing_neighbors_[vertex_map_.at(vertex_id)];
    }

    /**
     * @brief Get the set of incoming neighbors of a vertex.
     * @warning Make sure the vertex exists before invoking this method!
     *
     * @param vertex_id
     * @return const std::unordered_set<VID_T>&
     */
    const std::unordered_set<VID_T> &incoming_neighbors(VID_T vertex_id) const {
        return incoming_neighbors_[vertex_map_.at(vertex_id)];
    }

  private:
    std::unordered_set<VID_T> vertices_;
    std::unordered_map<VID_T, size_t> vertex_map_;
    std::vector<std::unordered_set<VID_T>> outgoing_neighbors_;
    std::vector<std::unordered_set<VID_T>> incoming_neighbors_;
    std::list<size_t> free_slots_;
};
}  // namespace naivedb

namespace fmt {
template <typename T>
struct formatter<naivedb::Graph<T>> : public naivedb_base_formatter {
    template <typename FormatContext>
    auto format(const naivedb::Graph<T> &obj, FormatContext &ctx) -> decltype(ctx.out()) {
        return format_to(ctx.out(),
                         "Graph {{ vertices_: {}, vertex_map_: {}, outgoing_neighbors_: {}, incoming_neighbors_: {}, "
                         "free_slots_: {} }}",
                         obj.vertices_,
                         obj.vertex_map_,
                         obj.outgoing_neighbors_,
                         obj.incoming_neighbors_,
                         obj.free_slots_);
    }
};
}  // namespace fmt