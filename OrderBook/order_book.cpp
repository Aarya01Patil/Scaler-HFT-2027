#include <cstdint>
#include <vector>
#include <string>
#include <unordered_map>
#include <map>
#include <memory>
#include <chrono>
#include <iostream>
#include <iomanip>
#include <algorithm>
#include <set>
#include <cassert>
#include <stdexcept>

struct Order {
    uint64_t order_id;
    bool is_buy;  // true for buy, false for sell
    double price;
    uint64_t quantity;
    uint64_t timestamp_ns;
};

struct PriceLevel {
    double price;
    uint64_t total_quantity;
    
    PriceLevel(double p, uint64_t qty) : price(p), total_quantity(qty) {}
    
    bool operator==(const PriceLevel& other) const {
        return price == other.price && total_quantity == other.total_quantity;
    }
};

class OrderBook {
private:
    struct OrderNode {
        Order order;
        OrderNode* next;
        OrderNode* prev;
        
        OrderNode(const Order& ord) : order(ord), next(nullptr), prev(nullptr) {}
    };
    
    struct PriceLevelData {
        uint64_t total_quantity;
        OrderNode* head;
        OrderNode* tail;
        
        PriceLevelData() : total_quantity(0), head(nullptr), tail(nullptr) {}
    };
    
    // Bid side (buy orders) - sorted descending by price
    std::map<double, PriceLevelData, std::greater<double>> bids_;
    
    // Ask side (sell orders) - sorted ascending by price  
    std::map<double, PriceLevelData, std::less<double>> asks_;
    
    // Order lookup for O(1) access
    std::unordered_map<uint64_t, OrderNode*> order_lookup_;
    
    // Trading statistics
    uint64_t total_trades_;
    uint64_t total_volume_;
    
    uint64_t get_current_timestamp() const {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch()).count();
    }
    
    void add_order_to_side(const Order& order, std::map<double, PriceLevelData, std::greater<double>>& side) {
        OrderNode* new_node = new OrderNode(order);
        order_lookup_[order.order_id] = new_node;
        
        auto& level_data = side[order.price];
        level_data.total_quantity += order.quantity;
        
        // Add to the tail of the price level (FIFO)
        if (!level_data.head) {
            level_data.head = level_data.tail = new_node;
        } else {
            level_data.tail->next = new_node;
            new_node->prev = level_data.tail;
            level_data.tail = new_node;
        }
    }
    
    void add_order_to_side(const Order& order, std::map<double, PriceLevelData, std::less<double>>& side) {
        OrderNode* new_node = new OrderNode(order);
        order_lookup_[order.order_id] = new_node;
        
        auto& level_data = side[order.price];
        level_data.total_quantity += order.quantity;
        
        // Add to the tail of the price level (FIFO)
        if (!level_data.head) {
            level_data.head = level_data.tail = new_node;
        } else {
            level_data.tail->next = new_node;
            new_node->prev = level_data.tail;
            level_data.tail = new_node;
        }
    }
    
    bool remove_order_from_side(uint64_t order_id, std::map<double, PriceLevelData, std::greater<double>>& side) {
        auto node_it = order_lookup_.find(order_id);
        if (node_it == order_lookup_.end()) return false;
        
        OrderNode* node = node_it->second;
        double price = node->order.price;
        
        auto level_it = side.find(price);
        if (level_it == side.end()) return false;
        
        auto& level_data = level_it->second;
        
        // Update quantity
        if (level_data.total_quantity >= node->order.quantity) {
            level_data.total_quantity -= node->order.quantity;
        } else {
            level_data.total_quantity = 0;
        }
        
        // Remove from linked list
        if (node->prev) node->prev->next = node->next;
        if (node->next) node->next->prev = node->prev;
        
        if (node == level_data.head) level_data.head = node->next;
        if (node == level_data.tail) level_data.tail = node->prev;
        
        // Remove price level if empty
        if (!level_data.head) {
            side.erase(level_it);
        }
        
        delete node;
        order_lookup_.erase(node_it);
        return true;
    }
    
    bool remove_order_from_side(uint64_t order_id, std::map<double, PriceLevelData, std::less<double>>& side) {
        auto node_it = order_lookup_.find(order_id);
        if (node_it == order_lookup_.end()) return false;
        
        OrderNode* node = node_it->second;
        double price = node->order.price;
        
        auto level_it = side.find(price);
        if (level_it == side.end()) return false;
        
        auto& level_data = level_it->second;
        
        if (level_data.total_quantity >= node->order.quantity) {
            level_data.total_quantity -= node->order.quantity;
        } else {
            level_data.total_quantity = 0;
        }
        
        if (node->prev) node->prev->next = node->next;
        if (node->next) node->next->prev = node->prev;
        
        if (node == level_data.head) level_data.head = node->next;
        if (node == level_data.tail) level_data.tail = node->prev;
        
        if (!level_data.head) {
            side.erase(level_it);
        }
        
        delete node;
        order_lookup_.erase(node_it);
        return true;
    }
    
    void execute_trade(OrderNode* buy_order, OrderNode* sell_order, uint64_t trade_quantity) {
        double trade_price = std::min(buy_order->order.price, sell_order->order.price);
        
        std::cout << "TRADE: " << trade_quantity << " @ " << trade_price 
                  << " (Buy: " << buy_order->order.order_id 
                  << ", Sell: " << sell_order->order.order_id << ")" << std::endl;
        
        total_trades_++;
        total_volume_ += trade_quantity;
        
        // Update quantities
        buy_order->order.quantity -= trade_quantity;
        sell_order->order.quantity -= trade_quantity;
        
        // Update price level quantities
        auto buy_level_it = bids_.find(buy_order->order.price);
        if (buy_level_it != bids_.end()) {
            buy_level_it->second.total_quantity -= trade_quantity;
        }
        
        auto sell_level_it = asks_.find(sell_order->order.price);
        if (sell_level_it != asks_.end()) {
            sell_level_it->second.total_quantity -= trade_quantity;
        }
        
        // Remove fully filled orders
        if (buy_order->order.quantity == 0) {
            remove_order_from_side(buy_order->order.order_id, bids_);
        }
        if (sell_order->order.quantity == 0) {
            remove_order_from_side(sell_order->order.order_id, asks_);
        }
    }
    
    void process_matching() {
        while (!bids_.empty() && !asks_.empty()) {
            double best_bid = bids_.begin()->first;
            double best_ask = asks_.begin()->first;
            
            if (best_bid < best_ask) {
                break; // No crossing
            }
            
            // Get the first orders at best bid/ask
            OrderNode* best_buy_order = bids_.begin()->second.head;
            OrderNode* best_sell_order = asks_.begin()->second.head;
            
            if (!best_buy_order || !best_sell_order) {
                break;
            }
            
            uint64_t trade_quantity = std::min(best_buy_order->order.quantity, best_sell_order->order.quantity);
            execute_trade(best_buy_order, best_sell_order, trade_quantity);
        }
    }

public:
    OrderBook() : total_trades_(0), total_volume_(0) {}
    
    ~OrderBook() {
        // Cleanup all orders
        for (auto& [order_id, node] : order_lookup_) {
            delete node;
        }
    }
    
    // Core interface
    void add_order(const Order& order, bool match_immediately = true) {
        if (order_lookup_.find(order.order_id) != order_lookup_.end()) {
            throw std::runtime_error("Order ID " + std::to_string(order.order_id) + " already exists");
        }
        
        if (order.quantity == 0) {
            throw std::runtime_error("Order quantity cannot be zero");
        }
        
        if (order.price <= 0.0) {
            throw std::runtime_error("Invalid price: " + std::to_string(order.price));
        }
        
        Order order_with_ts = order;
        if (order_with_ts.timestamp_ns == 0) {
            order_with_ts.timestamp_ns = get_current_timestamp();
        }
        
        if (order_with_ts.is_buy) {
            add_order_to_side(order_with_ts, bids_);
        } else {
            add_order_to_side(order_with_ts, asks_);
        }
        
        // Try to match orders
        if (match_immediately) {
            process_matching();
        }
    }
    
    bool cancel_order(uint64_t order_id) {
        auto it = order_lookup_.find(order_id);
        if (it == order_lookup_.end()) {
            return false;
        }
        
        OrderNode* node = it->second;
        bool success = false;
        
        if (node->order.is_buy) {
            success = remove_order_from_side(order_id, bids_);
        } else {
            success = remove_order_from_side(order_id, asks_);
        }
        
        return success;
    }
    
    bool amend_order(uint64_t order_id, double new_price, uint64_t new_quantity, bool match_immediately = true) {
        auto it = order_lookup_.find(order_id);
        if (it == order_lookup_.end()) {
            return false;
        }
        
        if (new_quantity == 0) {
            throw std::runtime_error("New quantity cannot be zero");
        }
        
        if (new_price <= 0.0) {
            throw std::runtime_error("Invalid new price: " + std::to_string(new_price));
        }
        
        OrderNode* node = it->second;
        Order& existing_order = node->order;
        
        // Check if price changed
        bool price_changed = std::abs(existing_order.price - new_price) > 1e-12;
        
        if (price_changed) {
            // Cancel and re-add
            Order new_order = existing_order;
            new_order.price = new_price;
            new_order.quantity = new_quantity;
            
            // Cancel old order
            if (!cancel_order(order_id)) {
                return false;
            }
            
            // Add new order
            add_order(new_order, match_immediately);
        } else {
            // Update quantity in place
            int64_t quantity_diff = static_cast<int64_t>(new_quantity) - static_cast<int64_t>(existing_order.quantity);
            
            if (existing_order.is_buy) {
                auto level_it = bids_.find(existing_order.price);
                if (level_it != bids_.end()) {
                    level_it->second.total_quantity += quantity_diff;
                }
            } else {
                auto level_it = asks_.find(existing_order.price);
                if (level_it != asks_.end()) {
                    level_it->second.total_quantity += quantity_diff;
                }
            }
            existing_order.quantity = new_quantity;
        }
        
        // Try to match after amendment
        if (match_immediately) {
            process_matching();
        }
        return true;
    }
    
    void get_snapshot(size_t depth, std::vector<PriceLevel>& bids, std::vector<PriceLevel>& asks) const {
        bids.clear();
        asks.clear();
        
        // Get top bids (highest prices first)
        size_t count = 0;
        for (const auto& [price, level_data] : bids_) {
            if (count++ >= depth) break;
            bids.push_back(PriceLevel(price, level_data.total_quantity));
        }
        
        // Get top asks (lowest prices first)
        count = 0;
        for (const auto& [price, level_data] : asks_) {
            if (count++ >= depth) break;
            asks.push_back(PriceLevel(price, level_data.total_quantity));
        }
    }
    
    void print_book(size_t depth = 10) const {
        std::vector<PriceLevel> bids, asks;
        get_snapshot(depth, bids, asks);
        
        std::cout << "\n=== ORDER BOOK (Top " << depth << ") ===" << std::endl;
        std::cout << std::setw(12) << "BID QTY" << " | " << std::setw(10) << "PRICE" 
                  << " || " << std::setw(10) << "PRICE" << " | " << std::setw(12) << "ASK QTY" << std::endl;
        std::cout << std::string(60, '-') << std::endl;
        
        size_t max_levels = std::max(bids.size(), asks.size());
        
        for (size_t i = 0; i < max_levels; ++i) {
            if (i < bids.size()) {
                std::cout << std::setw(12) << bids[i].total_quantity << " | " 
                          << std::setw(10) << std::fixed << std::setprecision(4) << bids[i].price;
            } else {
                std::cout << std::setw(12) << " " << " | " << std::setw(10) << " ";
            }
            
            std::cout << " || ";
            
            if (i < asks.size()) {
                std::cout << std::setw(10) << std::fixed << std::setprecision(4) << asks[i].price << " | " 
                          << std::setw(12) << asks[i].total_quantity;
            } else {
                std::cout << std::setw(10) << " " << " | " << std::setw(12) << " ";
            }
            std::cout << std::endl;
        }
        
        std::cout << "Total Orders: " << get_total_orders() 
                  << " (Bids: " << get_bid_levels() << " levels, "
                  << "Asks: " << get_ask_levels() << " levels)" << std::endl;
        std::cout << "Trades: " << total_trades_ << ", Volume: " << total_volume_ << std::endl;
    }
    
    // Additional utility methods
    size_t get_total_orders() const { return order_lookup_.size(); }
    size_t get_bid_levels() const { return bids_.size(); }
    size_t get_ask_levels() const { return asks_.size(); }
    bool order_exists(uint64_t order_id) const { 
        return order_lookup_.find(order_id) != order_lookup_.end(); 
    }
    double get_best_bid() const { 
        return bids_.empty() ? 0.0 : bids_.begin()->first; 
    }
    double get_best_ask() const { 
        return asks_.empty() ? 0.0 : asks_.begin()->first; 
    }
    double get_spread() const {
        return get_best_ask() - get_best_bid();
    }
    
    void get_statistics(uint64_t& trades, uint64_t& volume, size_t& active_orders) const {
        trades = total_trades_;
        volume = total_volume_;
        active_orders = order_lookup_.size();
    }
    
    void print_order(uint64_t order_id) const {
        auto it = order_lookup_.find(order_id);
        if (it == order_lookup_.end()) {
            std::cout << "Order " << order_id << " not found" << std::endl;
            return;
        }
        
        const Order& order = it->second->order;
        std::cout << "Order " << order_id << ": " 
                  << (order.is_buy ? "BUY" : "SELL") 
                  << " " << order.quantity << " @ " << order.price 
                  << " (TS: " << order.timestamp_ns << ")" << std::endl;
    }
    
    // Manual matching control
    void match_orders() {
        process_matching();
    }
};

//All different types of test
void run_comprehensive_tests() {
    std::cout << "=== RUNNING COMPREHENSIVE TESTS ===" << std::endl;
    int passed = 0, total = 0;
    
    // Test 1: Basic Order Addition
    {
        OrderBook book;
        book.add_order(Order{1, true, 100.0, 100, 1});
        book.add_order(Order{2, false, 101.0, 50, 2});
        
        assert(book.get_total_orders() == 2);
        assert(book.get_bid_levels() == 1);
        assert(book.get_ask_levels() == 1);
        std::cout << "✓ Test 1: Basic Order Addition - PASSED" << std::endl;
        passed++;
    }
    total++;
    
    // Test 2: Duplicate Order ID
    {
        OrderBook book;
        book.add_order(Order{1, true, 100.0, 100, 1});
        try {
            book.add_order(Order{1, true, 101.0, 50, 2});
            assert(false);
        } catch (const std::runtime_error&) {
            // Expected
        }
        std::cout << "✓ Test 2: Duplicate Order ID - PASSED" << std::endl;
        passed++;
    }
    total++;
    
    // Test 3: Order Cancellation
    {
        OrderBook book;
        book.add_order(Order{1, true, 100.0, 100, 1});
        book.add_order(Order{2, true, 100.0, 200, 2});
        
        assert(book.cancel_order(1) == true);
        assert(book.get_total_orders() == 1);
        assert(book.order_exists(2) == true);
        assert(book.order_exists(1) == false);
        assert(book.cancel_order(999) == false);
        std::cout << "✓ Test 3: Order Cancellation - PASSED" << std::endl;
        passed++;
    }
    total++;
    
    // Test 4: Price Level Aggregation
    {
        OrderBook book;
        book.add_order(Order{1, true, 100.0, 100, 1});
        book.add_order(Order{2, true, 100.0, 200, 2});
        book.add_order(Order{3, true, 101.0, 50, 3});
        
        std::vector<PriceLevel> bids, asks;
        book.get_snapshot(5, bids, asks);
        
        assert(bids.size() == 2);
        assert(bids[0].price == 101.0 && bids[0].total_quantity == 50);
        assert(bids[1].price == 100.0 && bids[1].total_quantity == 300);
        std::cout << "✓ Test 4: Price Level Aggregation - PASSED" << std::endl;
        passed++;
    }
    total++;
    
    // Test 5: Order Matching
    {
        OrderBook book;
        book.add_order(Order{1, true, 100.0, 100, 1});   // Buy
        book.add_order(Order{2, false, 99.0, 50, 2});    // Sell - should match
        
        // After matching, buy order should have 50 left, sell order should be gone
        assert(book.get_total_orders() == 1);
        assert(book.order_exists(1) == true);
        assert(book.order_exists(2) == false);
        
        std::vector<PriceLevel> bids, asks;
        book.get_snapshot(5, bids, asks);
        assert(bids[0].total_quantity == 50); // Remaining quantity
        
        std::cout << "✓ Test 5: Order Matching - PASSED" << std::endl;
        passed++;
    }
    total++;
    
    // Test 6: Amend Order
    {
        OrderBook book;
        book.add_order(Order{1, true, 100.0, 100, 1});
        book.add_order(Order{2, true, 100.0, 200, 2});
        
        assert(book.amend_order(1, 100.0, 500) == true);
        
        std::vector<PriceLevel> bids, asks;
        book.get_snapshot(5, bids, asks);
        assert(bids[0].total_quantity == 700); // 500 + 200
        std::cout << "✓ Test 6: Amend Order - PASSED" << std::endl;
        passed++;
    }
    total++;
    
    // Test 7: Large Scale Operations - FIXED
    {
        OrderBook book;
        const int NUM_ORDERS = 1000;
        
        // Add orders without immediate matching to avoid trades
        for (int i = 0; i < NUM_ORDERS; ++i) {
            // Use prices that won't cross to avoid matching
            double price = 100.0 + (i % 20); // Spread out prices to avoid crossing
            book.add_order(Order{
                static_cast<uint64_t>(i + 1), 
                true,  // All buys to avoid crossing with sells
                price, 
                100, 
                static_cast<uint64_t>(i + 1)
            }, false); // Don't match immediately
        }
        
        assert(book.get_total_orders() == NUM_ORDERS);
        
        // Cancel half the orders
        int cancelled_count = 0;
        for (int i = 0; i < NUM_ORDERS / 2; ++i) {
            if (book.cancel_order(static_cast<uint64_t>(i + 1))) {
                cancelled_count++;
            }
        }
        
        assert(book.get_total_orders() == NUM_ORDERS - cancelled_count);
        std::cout << "✓ Test 7: Large Scale Operations - PASSED" << std::endl;
        passed++;
    }
    total++;
    
    std::cout << "\n=== TEST SUMMARY ===" << std::endl;
    std::cout << "Passed: " << passed << "/" << total << std::endl;
    
    if (passed == total) {
        std::cout << "ALL TESTS PASSED!" << std::endl;
    } else {
        std::cout << "SOME TESTS FAILED!" << std::endl;
        exit(1);
    }
}

void demonstrate_features() {
    std::cout << "\n=== DEMONSTRATING ALL FEATURES ===" << std::endl;
    
    OrderBook book;
    
    // 1. Add orders
    std::cout << "\n1. Adding initial orders..." << std::endl;
    book.add_order(Order{1, true, 100.00, 1000, 1});
    book.add_order(Order{2, true, 99.50, 500, 2});
    book.add_order(Order{3, true, 99.00, 750, 3});
    book.add_order(Order{4, true, 100.00, 250, 4});
    
    book.add_order(Order{5, false, 101.00, 800, 5});
    book.add_order(Order{6, false, 101.50, 600, 6});
    book.add_order(Order{7, false, 102.00, 400, 7});
    book.add_order(Order{8, false, 101.00, 200, 8});
    
    book.print_book();
    
    // 2. Cancel order
    std::cout << "\n2. Cancelling order #3..." << std::endl;
    book.cancel_order(3);
    book.print_book(5);
    
    // 3. Amend order (quantity change)
    std::cout << "\n3. Amending order #1 (quantity 1000 → 1500)..." << std::endl;
    book.amend_order(1, 100.00, 1500);
    book.print_book(5);
    
    // 4. Amend order (price change)
    std::cout << "\n4. Amending order #5 (price 101.00 → 100.50)..." << std::endl;
    book.amend_order(5, 100.50, 800);
    book.print_book(5);
    
    // 5. Test matching engine
    std::cout << "\n5. Testing matching engine with aggressive sell..." << std::endl;
    book.add_order(Order{9, false, 99.00, 300, 9}); // Should match with bids
    book.print_book(5);
    
    // 6. Get snapshot
    std::cout << "\n6. Getting top 3 snapshot..." << std::endl;
    std::vector<PriceLevel> bids, asks;
    book.get_snapshot(3, bids, asks);
    
    std::cout << "Top 3 Bids:" << std::endl;
    for (const auto& bid : bids) {
        std::cout << "  Price: " << std::fixed << std::setprecision(2) << bid.price 
                  << ", Total Qty: " << bid.total_quantity << std::endl;
    }
    
    std::cout << "Top 3 Asks:" << std::endl;
    for (const auto& ask : asks) {
        std::cout << "  Price: " << std::fixed << std::setprecision(2) << ask.price 
                  << ", Total Qty: " << ask.total_quantity << std::endl;
    }
    
    // 7. Print individual order
    std::cout << "\n7. Printing individual orders..." << std::endl;
    book.print_order(1);
    book.print_order(999); // Non-existent
    
    // 8. Get statistics
    std::cout << "\n8. Final statistics:" << std::endl;
    uint64_t trades, volume;
    size_t active_orders;
    book.get_statistics(trades, volume, active_orders);
    std::cout << "Trades: " << trades << ", Volume: " << volume 
              << ", Active Orders: " << active_orders << std::endl;
    std::cout << "Best Bid: " << book.get_best_bid() 
              << ", Best Ask: " << book.get_best_ask()
              << ", Spread: " << book.get_spread() << std::endl;
}

void performance_test() {
    std::cout << "\n=== PERFORMANCE TEST ===" << std::endl;
    
    OrderBook book;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    // Add orders without immediate matching for performance test
    for (int i = 0; i < 10000; i++) {
        Order o;
        o.order_id = i + 1;
        o.is_buy = (i % 3 == 0);
        // Use non-crossing prices to avoid trades during performance test
        o.price = o.is_buy ? 90.0 + (i % 10) : 110.0 + (i % 10);
        o.quantity = 25 + (i % 150); 
        o.timestamp_ns = 0;
        book.add_order(o, false); // Don't match immediately
    }
    
    auto mid = std::chrono::high_resolution_clock::now();
    
    // Cancel some orders
    for (int i = 0; i < 2000; i++) {
        book.cancel_order(i * 5 + 1);
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    
    auto add_time = std::chrono::duration_cast<std::chrono::microseconds>(mid - start);
    auto cancel_time = std::chrono::duration_cast<std::chrono::microseconds>(end - mid);
    
    std::cout << "Added 10000 orders in " << add_time.count() << " us" << std::endl;
    std::cout << "Cancelled 2000 orders in " << cancel_time.count() << " us" << std::endl;
    
    uint64_t trades, volume;
    size_t active_orders;
    book.get_statistics(trades, volume, active_orders);
    std::cout << "Trades: " << trades << ", Volume: " << volume 
              << ", Active Orders: " << active_orders << std::endl;
}

int main() {
    try {
        std::cout << "=== ORDER BOOK IMPLEMENTATION ===" << std::endl;
        
        // Run comprehensive tests
        run_comprehensive_tests();
        
        // Demonstrate all features
        demonstrate_features();
        
        // Performance test
        performance_test();
        
        std::cout << "\n=== PROGRAM COMPLETED SUCCESSFULLY ===" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}