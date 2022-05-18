#pragma once
#ifndef CACHE_H
#define CACHE_H

#include <string>

namespace database {
    class Cache {
    public:
        static Cache get();
        void put(const std::string& login, const std::string& val);
        bool get(const std::string& login, std::string& val);
        size_t size();
        void remove(const std::string& login);
        void removeAll();

    private:
        Cache();
    };
}

#endif // !CACHE_H