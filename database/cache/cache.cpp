#include "cache.h"
#include "../../config/config.h"

#include <ignite/thin/ignite_client.h>
#include <ignite/thin/ignite_client_configuration.h>
#include <ignite/thin/cache/cache_peek_mode.h>
#include <exception>

static ignite::thin::IgniteClient m_client;
static ignite::thin::cache::CacheClient<std::string, std::string> m_cache;


namespace database {
    Cache Cache::get() {
        static Cache instance;
        return instance;
    }

    void Cache::put(const std::string& login, const std::string& val) {
        m_cache.Put(login, val);
    }

    bool Cache::get(const std::string& login, std::string& val) {
        try {
            val = m_cache.Get(login);
            return true;
        }
        catch(...) {
            throw std::logic_error("key not found in cache");
        }
    }

    size_t Cache::size() {
        return m_cache.GetSize(ignite::thin::cache::CachePeekMode::ALL);
    }

    void Cache::remove(const std::string& login) {
        m_cache.Remove(login);
    }

    void Cache::removeAll() {
        m_cache.RemoveAll();
    }

    Cache::Cache() {
        ignite::thin::IgniteClientConfiguration cfg;
        cfg.SetEndPoints(Config::get().getCacheServers());
        cfg.SetPartitionAwareness(true);

        try {
            m_client = ignite::thin::IgniteClient::Start(cfg);
            m_cache = m_client.GetOrCreateCache<std::string, std::string>("persons");
        }
        catch (ignite::IgniteError* error) {
            std::cout << "error: " << error->what() << std::endl;
            throw;
        }
    }
}