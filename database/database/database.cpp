#include "database.h"
#include "../../config/config.h"

#include <functional>

namespace database {
    Database& Database::get() {
        static Database instance;
        return instance;
    }

    Poco::Data::Session Database::createSessionDirect() {
        return Poco::Data::Session(Poco::Data::SessionFactory::instance().create(Poco::Data::MySQL::Connector::KEY, m_connectionStringDirect));
    }

    Poco::Data::Session Database::createSessionRead() {
        return Poco::Data::Session(Poco::Data::SessionFactory::instance().create(Poco::Data::MySQL::Connector::KEY, m_connectionStringRead));
    }

    Poco::Data::Session Database::createSessionWrite() {
        return Poco::Data::Session(Poco::Data::SessionFactory::instance().create(Poco::Data::MySQL::Connector::KEY, m_connectionStringWrite));
    }
    
    size_t Database::getMaxShards() {
		return 2;
	}

    std::string Database::shardingHint(std::string login) {
		size_t shardNumber = std::hash<std::string>{}(login) % getMaxShards();

		std::string result = "-- sharding:";
        result += std::to_string(shardNumber);
		return result;
	}

	std::vector<std::string> Database::getAllHints() {
		std::vector<std::string> results;
		for (size_t i = 0; i < getMaxShards(); ++i) {
			std::string shardName = "-- sharding:";
			shardName += std::to_string(i);
			results.push_back(shardName);
		}
		return results;
	}

	Database::Database() {
        m_connectionStringDirect += "host=";
        m_connectionStringDirect += Config::get().getHost();

		m_connectionStringDirect += ";port=";
		m_connectionStringDirect += Config::get().getPort();

		m_connectionStringDirect += ";user=";
        m_connectionStringDirect += Config::get().getLogin();

        m_connectionStringDirect += ";db=";
        m_connectionStringDirect += Config::get().getDatabase();

        m_connectionStringDirect += ";password=";
        m_connectionStringDirect += Config::get().getPassword();


        m_connectionStringRead += "host=";
        m_connectionStringRead += Config::get().getReadRequestIp();

		m_connectionStringRead += ";port=";
		m_connectionStringRead += Config::get().getPort();

		m_connectionStringRead += ";user=";
        m_connectionStringRead += Config::get().getLogin();

        m_connectionStringRead += ";db=";
        m_connectionStringRead += Config::get().getDatabase();

        m_connectionStringRead += ";password=";
        m_connectionStringRead += Config::get().getPassword();


        m_connectionStringWrite += "host=";
        m_connectionStringWrite += Config::get().getWriteRequestIp();

		m_connectionStringWrite += ";port=";
		m_connectionStringWrite += Config::get().getPort();

		m_connectionStringWrite += ";user=";
        m_connectionStringWrite += Config::get().getLogin();

        m_connectionStringWrite += ";db=";
        m_connectionStringWrite += Config::get().getDatabase();

        m_connectionStringWrite += ";password=";
        m_connectionStringWrite += Config::get().getPassword();


        Poco::Data::MySQL::Connector::registerConnector();
    }
}