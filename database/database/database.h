#pragma once
#ifndef DATABASE_H
#define DATABASE_H

#include <string>
#include <vector>

#include "Poco/Data/MySQL/Connector.h"
#include "Poco/Data/MySQL/MySQLException.h"
#include "Poco/Data/SessionFactory.h"

namespace database {
    class Database {
    public:
        static Database& get();
        Poco::Data::Session createSessionDirect();
        Poco::Data::Session createSessionRead();
        Poco::Data::Session createSessionWrite();
        
		static size_t getMaxShards();
		static std::string shardingHint(std::string login);
		static std::vector<std::string> getAllHints();

	private:
        Database();
        std::string m_connectionStringDirect;

        std::string m_connectionStringRead;
        std::string m_connectionStringWrite;
    };
}

#endif // !DATABASE_H