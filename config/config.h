#pragma once
#ifndef CONFIG_H
#define CONFIG_H

#include <string>

class Config {
public:
    static Config& get();

    std::string& port();
    std::string& host();
    std::string& login();
    std::string& password();
    std::string& database();

    std::string& cacheServers();

    std::string& readRequestIp();
    std::string& writeRequestIp();

    std::string& queueGroupId();
    std::string& queueHost();
    std::string& queueTopic();

    const std::string& getPort() const;
    const std::string& getHost() const;
    const std::string& getLogin() const;
    const std::string& getPassword() const;
    const std::string& getDatabase() const;

    const std::string& getCacheServers() const;

    const std::string& getReadRequestIp() const;
    const std::string& getWriteRequestIp() const;
    
    const std::string& getQueueGroupId() const;
    const std::string& getQueueHost() const;
    const std::string& getQueueTopic() const;

private:
    Config() = default;
    
    std::string m_host;
    std::string m_port;
    std::string m_login;
    std::string m_password;
    std::string m_database;

    std::string m_readRequestIp;
    std::string m_writeRequestIp;

    std::string m_queueHost;
    std::string m_queueTopic;
    std::string m_queueGroupId;

    std::string m_cacheServers;
};

#endif // !CONFIG_H