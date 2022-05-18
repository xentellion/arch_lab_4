#pragma once
#ifndef PERSON_H
#define PERSON_H

#include <string>
#include <vector>
#include "Poco/JSON/Object.h"

namespace database {
    class Person {
    public:
        static Person fromJSON(const std::string& str);

        const std::string& getLogin() const;
        const std::string& getFirstName() const;
        const std::string& getLastName() const;
        long getAge() const;

        std::string& login();
        std::string& firstName();
        std::string& lastName();
        long& age();

        static void initDirectSession();
        static void initQueueSession();
        static void warmUpCache();

        static Person readByLoginDirect(std::string login);
        static Person readByLoginQueue(std::string login);
        static Person readByLoginFromCache(std::string login);

        static std::vector<Person> readByMaskDirect(std::string firstName, std::string lastName);
        static std::vector<Person> readByMaskQueue(std::string firstName, std::string lastName);
        
        static std::vector<Person> readAll();
        
        void saveToMysqlDirect();
        void saveToMysqlQueue();

        void saveToCache();
        
        void sendToQueue();

        static size_t sizeOfCache();

        Poco::JSON::Object::Ptr toJSON() const;

    private:
        std::string m_login;
        std::string m_firstName;
        std::string m_lastName;
        long m_age;
    };
}

#endif // !PERSON_H