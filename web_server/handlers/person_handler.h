#pragma once
#ifndef PERSON_HANDLER_H
#define PERSON_HANDLER_H

#include "Poco/Net/HTTPServer.h"
#include "Poco/Net/HTTPRequestHandler.h"
#include "Poco/Net/HTTPRequestHandlerFactory.h"
#include "Poco/Net/HTTPServerParams.h"
#include "Poco/Net/HTTPServerRequest.h"
#include "Poco/Net/HTTPServerResponse.h"
#include "Poco/Net/HTTPServerParams.h"
#include "Poco/Net/HTMLForm.h"
#include "Poco/Net/ServerSocket.h"
#include "Poco/Timestamp.h"
#include "Poco/DateTimeFormatter.h"
#include "Poco/DateTimeFormat.h"
#include "Poco/Exception.h"
#include "Poco/ThreadPool.h"
#include "Poco/Util/ServerApplication.h"
#include "Poco/Util/Option.h"
#include "Poco/Util/OptionSet.h"
#include "Poco/Util/HelpFormatter.h"

#include <iostream>
#include <fstream>
#include <map>

using Poco::DateTimeFormat;
using Poco::DateTimeFormatter;
using Poco::ThreadPool;
using Poco::Timestamp;
using Poco::Net::HTMLForm;
using Poco::Net::HTTPRequestHandler;
using Poco::Net::HTTPRequestHandlerFactory;
using Poco::Net::HTTPServer;
using Poco::Net::HTTPServerParams;
using Poco::Net::HTTPServerRequest;
using Poco::Net::HTTPServerResponse;
using Poco::Net::NameValueCollection;
using Poco::Net::ServerSocket;
using Poco::Util::Application;
using Poco::Util::HelpFormatter;
using Poco::Util::Option;
using Poco::Util::OptionCallback;
using Poco::Util::OptionSet;
using Poco::Util::ServerApplication;

#include "../../database/person/person.h"

class PersonHandler : public HTTPRequestHandler {
public:
    PersonHandler(const std::string& format): m_format(format) { }

    void handleRequest(HTTPServerRequest& request, HTTPServerResponse& response) {
        HTMLForm form(request, request.stream());
        response.setChunkedTransferEncoding(true);
        response.setContentType("application/json");
        std::ostream& ostr = response.send();
        
        if (form.has("login") && form.has("first_name") && form.has("last_name") && form.has("age")) {
            std::cout << "Adding a new user" << std::endl;
            database::Person person;
            person.login() = form.get("login");
            person.firstName() = form.get("first_name");
            person.lastName() = form.get("last_name");
            person.age() = atol(form.get("age").c_str());

            bool checkResult = true;
            std::string message;
            std::string reason;

            if (!checkName(person.getLogin(), reason)) {
                checkResult = false;
                message += reason;
                message += "<br>";
            }

            if (!checkName(person.getFirstName(), reason)) {
                checkResult = false;
                message += reason;
                message += "<br>";
            }

            if (!checkName(person.getLastName(), reason)) {
                checkResult = false;
                message += reason;
                message += "<br>";
            }

            if (!checkAge(person.getAge(), reason)) {
                checkResult = false;
                message += reason;
                message += "<br>";
            }
            if (checkResult)
            {
                try {
                    //person.saveToMysqlQueue();
                    person.sendToQueue();
                    person.saveToCache();
                    ostr << "{ \"result\": \"";
                    ostr << person.getLogin();
                    ostr << "\"}";
                    return;
                }
                catch (...) {
                    ostr << "{ \"result\": false, \"reason\": \" database error\" }";
                    return;
                }
            }
            else {
                ostr << "{ \"result\": false, \"reason\": \"" << message << "\" }";
                return;
            }
        }
        
        else if (form.has("first_name") && form.has("last_name")) {
            std::cout << "Reading a user by mask" << std::endl;
            try {
                std::string firstName = form.get("first_name");
                std::string lastName = form.get("last_name");
                auto results = database::Person::readByMaskQueue(firstName, lastName);
                
                Poco::JSON::Array arrayJSON;
                for (auto result : results) {
                    arrayJSON.add(result.toJSON());
                }
                Poco::JSON::Stringifier::stringify(arrayJSON, ostr);
            }
            catch (...) {
                ostr << "{ \"result\": false, \"reason\": \"not found\" }";
                return;
            }
            return;
        }
        
        else if (form.has("login")) {
            std::cout << "Reading a user by login" << std::endl;
            std::string login = form.get("login");
            
            try {
                database::Person result = database::Person::readByLoginFromCache(login);
                std::cout << "cache hit: " << login << std::endl;

                Poco::JSON::Stringifier::stringify(result.toJSON(), ostr);

                return;
            }
            catch (...) {
                std::cout << "cache missed: " << login << std::endl;
            }
            
            try {
                database::Person result = database::Person::readByLoginQueue(login);

                result.saveToCache();
                
                Poco::JSON::Stringifier::stringify(result.toJSON(), ostr);

                return;
            }
            catch (...) {
                ostr << "{ \"result\": false, \"reason\": \"not found\" }";
                return;
            }
        }
    } 


private:
    std::string m_format;

    bool checkName(const std::string& name, std::string& reason) {
        if (name.length() < 3) {
            reason = "Name must be at least 3 characters";
            return false;
        }

        if (name.find(' ') != std::string::npos) {
            reason = "Name cannot contain whitespaces";
            return false;
        }

        if (name.find('\t') != std::string::npos) {
            reason = "Name cannot contain whitespaces";
            return false;
        }

        return true;
    }

    bool checkAge(const long age, std::string& reason) {
        if (age < 0) {
            reason = "Age must not be a negative number";
            return false;
        }

        return true;
    }
};

#endif // !PERSON_HANDLER_H