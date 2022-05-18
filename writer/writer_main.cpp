#include <iostream>
#include <csignal>

#include <boost/program_options.hpp>

#include <cppkafka/consumer.h>
#include <cppkafka/configuration.h>

#include "../config/config.h"
#include "../database/person/person.h"

namespace po = boost::program_options;

bool running = true;

int main(int argc, char *argv[]) {
    try {
        po::options_description desc{"Options"};
        desc.add_options()("help,h", "This screen")("read,", po::value<std::string>()->required(), "set ip address for read requests")("write,", po::value<std::string>()->required(), "set ip address for write requests")("port,", po::value<std::string>()->required(), "databaase port")("login,", po::value<std::string>()->required(), "database login")("password,", po::value<std::string>()->required(), "database password")("database,", po::value<std::string>()->required(), "database name")("queue,", po::value<std::string>()->required(), "queue url")("topic,", po::value<std::string>()->required(), "topic name")("group_id,", po::value<std::string>()->required(), "consumer group_id name")("cache_servers,", po::value<std::string>()->required(), "iginite cache servers");

        po::variables_map vm;
        po::store(parse_command_line(argc, argv, desc), vm);

        if (vm.count("help"))
            std::cout << desc << '\n';

        if (vm.count("read"))
            Config::get().readRequestIp() = vm["read"].as<std::string>();
        if (vm.count("write"))
            Config::get().writeRequestIp() = vm["write"].as<std::string>();
        if (vm.count("port"))
            Config::get().port() = vm["port"].as<std::string>();
        if (vm.count("login"))
            Config::get().login() = vm["login"].as<std::string>();
        if (vm.count("password"))
            Config::get().password() = vm["password"].as<std::string>();
        if (vm.count("database"))
            Config::get().database() = vm["database"].as<std::string>();
        if (vm.count("queue"))
            Config::get().queueHost() = vm["queue"].as<std::string>();
        if (vm.count("topic"))
            Config::get().queueTopic() = vm["topic"].as<std::string>();
        if (vm.count("group_id"))
            Config::get().queueGroupId() = vm["group_id"].as<std::string>();

        // Stop processing on SIGINT
        signal(SIGINT, [](int){ running = false; });

        // Construct the configuration
        cppkafka::Configuration config = {
            {"metadata.broker.list", Config::get().getQueueHost()},
            {"group.id", Config::get().getQueueGroupId()},
            // Disable auto commit
            {"enable.auto.commit", false}};

        // Create the consumer
        cppkafka::Consumer consumer(config);

        // Print the assigned partitions on assignment
        consumer.set_assignment_callback([](const cppkafka::TopicPartitionList &partitions)
                                         { std::cout << "Got assigned: " << partitions << std::endl; });

        // Print the revoked partitions on revocation
        consumer.set_revocation_callback([](const cppkafka::TopicPartitionList &partitions)
                                         { std::cout << "Got revoked: " << partitions << std::endl; });

        // Subscribe to the topic
        consumer.subscribe({Config::get().getQueueTopic()});

        std::cout << "Consuming messages from topic " << Config::get().getQueueTopic() << std::endl;

        // Now read lines and write them into kafka
        while (running) {
            // Try to consume a message
            cppkafka::Message msg = consumer.poll();
            if (msg) {
                // If we managed to get a message
                if (msg.get_error()) {
                    // Ignore EOF notifications from rdkafka
                    if (!msg.is_eof()) {
                        std::cout << "[+] Received error notification: " << msg.get_error() << std::endl;
                    }
                }
                else {
                    // Print the key (if any)
                    if (msg.get_key()) {
                        std::cout << msg.get_key() << " -> ";
                    }
                    // Print the payload
                    std::string payload = msg.get_payload();
                    std::cout << msg.get_payload() << std::endl;
                    
                    database::Person person = database::Person::fromJSON(payload);
                    try {
                        person.saveToMysqlQueue();
                    }
                    catch(...) {
                        std::cerr << "Error: Person already exists." << std::endl;
                        consumer.commit(msg);
                    }
             
                    // Now commit the message
                    consumer.commit(msg);
                }
            }
        }
    }
    catch (const std::exception &e) {
        std::cerr << e.what() << std::endl;
    }

    return 1;
}