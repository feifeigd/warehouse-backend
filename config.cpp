#include "config.h"

config::config() {
    opt_group{custom_options_, "global"}
        .add(cmd_addr, "cmd-addr,A", "bind address for the contrlller")
        .add(cmd_port, "cmd-port,P", "port to listen for commands");

    opt_group{ custom_options_, "tls" }
        .add<std::string>("cert-file,c", "path to the certificate file")
        .add<std::string>("key-file,k", "path to the private file");
}
