#include "config.h"

config::config() {
    opt_group{custom_options_, "global"}
        .add(cmd_addr, "cmd-addr,A", "bind address for the contrlller")
        .add(cmd_port, "cmd-port,P", "port to listen for commands")
        .add(http_port, "http-port", "http/ws port ");

    opt_group{ custom_options_, "tls" }
        .add(cert_file, "cert-file,c", "path to the certificate file")
        .add(key_file, "key-file,k", "path to the private key file");
}
