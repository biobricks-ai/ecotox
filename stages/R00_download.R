#!/usr/bin/env Rscript
library(httr2)
#options(curl_ssl_backend = "openssl")
#Sys.setenv(CURL_CA_BUNDLE = "/etc/pki/tls/certs/ca-bundle.crt")

library(ECOTOXr)
download_ecotox_data(verify_ssl = FALSE)
#download_ecotox_data(verify_ssl = TRUE)

install.packages("rdflib") 
