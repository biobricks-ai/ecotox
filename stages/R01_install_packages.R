#!/usr/bin/env Rscript

# setting CRAN mirror
options(repos = c(CRAN = "https://cloud.r-project.org"))

#install.packages("remotes")           # To install ECOTOXr
#remotes::install_github("sdsu-ecology/ECOTOXr")  # Install ECOTOXr from GitHub
install.packages("ECOTOXr")  # Install ECOTOXr
install.packages("rdflib")            # For creating RDF triples
