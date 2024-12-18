#!/usr/bin/env Rscript
# edit this stage to create new resources in the data directory
# mtcars |> arrow::write_parquet("data/mtcars.parquet")

library(ECOTOXr)
library(rdflib)

# Get the path to the ECOTOX data file
data_file <- get_ecotox_sqlite_file()
print(data_file)

# Build the SQLite database
build_ecotox_sqlite(source = data_file)

# Connect to the ECOTOX database
conn <- dbConnectEcotox()

# Check the connection
print(conn)

## Connect to the ECOTOX SQLite database
#ecotox_db <- connect_ecotox_sqlite()
#
## Query the database for test data
#test_data <- DBI::dbGetQuery(ecotox_db, "
#  SELECT carrier_id, test_id, cas_number, chem_name, purpose
#  FROM test
#  LIMIT 100
#")
#
## Print the queried data
#print(test_data)
