# edit this stage to create new resources in the data directory
mtcars |> arrow::write_parquet("data/mtcars.parquet")
