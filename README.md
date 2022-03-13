# How to run

`go run . --input_file=in.json --window_size=10`

## Thoughts on solutions

There are 2 ways to implement the solution:

1. read file continuously and calculate avg on the go
2. read file till the end before we start calculating avg

Since we aggregate average by minute the memory should not be a problem so i've decided to implement the second option.

## Bonus 

In real life this sort of tasks should be done with OLAP or Timeseries databases. 
Here is the example how to do it with Clickhouse and it's one-time server:

`apt install clickhouse-common-static`

`clickhouse-local --query "$(cat query.sql)" --param_ws="10" --structure "json String" --input-format "JSONAsString" --output-format "JSONEachRow" < in.json`