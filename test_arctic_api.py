from arctic import Arctic

arctic = Arctic('localhost')

# test the behavior of chunk_range in read, write and update functions
# chunk_range:
# minute data - not include right end date
# maybe because the chunk end before 08:00, and there's no data yet.
# daily data - include right end date
