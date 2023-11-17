# Sqld stresstest tool

## Single connection

Default: 50k 6000b serial inserts

```
cargo run -- --turso-db <dbname> single-conn
```

`--turso-db` can be omitted if you want to run it against your local sqld.
