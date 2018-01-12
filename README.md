[![Build Status](https://travis-ci.org/elegans-io/orac-api.png)](https://travis-ci.org/elegans-io/orac-api)
[![Code Triagers Badge](https://www.codetriage.com/elegans-io/orac-api/badges/users.svg)](https://www.codetriage.com/elegans-io/orac-api)


# Welcome!

This is the official repository for ORAC APIs

# API Documentation

https://app.swaggerhub.com/apis/angleto/orac/v0.0.1

# indexing data

## items

```bash
sbt "runMain io.elegans.orac.command.IndexItems --help"
```

e.g.

```bash
sbt "runMain io.elegans.orac.command.IndexItems --inputfile ./sydatacsv/is.csv --header_kv \"Authorization:Basic `echo -n 'test_user:p4ssw0rd' | base64`\""
```

## actions

```bash
sbt "runMain io.elegans.orac.command.IndexActions --help"
```

e.g.

```bash
sbt "runMain io.elegans.orac.command.IndexActions --inputfile ./sydatacsv/os.csv --header_kv \"Authorization:Basic `echo -n 'test_user:p4ssw0rd' | base64`\""
```

## users

```bash
sbt "runMain io.elegans.orac.command.IndexUsers --help"
```

e.g.

```bash
sbt "runMain io.elegans.orac.command.IndexUsers --inputfile ./sydatacsv/us.csv --header_kv \"Authorization:Basic `echo -n 'test_user:p4ssw0rd' | base64`\""
```