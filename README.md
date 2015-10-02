# Overview

This project contains a facade for [OriendDb](http://orientdb.com/orientdb/) with specific focus on the components that embed a document based database in a java application without the need for 
a dedicated server.

# How to get it

This project is currently not hosted on any repository-manager so the fastest way to get it is clone this git-repo and run mvn:install
in the root package

## Dependencies

All dependencies are hosted in [Maven Central](http://mvnrepository.com/), so just running mvn:install will download the required dependencies.

# Usage

## 1. Create a database

OrientDb supports multiple database [engines](http://orientdb.com/docs/last/Concepts.html#database-url), but for the sake of this tutorial, we will be making a database located on the same machine as the code.

```java
String path = "plocal:/path/to/db"
String user = "admin"
String pass = "admin"

ODBFacade db = new ODBFacade(path, user, pass);
db.create();
```

The above code will check if a db exists in the path specified, and will create it if it doesn't find one.

(TO BE COMPLETED)
