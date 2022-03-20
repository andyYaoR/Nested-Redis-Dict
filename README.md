# Nested-Redis-Dict

A Python dictionary with Redis as the storage back-end, with support for nested dictionary reads and writes.  

**Nested-redis-dict has all the methods and behavior of a normal dictionary.**

 


## Pre-requirements
**1. Running redis server with RedisJSON module.**
  - Running in Docker
  ```
  docker run -p 6379:6379 --name redis-redisjson redislabs/rejson:latest
  ```
  - Build from source
  
    ***Install Rust***
    ```
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

    ```
    ***Build***
    ```
    git clone https://github.com/RedisJSON/RedisJSON.git && cd RedisJSON
    cargo build --release
    cargo test --features test
    ```
    ***Run***
    
    **Linux**
    ```
    redis-server --loadmodule ./target/release/librejson.so
    ```
    
    **Mac OS**
    ```
    redis-server --loadmodule ./target/release/librejson.dylib
    ```
**2. rapidjson for serialization**
```
pip install python-rapidjson
```


**Warning: This is an early hack. Maybe not stable for production environment!** 
