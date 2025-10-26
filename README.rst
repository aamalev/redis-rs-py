redis-rs
========

.. image:: https://img.shields.io/badge/License-MIT-blue.svg
   :target: https://lbesson.mit-license.org/

.. image:: https://img.shields.io/pypi/v/redis-rs.svg
  :target: https://pypi.org/project/redis-rs

.. image:: https://img.shields.io/pypi/pyversions/redis-rs.svg
  :target: https://pypi.org/project/redis-rs
  :alt: Python versions

.. image:: https://readthedocs.org/projects/redis-rs/badge/?version=latest
  :target: https://github.com/aamalev/redis-rs-py#redis-rs
  :alt: Documentation Status

.. image:: https://github.com/aamalev/redis-rs-py/workflows/Tests/badge.svg
  :target: https://github.com/aamalev/redis-rs-py/actions?query=workflow%3ATests

.. image:: https://img.shields.io/pypi/dm/redis-rs.svg
  :target: https://pypistats.org/packages/redis-rs

|

.. image:: https://img.shields.io/badge/Rustc-1.73.0-blue?logo=rust
  :target: https://www.rust-lang.org/

.. image:: https://img.shields.io/badge/cargo-clippy-blue?logo=rust
  :target: https://doc.rust-lang.org/stable/clippy/

.. image:: https://img.shields.io/badge/PyO3-maturin-blue.svg
  :target: https://github.com/PyO3/maturin

.. image:: https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json
  :target: https://github.com/astral-sh/ruff
  :alt: Linter: ruff

.. image:: https://img.shields.io/badge/code%20style-ruff-000000.svg
  :target: https://github.com/astral-sh/ruff
  :alt: Code style: ruff

.. image:: https://img.shields.io/badge/types-Mypy-blue.svg
  :target: https://github.com/python/mypy
  :alt: Code style: Mypy

.. image:: https://img.shields.io/badge/%F0%9F%A5%9A-Hatch-4051b5.svg
  :alt: Hatch project
  :target: https://github.com/pypa/hatch


Python wrapper for:
  | `redis-rs <https://github.com/redis-rs/redis-rs>`_,
  | `bb8 <https://github.com/djc/bb8>`_,
  | bb8-redis,


Features
--------

* Async client for single and cluster
* Support typing
* Encoding values from str, int, float
* Decoding values to str, int, float, list, dict
* Partial implementation redis mock-client for testing mode

Installation
------------

.. code-block:: shell

    pip install redis-rs

Quick Start
-----------

.. code-block:: python

    import asyncio
    import redis_rs


    async def main():
        # Connect to Redis cluster
        async with redis_rs.create_client(
            "redis://redis-node001",
            "redis://redis-node002",
            max_size=1,
            cluster=True,
            features=["mock"],  # enable mock mode for testing
        ) as client:

            # Get server information
            info = await client.execute("INFO", "SERVER", encoding="info")
            print(f"Redis version: {info['redis_version']}")

            # Check connection status
            print(f"Connection status: {client.status()}")


    asyncio.run(main())

Usage Examples
==============

Basic Operations
----------------

String Operations
~~~~~~~~~~~~~~~~~

.. code-block:: python

    async def string_operations(client):
        # Set and get string values
        await client.set("my_key", "Hello, Redis!")
        value = await client.get("my_key", encoding="utf-8")
        print(f"String value: {value}")

        # Set with expiration
        await client.set("temp_key", "data", ex=10)

        # Get as integer
        await client.set("counter", "100")
        counter = await client.get("counter", encoding="int")
        print(f"Counter: {counter}")

Numeric Operations
~~~~~~~~~~~~~~~~~~

.. code-block:: python

    async def numeric_operations(client):
        # Set numeric values directly
        await client.set("counter", 100)

        # Increment operation
        new_value = await client.incr("counter")
        print(f"After increment: {new_value}")

        # Get as integer using fetch_int
        counter_value = await client.fetch_int("GET", "counter")
        print(f"Counter as int: {counter_value}")

Hash Operations
---------------

.. code-block:: python

    async def hash_operations(client):
        # Create hash with multiple fields
        await client.hset("user:1000", "name", "Alice", "age", 25, "city", "Moscow")

        # Get all hash fields
        user_data = await client.hgetall("user:1000", encoding="utf-8")
        print(f"User hash: {user_data}")

        # Get specific field
        user_name = await client.hget("user:1000", "name", encoding="utf-8")
        print(f"User name: {user_name}")

        # Set hash using mapping
        await client.hset("user:1001", mapping={"name": "Bob", "age": 30})

        # Get multiple fields
        fields = await client.hmget("user:1001", "name", "age", encoding="utf-8")
        print(f"Fields: {fields}")

        # Check field existence
        exists = await client.hexists("user:1001", "name")
        print(f"Field exists: {exists}")

        # Delete field
        deleted = await client.hdel("user:1001", "age")
        print(f"Fields deleted: {deleted}")

List Operations
---------------

.. code-block:: python

    async def list_operations(client):
        key = "mylist"

        # Push to left
        await client.lpush(key, "item1")
        await client.lpush(key, "item2")

        # Push to right
        await client.rpush(key, "item3")

        # Get list range
        items = await client.lrange(key, 0, -1, encoding="utf-8")
        print(f"All items: {items}")

        # Get list length
        length = await client.llen(key)
        print(f"List length: {length}")

        # Pop from left
        item = await client.lpop(key, encoding="utf-8")
        print(f"Popped item: {item}")

        # Blocking pop
        result = await client.blpop(key, timeout=1, encoding="utf-8")
        print(f"Blocking pop: {result}")

Sorted Set Operations
---------------------

.. code-block:: python

    async def sorted_set_operations(client):
        key = "leaderboard"

        # Add members with scores
        await client.zadd(key, "player1", 1500)
        await client.zadd(key, "player2", 1700)
        await client.zadd(key, {"player3": 1200})

        # Get range with scores
        leaders = await client.zrange(key, withscores=True)
        print(f"Leaderboard: {leaders}")

        # Get cardinality
        count = await client.zcard(key)
        print(f"Number of players: {count}")

        # Remove member
        removed = await client.zrem(key, "player1")
        print(f"Removed players: {removed}")

        # Pop min score
        min_player = await client.zpopmin(key)
        print(f"Lowest score: {min_player}")

        # Blocking pop min
        result = await client.bzpopmin(key, timeout=1)
        print(f"Blocking pop: {result}")

Stream Operations
-----------------

.. code-block:: python

    async def stream_operations(client):
        stream = "mystream"

        # Add messages to stream
        message_id = await client.xadd(stream, {"event": "login", "user_id": "123"})
        print(f"Message ID: {message_id}")

        # Add message with auto ID
        await client.xadd(stream, "event", "purchase", "amount", "99.99")

        # Read from stream
        messages = await client.xread({stream: 0}, encoding="utf-8")
        print(f"Stream messages: {messages}")

        # Read using alternative syntax
        messages = await client.xread(stream, id=0, encoding="utf-8")
        print(f"Alternative read: {messages}")

Stream Groups
~~~~~~~~~~~~~

.. code-block:: python

    async def stream_group_operations(client):
        stream = "events"
        group = "consumers"

        # Create consumer group
        await client.execute("XGROUP", "CREATE", stream, group, "$", "MKSTREAM")

        # Add message
        message_id = await client.xadd(stream, {"type": "notification"})

        # Read as consumer
        messages = await client.xread(stream, group=group, consumer="consumer1")
        print(f"Group messages: {messages}")

        # Acknowledge message
        await client.xack(stream, group, message_id)

HyperLogLog Operations
----------------------

.. code-block:: python

    async def hyperloglog_operations(client):
        key = "unique_visitors"

        # Add elements
        added = await client.pfadd(key, "user1", "user2", "user3")
        print(f"Elements added: {added}")

        # Count unique elements
        count = await client.pfcount(key)
        print(f"Unique count: {count}")

        # Merge multiple HLLs
        key2 = "more_visitors"
        await client.pfadd(key2, "user3", "user4")
        await client.pfmerge(key, key2)

        final_count = await client.pfcount(key)
        print(f"Merged count: {final_count}")

Scripting
---------

.. code-block:: python

    async def scripting_operations(client):
        # Execute Lua script
        result = await client.eval("return ARGV[1]", 0, "hello", encoding="utf-8")
        print(f"Script result: {result}")

Data Encoding and Decoding
--------------------------

.. code-block:: python

    async def encoding_examples(client):
        # Different encoding types
        await client.set("string_data", "hello")
        await client.set("int_data", 42)
        await client.set("float_data", 3.14)

        # Decode with specific encoding
        string_val = await client.get("string_data", encoding="utf-8")
        int_val = await client.get("int_data", encoding="int")
        float_val = await client.get("float_data", encoding="float")
        bytes_val = await client.fetch_bytes("GET", "string_data")

        print(f"String: {string_val}, Int: {int_val}, Float: {float_val}, Bytes: {bytes_val}")

        # Parse INFO command output
        info = await client.execute("INFO", encoding="info")
        print(f"Redis version: {info['redis_version']}")

Cluster Operations
------------------

.. code-block:: python

    async def cluster_operations(client):
        # Get cluster nodes information
        cluster_nodes = await client.execute("CLUSTER", "NODES")
        print(f"Cluster nodes: {cluster_nodes}")

        # Automatic key routing in cluster
        await client.set("user:session:123", "session_data")
        session = await client.get("user:session:123", encoding="utf-8")
        print(f"Session data: {session}")

Development
-----------

For development and building the project:

.. code-block:: bash

    # Using cargo directly
    cargo fmt
    cargo clippy
    maturin develop

    # Or using hatch
    hatch run fmt      # code formatting
    hatch run check    # style and type checking
    hatch run build    # project building

License
-------

MIT License. See LICENSE file for details.
