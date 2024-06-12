# HashRing

A Rust Implementation of a distributed hashing algorithm used to achieve load balancing and minimize the need for rehashing when the number of nodes in a system changes

## Features

- **Consistent Hashing**: Efficiently distribute data across multiple nodes.
- **Node Replication**: Each node can be replicated multiple times to ensure balanced load distribution.
- **Partitioning**: Evenly partition the hash space to manage and allocate data efficiently.

## Installation

To use this crate, add it to your `Cargo.toml`:

```toml
[dependencies]
hashring = { git = "https://github.com/TheDhejavu/hashring-rs.git" }
```

## Usage

### Creating a HashRing

First, create a `Config` to specify the replication factor and partition count. Then, create a `HashRing` using this configuration.

```rust
use hashring::{Config, HashRing, Member};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a configuration with custom replication factor and partition count
    let config = Config {
        replication_factor: 3,
        partition_count: 100,
    };

    // Create a new HashRing using the configuration
    let mut hash_ring = HashRing::new(config)?;

    // Add members to the HashRing
    hash_ring.add_member(Member {
        ip_addr: "192.168.0.1".to_string(),
        name: "node1".to_string(),
    })?;

    hash_ring.add_member(Member {
        ip_addr: "192.168.0.2".to_string(),
        name: "node2".to_string(),
    })?;

    // Retrieve a member responsible for a given key
    let key = b"some_key";
    if let Some(member) = hash_ring.get_key(key) {
        println!("Member responsible for key: {:?}", member);
    } else {
        println!("No member found for the key");
    }

    Ok(())
}
```

### Adding Members

Members can be added to the hash ring using the `add_member` method. Each member will be hashed and inserted into the ring multiple times based on the replication factor.

```rust
let member = Member {
    ip_addr: "192.168.0.3".to_string(),
    name: "node3".to_string(),
};

hash_ring.add_member(member)?;
```

### Retrieving Members

To retrieve the member responsible for a specific key, use the `get_key` method. This method hashes the key and finds the closest member in the ring.

```rust
let key = b"another_key";
if let Some(member) = hash_ring.get_key(key) {
    println!("Member responsible for key: {:?}", member);
} else {
    println!("No member found for the key");
}
```

### Configuration

The `Config` struct allows you to specify the replication factor and the number of partitions.

```rust
let config = Config {
    replication_factor: 5,  // Number of times each member is replicated
    partition_count: 200,   // Number of partitions
};
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request on GitHub.
