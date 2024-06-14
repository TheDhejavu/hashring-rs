# HashRing ⚠️ (WIP)

A Rust Implementation of a distributed hashing algorithm used to achieve load balancing and minimize the need for rehashing when the number of nodes in a system changes


>
> What is consistent hashing ?
>
> "Consistent hashing is a caching protocol for distributed networks that can be used to decrease or eliminate the occurrences of 'hot spots'."
>
> *Source: [Consistent Hashing and Random Trees: Distributed Caching Protocols for Relieving Hot Spots on the World Wide Web](https://www.cs.princeton.edu/courses/archive/fall09/cos518/papers/chash.pdf)*

## Features

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
use std::sync::Arc;

use hashring::{Config, HashRing, Node as HashRingNode};

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct Node {
    pub ip_addr: String,
    pub name: String,
}

impl HashRingNode for Node {
    fn id(&self) -> &str {
        &self.name
    }
}

fn main() {
    let config = Config {
        replication_factor: 3,
        partition_count: 100,
    };

    // Create a new HashRing using the configuration
    let mut hash_ring = HashRing::new(config).unwrap();

    // Add nodes to the HashRing
    let _ = hash_ring.add_node(Arc::new(Node {
        ip_addr: "192.168.0.1".to_string(),
        name: "node1".to_string(),
    }));

    let _ = hash_ring.add_node(Arc::new(Node {
        ip_addr: "192.168.0.2".to_string(),
        name: "node2".to_string(),
    }));

    // Retrieve a node responsible for a given key
    let key = b"some_random_key";
    if let Some(node) = hash_ring.get_key(key) {
        // Print the node information using the Display implementation
        println!("Node responsible for key {}: {}", String::from_utf8_lossy(key), node);
    } else {
        println!("No node found for the key");
    }
}
```

### Adding Nodes

Nodes can be added to the hash ring using the `add_node` method. Each node will be hashed and inserted into the ring multiple times based on the replication factor.

```rust
let node = Arc::new(Node {
    ip_addr: "192.168.0.3:5000".to_string(),
    name: "node3".to_string(),
});

hash_ring.add_node(node)?;
```

### Retrieving nodes

To retrieve the node responsible for a specific key, use the `get_key` method. This method hashes the key and finds the closest node in the ring.

```rust
let key = b"another_key";
if let Some(node) = hash_ring.get_key(key) {
    println!("node responsible for key: {:?}", node);
} else {
    println!("No node found for the key");
}
```

### Configuration

The `Config` struct allows you to specify the replication factor and the number of partitions.

```rust
let config = Config {
    replication_factor: 5,  // Number of times each node is replicated
    partition_count: 200,   // Number of partitions
};
```

## Contributing

Contributions are welcome! Please open an issue if you come accross any!