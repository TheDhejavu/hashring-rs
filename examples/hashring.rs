extern crate hashring;

use std::sync::Arc;

use hashring::{Config, HashRing, Node as HashRingNode};

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct Node<'a> {
    pub ip_addr: &'a str,
    pub name: &'a str,
}

impl<'a> HashRingNode<'a> for Node<'a> {
    fn id(&self) -> &'a str {
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
        ip_addr: "192.168.0.1",
        name: "node1",
    }));

    let _ = hash_ring.add_node(Arc::new(Node {
        ip_addr: "192.168.0.2",
        name: "node2",
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