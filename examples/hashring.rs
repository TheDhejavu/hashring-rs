extern crate hashring;

use hashring::{Config, HashRing, Member};

fn main() {
    let config = Config {
        replication_factor: 3,
        partition_count: 100,
    };

    // Create a new HashRing using the configuration
    let mut hash_ring = HashRing::new(config).unwrap();

    // Add members to the HashRing
    let _ = hash_ring.add_member(Member {
        ip_addr: "192.168.0.1".to_string(),
        name: "node1".to_string(),
    });

    let _ = hash_ring.add_member(Member {
        ip_addr: "192.168.0.2".to_string(),
        name: "node2".to_string(),
    });

    // Retrieve a member responsible for a given key
    let key = b"500";
    if let Some(member) = hash_ring.get_key(key) {
        println!("Member responsible for key: {:?}", member);
    } else {
        println!("No member found for the key");
    }
}