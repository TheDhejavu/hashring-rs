// Hash Ring Implementation
// 
// --------------
// 1. Hash the name of each node of the ring and insert each hash into a sorted set using a BTreeMap. 
//    - During this process, factor in the replication factor. The replication factor represents 
//      the total number of times each node should be replicated in the ring to balance the 
//      distribution of keys during allocation. Each replica will be assigned a different hash to ensure 
//      an even distribution of the nodes across the hash space.
//    - This ensures that even if some nodes fail, the load will be redistributed among the remaining nodes smoothly.

// 2. Create partitions based on the partition count defined in the configuration. 
//    - This partitioning allows for an even distribution of nodes to different partitions.
//    - Each partition will map to a specific range of hashes, ensuring that the data is evenly distributed 
//      across all available nodes. This helps in managing the load and providing fault tolerance.
// 
// Example Usage:
// --------------
/// ```rust
// use core::fmt;
// use std::sync::Arc;
//
// use hashring::{Config, HashRing, Node as HashRingNode};
//
// #[derive(Clone, Eq, PartialEq, Hash, Debug)]
// pub struct Node {
//     pub ip_addr: String,
//     pub name: String,
// }
//
// impl fmt::Display for Node {
//     fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
//         write!(fmt, "{}:{}", self.name, self.ip_addr)
//     }
// }
//
// impl HashRingNode for Node {
//     fn id(&self) -> &str {
//         &self.ip_addr
//     }
// }
//
// fn main() {
//     let config = Config {
//         replication_factor: 3,
//         partition_count: 100,
//     };
//
//     // Create a new HashRing using the configuration
//     let mut hash_ring = HashRing::new(config).unwrap();
//
//     // Add nodes to the HashRing
//     let _ = hash_ring.add_node(Arc::new(Node {
//         ip_addr: "192.168.0.1".to_string(),
//         name: "node1".to_string(),
//     }));
//
//     let _ = hash_ring.add_node(Arc::new(Node {
//         ip_addr: "192.168.0.2".to_string(),
//         name: "node2".to_string(),
//     }));
//
//     // Retrieve a node responsible for a given key
//     let key = b"500";
//     if let Some(node) = hash_ring.get_key(key) {
//         // Print the node information using the Display implementation
//         println!("Node responsible for key {}: {}", String::from_utf8_lossy(key), node);
//     } else {
//         println!("No node found for the key");
//     }
// }
/// ```


use core::fmt;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::error::Error;
use std::fmt::Debug;
use std::hash::{BuildHasher, BuildHasherDefault, Hasher};
use std::sync::{Arc, RwLock};
use xxhash_rust::xxh3::Xxh3;

type XxHash64Hasher = BuildHasherDefault<Xxh3>;

const DEFAULT_PARTITION_COUNT: usize = 271;
const DEFAULT_REPLICATION_FACTOR: usize = 20;

pub trait Node<'a>: Send + Sync + Debug {
    fn id(&self) -> &'a str;
}

impl<'a> fmt::Display for dyn Node<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "{}", self.id())
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub replication_factor: usize,
    pub partition_count: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            replication_factor: DEFAULT_REPLICATION_FACTOR,
            partition_count: DEFAULT_PARTITION_COUNT,
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<(),Box<dyn Error>> {
        if self.partition_count == 0 {
            return Err("Partition count must be greater than 0")?;
        }
        if self.replication_factor == 0 {
            return Err("Replication factor must be greater than 0")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct HashRing<'a, H = XxHash64Hasher> {
    config: Config,
    hasher: H,
    nodes: Arc<RwLock<HashMap<String, Arc<dyn Node<'a> + 'a>>>>,
    sorted_nodes_hash_set: Arc<RwLock<BTreeMap<u64, Arc<dyn Node<'a> + 'a>>>>,
    partitions: Arc<RwLock<HashMap<usize, Arc<dyn Node<'a> + 'a>>>>,
}

impl<'a> HashRing<'a, XxHash64Hasher> {
    pub fn new(config: Config) -> Result<HashRing<'a, XxHash64Hasher>, Box<dyn Error>> {
        HashRing::with_hasher(config, XxHash64Hasher::default())
    }
}

impl<'a, H> HashRing<'a, H>
where
    H: BuildHasher,
{
    /// Creates a new `HashRing` with the specified configuration and hasher.
    ///
    /// # Arguments
    ///
    /// * `config` - A `Config` struct that defines the replication factor and partition count.
    /// * `hasher` - A custom hasher that implements the `BuildHasher` trait.
    ///
    /// # Example
    ///
    /// ```
    /// use core::hash::BuildHasherDefault;
    /// use hashring::Config;
    /// use hashring::HashRing;
    /// 
    /// type CustomBuildHasher = BuildHasherDefault<std::collections::hash_map::DefaultHasher>;
    /// let config = Config::default();
    /// let hash_ring = HashRing::with_hasher(config, CustomBuildHasher::default()).unwrap();
    /// ```
    pub fn with_hasher(config: Config, hasher: H) -> Result<HashRing<'a, H>, Box<dyn Error>> {
        config.validate()?;
        let hash_ring = HashRing {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            sorted_nodes_hash_set: Arc::new(RwLock::new(BTreeMap::new())),
            partitions: Arc::new(RwLock::new(HashMap::new())),
            config,
            hasher,
        };

        Ok(hash_ring)
    }

    /// Adds a node to the `HashRing`.
    ///
    /// # Arguments
    ///
    /// * `node` - An `Arc` containing a `Node` to be added to the hash ring.
    ///
    /// # Returns
    ///
    /// * `Result<Arc<dyn Node<'a> + 'a>, Box<dyn Error>>` - On success, returns the added node wrapped in an `Arc`.
    ///
    /// # Example
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::fmt::Debug;
    /// use std::sync::RwLock;
    /// use std::collections::{BTreeMap, HashMap};
    /// use hashring::{HashRing, Config, Node};
    /// 
    /// #[derive(Debug)]
    /// struct MyNode<'a> {
    ///     name: &'a str,
    ///     ip_addr: String,
    /// }
    /// 
    /// impl<'a> Node<'a> for MyNode<'a> {
    ///     fn id(&self) -> &'a str {
    ///         self.name
    ///     }
    /// }
    /// 
    /// let config = Config::default();
    /// let mut hash_ring = HashRing::new(config).unwrap();
    /// 
    /// let node = Arc::new(MyNode {
    ///     ip_addr: "192.168.0.1".to_string(),
    ///     name: "node1",
    /// });
    ///     
    /// hash_ring.add_node(node).unwrap();
    /// ```
    pub fn add_node(&mut self, node: Arc<dyn Node<'a> + 'a>) -> Result<Arc<dyn Node<'a> + 'a>, Box<dyn Error>> {
        let mut nodes = self.nodes.write().map_err(|_| "unable to acquire lock")?;
        if nodes.contains_key(node.id()) {
            return Err("node already exist".into());
        }

        let mut sorted_set = self.sorted_nodes_hash_set.write().map_err(|_| "unable to acquire lock")?;
        for i in 0..self.config.replication_factor {
            let hash = self.hash_with_replica_idx(&node.id(), i);
            sorted_set.insert(hash, node.clone());
        }

        nodes.insert(node.id().to_string(), node.clone());
        drop(nodes);
        drop(sorted_set);

        self.distribute_partitions();

        Ok(node)
    }

    /// Removes a node from the `HashRing`.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the node to be removed.
    ///
    /// # Returns
    ///
    /// * `Result<(), Box<dyn Error>>` - On success, returns `Ok(())`.
    ///
    /// # Example
    ///
    /// ```
    /// let config = Config::default();
    /// let mut hash_ring = HashRing::new(config).unwrap();
    /// use std::sync::Arc;
    /// use std::fmt::Debug;
    /// use std::sync::RwLock;
    /// use std::collections::{BTreeMap, HashMap};
    /// use hashring::{HashRing, Config, Node};
    /// 
    /// #[derive(Debug)]
    /// struct MyNode<'a> {
    ///     name: &'a str,
    ///     ip_addr: String,
    /// }
    /// 
    /// impl<'a> Node<'a> for MyNode<'a> {
    ///     fn id(&self) -> &'a str {
    ///         self.name
    ///     }
    /// }
    /// 
    /// let config = Config::default();
    /// let mut hash_ring = HashRing::new(config).unwrap();
    /// 
    /// let node = Arc::new(MyNode {
    ///     ip_addr: "192.168.0.1".to_string(),
    ///     name: "node1",
    /// });
    /// 
    /// hash_ring.add_node(node.clone()).unwrap();
    /// hash_ring.remove_node(node.id()).unwrap();
    /// ```
    pub fn remove_node(&mut self, id: &str) -> Result<(), Box<dyn Error>> {
        let mut sorted_set = self.sorted_nodes_hash_set.write().map_err(|_| "unable to acquire lock")?;
        let mut nodes = self.nodes.write().map_err(|_| "unable to acquire lock")?;
        if !nodes.contains_key(id) {
            return Err("node not found".into());
        }

        for i in 0..self.config.replication_factor {
            let hash = self.hash_with_replica_idx(id, i);
            sorted_set.remove(&hash);
        }

        nodes.remove(id);
        drop(nodes);
        drop(sorted_set);

        self.distribute_partitions();
        Ok(())
    }

    fn hash_with_replica_idx(&self, name: &str, replica: usize) -> u64 {
        let data = format!("{}:{}", name, replica);
        let mut hasher: <H as BuildHasher>::Hasher = self.hasher.build_hasher();
        hasher.write(data.as_bytes());
        hasher.finish()
    }

    fn hash_partition_id(&self, part_id: usize) -> u64 {
        let mut hasher: <H as BuildHasher>::Hasher = self.hasher.build_hasher();
        hasher.write(&part_id.to_ne_bytes());
        hasher.finish()
    }

    fn hash_key(&self, key: &[u8]) -> u64 {
        let mut hasher: <H as BuildHasher>::Hasher = self.hasher.build_hasher();
        hasher.write(key);
        hasher.finish()
    }

    fn distribute_partitions(&self) {
        let sorted_set = self.sorted_nodes_hash_set.read().unwrap();
        let mut partitions = self.partitions.write().unwrap();
        partitions.clear();

        for part_id in 0..self.config.partition_count {
            let hashed_part_id = self.hash_partition_id(part_id);
            let idx = self.find_closest_idx(hashed_part_id);
            if let Some(node) = sorted_set.get(&idx) {
                partitions.insert(part_id, node.clone());
            }
        }
    }

    fn find_closest_idx(&self, hashed_part_id: u64) -> u64 {
        let sorted_set = self.sorted_nodes_hash_set.read().unwrap();
        sorted_set
            .range(hashed_part_id..)
            .next()
            .or_else(|| sorted_set.iter().next())
            .map(|(k, _)| *k)
            .unwrap_or(0)
    }

    /// Retrieves the node responsible for the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key for which the responsible node is to be found.
    ///
    /// # Returns
    ///
    /// * `Option<Arc<dyn Node<'a> + 'a>>` - The node responsible for the given key, if found.
    ///
    /// # Example
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::fmt::Debug;
    /// use std::sync::RwLock;
    /// use std::collections::{BTreeMap, HashMap};
    /// use hashring::{HashRing, Config, Node};
    /// 
    /// #[derive(Debug)]
    /// struct MyNode<'a> {
    ///     name: &'a str,
    /// }
    /// 
    /// impl<'a> Node<'a> for MyNode<'a> {
    ///     fn id(&self) -> &'a str {
    ///         self.name
    ///     }
    /// }
    /// 
    /// let config = Config::default();
    /// let mut hash_ring = HashRing::new(config).unwrap();
    /// 
    /// let node = Arc::new(MyNode {
    ///     name: "node1",
    /// });
    /// 
    /// hash_ring.add_node(node).unwrap();
    ///
    /// let key = b"some_key";
    /// if let Some(node) = hash_ring.get_key(key) {
    ///     println!("Node responsible for key: {}", node);
    /// }
    /// ```
    pub fn get_key(&self, key: &[u8]) -> Option<Arc<dyn Node<'a> + 'a>> {
        let hashed_key = self.hash_key(key);
        let sorted_set = self.sorted_nodes_hash_set.read().ok()?;
        sorted_set
            .range(hashed_key..)
            .next()
            .or_else(|| sorted_set.iter().next())
            .map(|(_, node)| node.clone())
    }

    /// Returns a mapping of nodes to their number of virtual nodes in the hash ring.
    ///
    /// # Returns
    ///
    /// * `HashMap<String, usize>` - A mapping of node IDs to their virtual node count.
    ///
    /// # Example
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::fmt::Debug;
    /// use std::sync::RwLock;
    /// use std::collections::{BTreeMap, HashMap};
    /// use hashring::{HashRing, Config, Node};
    /// 
    /// #[derive(Debug)]
    /// struct MyNode<'a> {
    ///     name: &'a str,
    /// }
    /// 
    /// impl<'a> Node<'a> for MyNode<'a> {
    ///     fn id(&self) -> &'a str {
    ///         self.name
    ///     }
    /// }
    /// 
    /// let config = Config::default();
    /// let mut hash_ring = HashRing::new(config).unwrap();
    /// 
    /// let node = Arc::new(MyNode {
    ///     name: "node1",
    /// });
    /// 
    /// hash_ring.add_node(node).unwrap();
    ///
    /// let virtual_nodes = hash_ring.virtual_nodes_per_node();
    /// for (node_id, count) in virtual_nodes {
    ///     println!("Node ID: {}, Virtual Nodes: {}", node_id, count);
    /// }
    /// ```
    pub fn virtual_nodes_per_node(&self) -> HashMap<String, usize> {
        let mut virtual_nodes = HashMap::new();
        let sorted_set = self.sorted_nodes_hash_set.read().unwrap();
        for node in sorted_set.values() {
            *virtual_nodes.entry(node.id().to_string()).or_insert(0) += 1;
        }
        virtual_nodes
    }

    /// Retrieves the preference list of nodes responsible for the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key for which the preference list is to be found.
    ///
    /// # Returns
    ///
    /// * `Vec<Arc<dyn Node<'a> + 'a>>` - A vector of nodes in the preference list for the given key.
    ///
    /// # Example
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::fmt::Debug;
    /// use std::sync::RwLock;
    /// use std::collections::{BTreeMap, HashMap};
    /// use hashring::{HashRing, Config, Node};
    /// 
    /// #[derive(Debug)]
    /// struct MyNode<'a> {
    ///     name: &'a str,
    /// }
    /// 
    /// impl<'a> Node<'a> for MyNode<'a> {
    ///     fn id(&self) -> &'a str {
    ///         self.name
    ///     }
    /// }
    /// 
    /// let config = Config::default();
    /// let mut hash_ring = HashRing::new(config).unwrap();
    /// 
    /// let node = Arc::new(MyNode {
    ///     name: "node1",
    /// });
    /// 
    /// hash_ring.add_node(node).unwrap();
    ///
    /// let key = b"some_key";
    /// let preference_list = hash_ring.get_preference_list(key);
    /// for node in preference_list {
    ///     println!("Node in preference list: {:?}", node);
    /// }
    /// ```
    pub fn get_preference_list(&self, key: &[u8]) -> Vec<Arc<dyn Node<'a> + 'a>> {
        let mut preference_list: Vec<Arc<dyn Node<'a> + 'a>> = Vec::new();
        let hashed_key = self.hash_key(key);
        let sorted_set = self.sorted_nodes_hash_set.read().unwrap();
        let mut unique_nodes = HashSet::new();

        for (_, node) in sorted_set.range(hashed_key..).chain(sorted_set.range(..hashed_key)) {
            if unique_nodes.insert(node.id().to_string()) {
                preference_list.push(node.clone());
                if preference_list.len() >= self.config.replication_factor {
                    break;
                }
            }
        }

        preference_list
    }
}
// Tests
#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Eq, PartialEq, Hash, Debug)]
    pub struct TestNode<'a> {
        pub ip_addr: String,
        pub name: &'a str,
    }

    impl<'a> Node<'a> for TestNode<'a> {
        fn id(&self) -> &'a str {
            &self.name
        }
    }

    #[test]
    fn test_add_node() {
        let config = Config {
            replication_factor: 3,
            partition_count: 100,
        };

        let mut hash_ring = HashRing::new(config.clone()).unwrap();

        let node1 = hash_ring.add_node(Arc::new(TestNode {
            ip_addr: "180.01.01.2:5000".to_string(),
            name: "node1",
        }));

        assert_eq!(node1.is_ok(), true);

        let node2 = hash_ring.add_node(Arc::new(TestNode {
            ip_addr: "170.01.01.2:5000".to_string(),
            name: "node2"
        }));
        assert_eq!(node2.is_ok(), true);

        assert_eq!(hash_ring.nodes.read().unwrap().len(), 2);
        assert!(hash_ring.partitions.read().unwrap().len() <= config.partition_count);
    }

    #[test]
    fn test_get_key() {
        let config = Config {
            replication_factor: 3,
            partition_count: 100,
        };

        let mut hash_ring = HashRing::new(config).unwrap();

        hash_ring.add_node(Arc::new(
            TestNode {
                ip_addr: "170.01.01.1:1000".to_string(),
                name: "node1"
            }
        )).unwrap();

        hash_ring.add_node(Arc::new(
            TestNode {
                ip_addr: "170.01.01.2:2000".to_string(),
                name: "node2"
            }
        )).unwrap();

        let key1 = b"some_key";
        let node1 = hash_ring.get_key(key1);
        assert!(node1.is_some());

        let key2 = b"100";
        let node2 = hash_ring.get_key(key2);
        assert!(node2.is_some());
    }

    #[test]
    fn test_virtual_nodes_per_node() {
        let config = Config {
            partition_count: 10,
            replication_factor: 2,
        };
        let mut hash_ring = HashRing::new(config).unwrap();

        let node1 = Arc::new(TestNode {
            ip_addr: "127.0.0.1:5000".to_string(),
            name: "node1"
        });

        let node2 = Arc::new(TestNode {
            ip_addr: "127.0.0.1:6000".to_string(),
            name: "node2"
        });

        hash_ring.add_node(node1.clone()).unwrap();
        hash_ring.add_node(node2.clone()).unwrap();

        let virtual_nodes = hash_ring.virtual_nodes_per_node();
        assert_eq!(virtual_nodes.get("node1"), Some(&2));
        assert_eq!(virtual_nodes.get("node2"), Some(&2));
    }

    #[test]
    fn test_preference_list() {
        let config = Config {
            replication_factor: 3,
            partition_count: 100,
        };

        let mut hash_ring = HashRing::new(config).unwrap();

        hash_ring.add_node(Arc::new(
            TestNode {
                ip_addr: "170.01.01.1".to_string(),
                name: "node1"
            }
        )).unwrap();

        hash_ring.add_node(Arc::new(
            TestNode {
                ip_addr: "170.01.01.2".to_string(),
                name: "node2"
            }
        )).unwrap();

        let key1 = b"some_key";
        let preference_list = hash_ring.get_preference_list(key1);
        assert_eq!(preference_list.len(), 2);
    }

    #[test]
    fn test_distribute_partitions() {
        type CustomBuildHasher = BuildHasherDefault<std::collections::hash_map::DefaultHasher>;

        let hasher = CustomBuildHasher::default();
        let config = Config {
            replication_factor: 3,
            partition_count: 10,
        };

        let mut hash_ring : HashRing<CustomBuildHasher>= HashRing::with_hasher(config.clone(), hasher).unwrap();

        let node1 = Arc::new(
            TestNode {
                ip_addr: "127.0.0.1:5000".to_string(),
                name: "node1"
            }
        );

        let node2 = Arc::new(
            TestNode {
                ip_addr: "127.0.0.1:6000".to_string(),
                name: "node2"
            }
        );

        hash_ring.add_node(node1.clone()).unwrap();
        hash_ring.add_node(node2.clone()).unwrap();

        let partitions = &hash_ring.partitions;

        assert_eq!(partitions.read().unwrap().len(), 10);
    }
}
