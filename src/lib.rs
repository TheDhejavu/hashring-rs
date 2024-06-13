// IDEA
//.......
// 1. Hash the name of each member of the ring and insert each hash into a sorted set using a BTreeMap. 
//    - During this process, factor in the replication factor. The replication factor represents 
//      the total number of times each node/member should be replicated in the ring to balance the 
//      distribution of keys during allocation. Each replica will be assigned a different hash to ensure 
//      an even distribution of the member nodes across the hash space.
//    - This ensures that even if some nodes fail, the load will be redistributed among the remaining nodes smoothly.

// 2. Create partitions based on the partition count defined in the configuration. 
//    - This partitioning allows for an even distribution/allocation of nodes/members to different partitions.
//    - Each partition will map to a specific range of hashes, ensuring that the data is evenly distributed 
//      across all available nodes. This helps in managing the load and providing fault tolerance.

use std::{collections::{BTreeMap, HashMap}, error::Error};
use xxhash_rust::xxh3::xxh3_64;

const DEFAULT_PARTITION_COUNT: usize = 271;
const DEFAULT_REPLICATION_FACTOR: usize = 20;

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
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.partition_count == 0 {
            return Err("Partition count must be greater than 0".into());
        }

        if self.replication_factor == 0 {
            return Err("Replication factor must be greater than 0".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct HashRing {
    config: Config,
    members: HashMap<String, Member>,
    sorted_hash_set: BTreeMap<u64, Member>,
    partitions: HashMap<usize, Vec<Member>>,
}

#[derive(Debug, Clone)]
pub struct Member {
    pub ip_addr: String,
    pub name: String,
}

impl HashRing {
    pub fn new(config: Config) -> Result<Self, Box<dyn Error>> {
        config.validate()?;
        Ok(Self {
            members: HashMap::new(),
            sorted_hash_set: BTreeMap::new(),
            partitions: HashMap::new(),
            config,
        })
    }

    pub fn add_member(&mut self, member: Member) -> Result<Member, Box<dyn Error>> {
        if self.members.contains_key(&member.name) {
            return Err("Member already exists".into());
        }

        for i in 0..self.config.replication_factor {
            let hash = self.hash_with_replica_idx(&member.name, i);
            self.sorted_hash_set.insert(hash, member.clone());
        }

        self.members.insert(member.name.clone(), member.clone());
        self.distribute_partitions();

        Ok(member)
    }

    fn hash_with_replica_idx(&self, name: &str, replica: usize) -> u64 {
        let data = format!("{}:{}", name, replica);
        xxh3_64(data.as_bytes())
    }

    fn hash_partition_id(&self, part_id: usize) -> u64 {
        xxh3_64(&part_id.to_ne_bytes())
    }

    fn hash_key(&self, key: &[u8]) -> u64 {
        xxh3_64(key)
    }

    fn distribute_partitions(&mut self) {
        self.partitions.clear();

        for part_id in 0..self.config.partition_count {
            let hashed_part_id = self.hash_partition_id(part_id);
            let idx = self.find_closest_idx(hashed_part_id);
            if let Some(member) = self.sorted_hash_set.get(&idx) {
                self.partitions.entry(part_id)
                    .or_insert_with(Vec::new)
                    .push(member.clone());   
            }
        }
    }

    fn find_closest_idx(&self, hashed_part_id: u64) -> u64 {
        self.sorted_hash_set
            .range(hashed_part_id..)
            .next()
            .or_else(|| self.sorted_hash_set.iter().next())
            .map(|(k, _)| *k)
            .unwrap_or(0)
    }

    pub fn get_key(&self, key: &[u8]) -> Option<Member> {
        let hashed_key = self.hash_key(key);
        self.sorted_hash_set
            .range(hashed_key..)
            .next()
            .or_else(|| self.sorted_hash_set.iter().next())
            .map(|(_, member)| member.clone())
    }
}

// Tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_member() {
        let config = Config {
            replication_factor: 3,
            partition_count: 100,
        };

        let mut hash_ring = HashRing::new(config.clone()).unwrap();

        let member1 = hash_ring.add_member(Member {
            ip_addr: "170.01.01.1".to_string(),
            name: "node1".to_string(),
        });

        assert_eq!(member1.is_ok(), true);

        let member2 = hash_ring.add_member(Member {
            ip_addr: "170.01.01.2".to_string(),
            name: "node2".to_string(),
        });
        assert_eq!(member2.is_ok(), true);

        assert_eq!(hash_ring.members.len(), 2);
        assert!(hash_ring.partitions.len() <= config.partition_count);
    }

    #[test]
    fn test_get_key() {
        let config = Config {
            replication_factor: 3,
            partition_count: 100,
        };

        let mut hash_ring = HashRing::new(config).unwrap();

        hash_ring.add_member(Member {
            ip_addr: "170.01.01.1".to_string(),
            name: "node1".to_string(),
        }).unwrap();

        hash_ring.add_member(Member {
            ip_addr: "170.01.01.2".to_string(),
            name: "node2".to_string(),
        }).unwrap();

        let key1 = b"some_key";
        let member1 = hash_ring.get_key(key1);
        assert!(member1.is_some());

        let key2 = b"100";
        let member2 = hash_ring.get_key(key2);
        assert!(member2.is_some());
    }
}
