use crate::cluster::get_connection_addr;
use crate::cluster_routing::SlotMap;
use crate::cluster_routing::SLOT_SIZE;
use crate::{cluster::TlsMode, cluster_routing::Slot, ErrorKind, RedisError, RedisResult, Value};
use derivative::Derivative;
use log::trace;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::{collections::HashMap, sync::atomic};

#[derive(Derivative)]
#[derivative(PartialEq, PartialOrd, Ord)]
#[derive(Debug, Eq)]
pub(crate) struct TopologyView {
    #[derivative(PartialOrd = "ignore", Ord = "ignore")]
    pub(crate) hash_value: u64,
    #[derivative(PartialEq = "ignore", PartialOrd = "ignore", Ord = "ignore")]
    pub(crate) topology_value: Value,
    #[derivative(PartialEq = "ignore")]
    pub(crate) nodes_count: u16,
}

// Parse slot data from raw redis value.
pub(crate) fn parse_slots(raw_slot_resp: &Value, tls: Option<TlsMode>) -> RedisResult<Vec<Slot>> {
    // Parse response.
    let mut result = Vec::with_capacity(2);

    if let Value::Bulk(items) = raw_slot_resp {
        let mut iter = items.iter();
        while let Some(Value::Bulk(item)) = iter.next() {
            if item.len() < 3 {
                continue;
            }

            let start = if let Value::Int(start) = item[0] {
                start as u16
            } else {
                continue;
            };

            let end = if let Value::Int(end) = item[1] {
                end as u16
            } else {
                continue;
            };

            let mut nodes: Vec<String> = item
                .iter()
                .skip(2)
                .filter_map(|node| {
                    if let Value::Bulk(node) = node {
                        if node.len() < 2 {
                            return None;
                        }

                        let ip = if let Value::Data(ref ip) = node[0] {
                            String::from_utf8_lossy(ip)
                        } else {
                            return None;
                        };
                        if ip.is_empty() {
                            return None;
                        }

                        let port = if let Value::Int(port) = node[1] {
                            port as u16
                        } else {
                            return None;
                        };
                        Some(get_connection_addr(ip.into_owned(), port, tls).to_string())
                    } else {
                        None
                    }
                })
                .collect();

            if nodes.is_empty() {
                continue;
            }

            let replicas = nodes.split_off(1);
            result.push(Slot::new(start, end, nodes.pop().unwrap(), replicas));
        }
    }

    Ok(result)
}

pub(crate) fn build_slot_map(
    slot_map: &mut SlotMap,
    mut slots_data: Vec<Slot>,
    read_from_replicas: bool,
) -> RedisResult<()> {
    slots_data.sort_by_key(|slot_data| slot_data.start());
    let last_slot = slots_data.iter().try_fold(0, |prev_end, slot_data| {
        if prev_end != slot_data.start() {
            return Err(RedisError::from((
                ErrorKind::ResponseError,
                "Slot refresh error.",
                format!(
                    "Received overlapping slots {} and {}..{}",
                    prev_end,
                    slot_data.start(),
                    slot_data.end()
                ),
            )));
        }
        Ok(slot_data.end() + 1)
    })?;

    if last_slot != SLOT_SIZE {
        return Err(RedisError::from((
            ErrorKind::ResponseError,
            "Slot refresh error.",
            format!("Lacks the slots >= {last_slot}"),
        )));
    }
    slot_map.clear();
    slot_map.fill_slots(&slots_data, read_from_replicas);
    trace!("{:?}", slot_map);
    Ok(())
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

pub(crate) fn calculate_topology(
    topology_views: Vec<Value>,
    retries: Option<Arc<atomic::AtomicUsize>>, // TODO: change to usize
    tls_mode: Option<TlsMode>,
    read_from_replicas: bool,
    num_of_queried_nodes: usize,
) -> Result<SlotMap, RedisError> {
    if topology_views.is_empty() {
        return Err(RedisError::from((
            ErrorKind::ResponseError,
            "Slot refresh error: All CLUSTER SLOTS results are errors",
        )));
    }
    const MIN_ACCURACY_RATE: f32 = 0.2;
    let mut hash_view_map = HashMap::new();
    let mut new_slots = SlotMap::new();
    for view in topology_views {
        let hash_value = calculate_hash(&view);
        let topology_entry = hash_view_map.entry(hash_value).or_insert(TopologyView {
            hash_value,
            topology_value: view,
            nodes_count: 0,
        });
        topology_entry.nodes_count += 1;
    }
    let mut most_frequent_topology: Option<&TopologyView> = None;
    let mut has_more_than_a_single_max = false;
    let vec_iter = hash_view_map.values();
    // Find the most frequent topology view
    for curr_view in vec_iter {
        let max_view = match most_frequent_topology {
            Some(view) => view,
            None => {
                most_frequent_topology = Some(curr_view);
                continue;
            }
        };
        match max_view.cmp(curr_view) {
            std::cmp::Ordering::Less => {
                most_frequent_topology = Some(curr_view);
                has_more_than_a_single_max = false;
            }
            std::cmp::Ordering::Equal => has_more_than_a_single_max = true,
            std::cmp::Ordering::Greater => continue,
        }
    }
    let most_frequent_topology = match most_frequent_topology {
        Some(view) => view,
        None => unreachable!(),
    };
    if has_more_than_a_single_max {
        // More than a single most frequent view was found
        if (retries.is_some() && retries.unwrap().fetch_sub(1, atomic::Ordering::SeqCst) == 1)
            || num_of_queried_nodes < 3
        {
            // If it's the last retry, or if we it's a 2-nodes cluster, we'll return all found topologies to be checked by the caller
            for (idx, topology_view) in hash_view_map.iter() {
                match parse_slots(&topology_view.topology_value, tls_mode)
                    .and_then(|v| build_slot_map(&mut new_slots, v, read_from_replicas))
                {
                    Ok(_) => {
                        return Ok(new_slots);
                    }
                    Err(e) => {
                        // If it's the last view, raise the error
                        if *idx as usize == hash_view_map.len() - 1 {
                            return Err(e);
                        } else {
                            continue;
                        }
                    }
                }
            }
        }
        return Err(RedisError::from((
            ErrorKind::ResponseError,
            "Slot refresh error: Couldn't get a majority in topology views",
        )));
    }
    // Calculates the accuracy of the topology view by checking how many nodes share this view out of the total number queried
    let accuracy_rate = most_frequent_topology.nodes_count as f32 / num_of_queried_nodes as f32;
    if accuracy_rate >= MIN_ACCURACY_RATE {
        parse_slots(&most_frequent_topology.topology_value, tls_mode)
            .and_then(|v| build_slot_map(&mut new_slots, v, read_from_replicas))?;
        Ok(new_slots)
    } else {
        Err(RedisError::from((
            ErrorKind::ResponseError,
            "Slot refresh error: The accuracy of the topology view is too low",
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_routing::SlotAddrs;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn test_topology_calculator() {
        let single_node_view = Value::Bulk(vec![Value::Bulk(vec![
            Value::Int(0_i64),
            Value::Int(16383_i64),
            Value::Bulk(vec![
                Value::Data("node1".as_bytes().to_vec()),
                Value::Int(6379_i64),
            ]),
        ])]);
        let single_node_missing_slots_view = Value::Bulk(vec![Value::Bulk(vec![
            Value::Int(0_i64),
            Value::Int(4000_i64),
            Value::Bulk(vec![
                Value::Data("node1".as_bytes().to_vec()),
                Value::Int(6379_i64),
            ]),
        ])]);
        let two_nodes_full_coverage_view = Value::Bulk(vec![
            Value::Bulk(vec![
                Value::Int(0_i64),
                Value::Int(4000_i64),
                Value::Bulk(vec![
                    Value::Data("node1".as_bytes().to_vec()),
                    Value::Int(6379_i64),
                ]),
            ]),
            Value::Bulk(vec![
                Value::Int(4001_i64),
                Value::Int(16383_i64),
                Value::Bulk(vec![
                    Value::Data("node2".as_bytes().to_vec()),
                    Value::Int(6380_i64),
                ]),
            ]),
        ]);
        let two_nodes_missing_slots_view = Value::Bulk(vec![
            Value::Bulk(vec![
                Value::Int(0_i64),
                Value::Int(3000_i64),
                Value::Bulk(vec![
                    Value::Data("node3".as_bytes().to_vec()),
                    Value::Int(6381_i64),
                ]),
            ]),
            Value::Bulk(vec![
                Value::Int(4001_i64),
                Value::Int(16383_i64),
                Value::Bulk(vec![
                    Value::Data("node4".as_bytes().to_vec()),
                    Value::Int(6382_i64),
                ]),
            ]),
        ]);

        // 4 nodes queried (1 error): Has a majority, single_node_view should be chosen
        let mut queried_nodes: usize = 4;
        let topology_results = vec![
            single_node_view.clone(),
            single_node_view.clone(),
            two_nodes_full_coverage_view.clone(),
        ];
        let node1_addr = SlotAddrs::new("node1:6379".to_string(), None);
        let node2_addr = SlotAddrs::new("node2:6380".to_string(), None);
        let topology_view =
            calculate_topology(topology_results, None, None, false, queried_nodes).unwrap();
        let res: Vec<_> = topology_view.values().collect();
        let excepted = vec![&node1_addr];
        assert_eq!(res, excepted);

        // 3 nodes queried: No majority, should return an error
        queried_nodes = 3;
        let topology_results = vec![
            single_node_view,
            two_nodes_full_coverage_view.clone(),
            two_nodes_missing_slots_view.clone(),
        ];
        let topology_view = calculate_topology(topology_results, None, None, false, queried_nodes);
        assert!(topology_view.is_err());

        // 3 nodes queried:: No majority, last retry, should get the view that has a full slot coverage
        let topology_results = vec![
            single_node_missing_slots_view,
            two_nodes_full_coverage_view.clone(),
            two_nodes_missing_slots_view.clone(),
        ];
        let topology_view = calculate_topology(
            topology_results,
            Some(Arc::new(AtomicUsize::new(1))),
            None,
            false,
            queried_nodes,
        )
        .unwrap();
        let res: Vec<_> = topology_view.values().collect();
        let excepted: Vec<&SlotAddrs> = vec![&node1_addr, &node2_addr];
        assert_eq!(res, excepted);

        //  2 nodes queried: No majority, should get the view that has a full slot coverage
        queried_nodes = 2;
        let topology_results = vec![two_nodes_full_coverage_view, two_nodes_missing_slots_view];
        let topology_view =
            calculate_topology(topology_results, None, None, false, queried_nodes).unwrap();
        let res: Vec<_> = topology_view.values().collect();
        assert_eq!(res, excepted);
    }
}
