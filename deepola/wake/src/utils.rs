use std::{thread::{ThreadId, current}, time::{UNIX_EPOCH, SystemTime}};

pub fn log_node_mapping(node_id: &str, thread_id: ThreadId) {
    log::warn!("[logging] (type,node-thread-map) (node,{node_id}) (thread,{:?})", thread_id);
}

pub fn log_node_edge(src_node: &str, dest_node: &str) {
    log::warn!("[logging] (type,node-edge) (src,{src_node}) (dest,{dest_node})");
}

pub fn log_event(task: &str, action: &str) {
    let current_thread = current().id();
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    log::warn!("[logging] (type,event) (thread,{:?}) (task,{task}) (action,{action}) (timestamp,{timestamp})", current_thread);
}
