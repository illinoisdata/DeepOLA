use std::{thread::{ThreadId, current}, time::{UNIX_EPOCH, SystemTime}};

pub fn log_node_mapping(node_id: &str, thread_id: ThreadId) {
    log::info!("[logging] (type,node-thread-map) (node,{node_id}) (thread,{:?})", thread_id);
}

pub fn log_node_edge(src_node: &str, dest_node: &str) {
    log::info!("[logging] (type,node-edge) (src,{src_node}) (dest,{dest_node})");
}

pub fn log_event(task: &str, action: &str) {
    let current_thread = current().id();
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    log::info!("[logging] (type,event) (thread,{:?}) (task,{task}) (action,{action}) (timestamp,{timestamp})", current_thread);
}
