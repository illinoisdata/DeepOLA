import os
import sys
import pandas as pd
import graphviz

def parse_log_file(log_file = "output.log", output_dir = "outputs"):
    log_lines = open(log_file,'r').readlines()
    collect_logs = []
    for line in log_lines:
        if "[logging]" in line:
            events = line.split("[logging]")[-1]
            key_values = events.split()
            log = {}
            for element in key_values:
                try:
                    key = element.split(",")[0][1:]
                    val = element.split(",")[1][:-1]
                    log[key] = val
                except Exception as e:
                    print(element, line)
                    raise e
            collect_logs.append(log)
    df = pd.DataFrame(collect_logs)
    df.to_csv(f"{output_dir}/parsed-logs.csv",index=False)
    print(f"Saved parsed dataframe to {output_dir}/parsed-logs.csv")
    return df

def generate_node_statistics(df, output_dir = "outputs"):
    node_statistics = []
    grouped_df = df.groupby("node")
    for (node, raw_df) in grouped_df:
        tasks = raw_df.groupby("task")
        for (task_type, task_type_df) in tasks:
            prev_timestamp = 0
            time_spent_in_task = 0
            for (idx,event) in task_type_df.iterrows():
                if event.action == "end":
                    time_spent_in_task += (event.timestamp - prev_timestamp)
                else:
                    prev_timestamp = event.timestamp
            node_statistics.append({
                "node": node,
                "task": task_type,
                "num_events": task_type_df.size,
                "time": time_spent_in_task
            })
    node_stats_df = pd.DataFrame(node_statistics)
    node_stats_df.sort_values(["node","task"], ascending=False, inplace=True)
    node_stats_df.to_csv(f"{output_dir}/node-statistics.csv",index=False)
    print(f"Saved node statistics to {output_dir}/node-statistics.csv")
    return node_stats_df

def generate_query_graph(df, output_dir = "outputs", display=False):
    graph_edges = df[df['type'] == 'node-edge']
    g = graphviz.Digraph(name = f'graph', format='png')
    for index,edge in graph_edges.iterrows():
        source = edge['src']
        dest = edge['dest']
        g.edge(source, dest)
    if display:
        return g
    else:
        g.render(directory=output_dir).replace('\\', '/')
        print(f"Saved output graph to {output_dir}/graph.gv.png")
        return g

# def generate_execution_query_graph(df, output_dir = 'outputs/', display=False):
#     g = generate_query_graph(df, output_dir, display)
#     execution_df = get_execution_log_df(df)
#     # process_time = execution_df[execution_df['action'] == 'process']
#     # nodes_by_process_time = process_time[['node','duration']].groupby(['node']).sum().sort_values('duration', ascending=False).reset_index()
#     # maximum_duration = nodes_by_process_time['duration'].max()
#     # minimum_duration = nodes_by_process_time['duration'].min()
#     # print(nodes_by_process_time)
#     for index,row in nodes_by_process_time.iterrows():
#         relative_duration = 9
#         # relative_duration = min(9,int((10*row['duration'])/maximum_duration))
#         g.node(row['node'], style='filled', fillcolor=f"/oranges9/{1 if relative_duration <= 0 else relative_duration}")
#     if display:
#         return g
#     else:
#         g.render(directory=output_dir).replace('\\','/')
#         return g

def get_node_thread_map(df):
    node_thread_map_logs = df[df['type'] == 'node-thread-map']
    thread_node_map = {}
    for _,log in node_thread_map_logs.iterrows():
        node = log['node']
        thread = log['thread']
        thread_node_map[thread] = node
    return thread_node_map

def get_execution_log_df(df):
    thread_node_map = get_node_thread_map(df)
    execution_logs = df[df['type'] == 'event']
    execution_logs_entries = []
    for _,log in execution_logs.iterrows():
        thread = log['thread']
        node = thread_node_map.get(thread, "main")
        task = log['task']
        action = log['action']
        timestamp = log['timestamp']
        if "write-message" in task or "read-message" in task:
            execution_logs_entries.append({
                'thread': thread,
                'node': node,
                'task': task,
                'action': action,
                'timestamp': int(timestamp),
            })
    execution_log_df = pd.DataFrame(execution_logs_entries)
    print(execution_log_df)
    min_start_time = execution_log_df['timestamp'].min()
    execution_log_df['timestamp'] -= min_start_time
    execution_log_df['timestamp'] /= 1e9
    execution_log_df = execution_log_df.sort_values(['timestamp', 'node', 'action'])
    execution_log_df.drop(columns=['thread'])
    execution_log_df.to_csv(f"{output_dir}/parsed-event-timeline.csv",index=False)
    print(f"Saved events df to {output_dir}/parsed-event-timeline.csv")
    return execution_log_df

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 process_logs.py <input-log-file> <output-directory>")
        exit(1)
    log_file = sys.argv[1]
    output_dir = sys.argv[2]
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    df = parse_log_file(log_file, output_dir)
    g = generate_query_graph(df, output_dir)
    events_df = get_execution_log_df(df)
    node_statistics_df = generate_node_statistics(events_df, output_dir)