import sys
import pandas as pd
import graphviz
from dateutil import parser
from datetime import timedelta
import re

def parse_log_file(log_file = "output.log"):
    regex = re.compile(r'\[(.*)\] \[logging\] (.*)')
    log_lines = open(log_file,'r').readlines()
    collect_logs = []
    for line in log_lines:
        if '[logging]' in line:
            matches = regex.match(line)
            event_time = parser.parse(matches.groups()[0].split()[0])
            tokens = matches.groups()[1].split()
            log = {'event_time': event_time}
            for token in tokens:
                key,value = token.split('=')
                log[key] = value
            collect_logs.append(log)
    df = pd.DataFrame(collect_logs)
    return df

def generate_query_graph(df, query_no, output_dir = 'outputs/', display=False):
    graph_edges = df[df['type'] == 'node-edge']
    g = graphviz.Digraph(name = f'q{query_no}', format='png')
    for index,edge in graph_edges.iterrows():
        source = edge['source']
        dest = edge['dest']
        g.edge(source, dest)
    if display:
        return g
    else:
        g.render(directory=output_dir).replace('\\', '/')
        return g

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
    execution_logs = df[df['type'] == 'execution']
    execution_logs_entries = []
    for _,log in execution_logs.iterrows():
        thread = log['thread']
        node = thread_node_map[thread]
        action = log['action']
        time = log['time']
        end_time = log['event_time']
        start_time = end_time - timedelta(microseconds=float(time))
        execution_logs_entries.append({
            'start_time': start_time.timestamp(),
            'end_time': end_time.timestamp(),
            'thread': thread,
            'node': node,
            'action': action,
            'duration': float(time)/1000
        })
    execution_log_df = pd.DataFrame(execution_logs_entries)
    min_start_time = execution_log_df['start_time'].min()
    execution_log_df['start_time'] -= min_start_time
    execution_log_df['end_time'] -= min_start_time
    return execution_log_df.sort_values(['start_time', 'end_time'])

if __name__ == "__main__":
    query_no = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    log_dir = '../../deepola/wake/logs'
    output_dir = '../../deepola/wake/outputs/queries/'
    log_file = f'{log_dir}/q{query_no}.log'
    df = parse_log_file(log_file)
    generate_query_graph(df, query_no, output_dir)
    execution_df = get_execution_log_df(df)
    print(execution_df)
