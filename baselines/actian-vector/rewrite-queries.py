for i in range(1,23):
    current_query = open(f"queries/{i}.sql").read()
    individual_queries = current_query.split(";")
    filtered_queries = []
    for q in individual_queries:
        if len(q.strip()) != 0:
            filtered_queries.append(q.strip())
    updated_queries = [f"\\rt\n{x};\\g" for x in filtered_queries]
    updated_sql = "".join(updated_queries)
    with open(f"updated-queries/{i}.sql","w") as f:
        f.write(updated_sql)
