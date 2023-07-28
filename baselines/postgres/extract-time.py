import sys

if __name__ == "__main__":
    output_dir = sys.argv[1]
    scale = float(sys.argv[2])
    partition = int(sys.argv[3])
    num_runs = int(sys.argv[4])
    start_run = int(sys.argv[5])
    start_qdx = int(sys.argv[6])
    end_qdx = int(sys.argv[7])

    print("query,run,time")
    for qdx in range(start_qdx,end_qdx):
        for run in range(start_run,num_runs+1):
            try:
                log_file = open(f"{output_dir}/q{qdx}-run{run}.log","r").readlines()
                total_time = 0
                for l in log_file:
                    if "Time:" in l:
                        time_taken = float(l.strip().split("ms")[0].split(":")[1].strip())
                        total_time += time_taken
                print(f"{qdx},{run},{round(total_time/1000.0,3)}")
            except:
                continue