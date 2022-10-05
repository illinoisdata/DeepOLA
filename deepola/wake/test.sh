queries=("q1" "q2" "q3" "q4" "q5" "q6" "q7" "q8" "q9" "q10" "q11" "q12" "q13" "q14" "q15" "q16" "q17" "q18" "q19" "q21")
for query_no in {1..23}
do
	echo q$query_no
	RUST_LOG=info cargo run --release --example tpch_polars -- query q$query_no > logs/q$query_no.log
done
