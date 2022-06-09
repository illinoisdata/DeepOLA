#!/bin/bash

LOG_LEVEL=info

DATASETS=(
  "q12 nb/tpch_q12_s42_p60 60 nb/tpch_q12_s42_p60/60.csv"
  "q12 nb/tpch_mode_q12_s42_p60 60 nb/tpch_mode_q12_s42_p60/60.csv"
  "q16 nb/tpch_q16_s42_p60 60 nb/tpch_q16_s42_p60/60.csv"
)

METHODS=(
  "default"
  "tail"
  "linear_scale"
  "least_square"
)


benchmark () {
  read -a dataset_params <<< $1
  read -a method_params <<< $2

  schema_name=${dataset_params[0]}
  series_dir=${dataset_params[1]}
  series_n=${dataset_params[2]}
  final_answer=${dataset_params[3]}

  forecase_by=${method_params[0]}

  echo ">>>>>"
  set -x
  RUST_LOG=forecast_eval=${LOG_LEVEL} ./target/release/examples/forecast_eval --final-answer-path ${final_answer} --series-n ${series_n} --series-dir ${series_dir} --schema-name ${schema_name} --forecast-by ${forecase_by}
  set +x
}

for ((i = 0; i < ${#DATASETS[@]}; i++)) do
  for ((j = 0; j < ${#METHODS[@]}; j++)) do
    benchmark "${DATASETS[$i]}" "${METHODS[$j]}"
  done
done