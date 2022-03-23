use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Throughput;
use rand::Rng;

use runtime::forecast::cell::AverageTrendAffineEstimator;
use runtime::forecast::cell::CellEstimator;
use runtime::forecast::cell::ForecastSelector;
use runtime::forecast::cell::LeastSquareAffineEstimator;
use runtime::forecast::cell::MeanEstimator;
use runtime::forecast::cell::SimpleExponentSmoothEstimator;
use runtime::forecast::cell::TailEstimator;
use runtime::forecast::Series;
use runtime::forecast::TimeType;
use runtime::forecast::ValueType;


/* Default cell estimator */

fn make_est_candidate() -> Box<dyn CellEstimator> {
    let mut selector = ForecastSelector::default();
    selector.include(Box::new(TailEstimator::default()));
    selector.include(Box::new(MeanEstimator::default()));
    selector.include(Box::new(SimpleExponentSmoothEstimator::with_base(0.5)));
    selector.include(Box::new(LeastSquareAffineEstimator::default()));
    selector.include(Box::new(AverageTrendAffineEstimator::with_tail()));
    selector.include(Box::new(AverageTrendAffineEstimator::with_mean()));
    selector.include(Box::new(AverageTrendAffineEstimator::with_ses(0.5)));
    Box::new(selector)
}


/* Dataset generation */

struct Dataset {
    pub times: Vec<TimeType>,
    pub values: Vec<ValueType>,
    pub final_time: TimeType,
    // pub final_answer: ValueType,
}

fn make_batches(num_batch: usize) -> Vec<Vec<i32>> {
    let mut rng = rand::thread_rng();
    (0..num_batch).map(|_| {
        let batch_size: usize = rng.gen_range(9000..11000);
        (0..batch_size).map(|_| rng.gen_range(0..10000)).collect()
    }).collect()
}

fn make_sum_data(num_batch: usize) -> Dataset {
    let mut total_rows = 0.0;
    let mut total_sum = 0.0;
    let mut proc_rows = Vec::new();
    let mut sums = Vec::new();
    for batch in make_batches(num_batch) {
        // aggregate within batch
        let sum: ValueType = batch.iter().sum::<i32>().into();

        // online aggregate so far
        total_rows += batch.len() as f64;
        total_sum += sum;

        // record online aggregate series
        proc_rows.push(total_rows);
        sums.push(total_sum);
    }
    Dataset {
        times: proc_rows,
        values: sums,
        final_time: total_rows,
        // final_answer: total_sum,
    }
}


/* Benchmarks */

fn forecast_cell_all_steps(c: &mut Criterion) {
    let mut group = c.benchmark_group("Default ForecastSelector Throughput");
    for num_batch in [1, 100, 10000] {
        let dataset = make_sum_data(num_batch);
        group.throughput(Throughput::Elements(num_batch as u64));
        group.sample_size(10);
        group.bench_with_input(BenchmarkId::from_parameter(num_batch), &dataset, |b, dataset| {
            b.iter(|| {
                let mut est = make_est_candidate();
                for tv in Series::new(&dataset.times, &dataset.values).iter() {
                    est.consume(&tv);
                    let f = est.produce();
                    let _pred_v = f.predict(dataset.final_time);
                }
            });
        });
    }
    group.finish();
}

fn forecast_cell_consume(c: &mut Criterion) {
    let mut group = c.benchmark_group("Default ForecastSelector Throughput");
    for num_batch in [1, 100, 10000] {
        let dataset = make_sum_data(num_batch);
        group.throughput(Throughput::Elements(num_batch as u64));
        group.sample_size(10);
        group.bench_with_input(BenchmarkId::from_parameter(num_batch), &dataset, |b, dataset| {
            b.iter(|| {
                let mut est = make_est_candidate();
                for tv in Series::new(&dataset.times, &dataset.values).iter() {
                    est.consume(&tv);
                }
            });
        });
    }
    group.finish();
}

criterion_group!(forecase_cell_benches,
    forecast_cell_all_steps,
    forecast_cell_consume,
);

criterion_main!(forecase_cell_benches);