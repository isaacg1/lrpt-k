use noisy_float::prelude::*;
use rand::prelude::*;
use rand_distr::Exp;

use std::f64::INFINITY;
const EPSILON: f64 = 1e-8;
#[derive(Debug)]
struct Job {
    rem_size: f64,
    //arrival_time: f64,
}

// Exp(1) jobs, LRPT policy, output mean workload
fn sim(num_jobs: u64, num_servers: usize, load: f64, seed: u64) -> f64 {
    let mut queue: Vec<Job> = vec![];
    let mut num_completions = 0;
    let mut num_arrivals = 0;
    let mut total_work = 0.0;
    let mut time = 0.0;
    let mut rng = StdRng::seed_from_u64(seed);
    let arrival_dist = Exp::new(load).unwrap();
    let size_dist = Exp::new(1.0).unwrap();
    let mut next_arrival_time = arrival_dist.sample(&mut rng);
    while num_completions < num_jobs {
        queue.sort_by_key(|job| -n64(job.rem_size));
        if false {
            println!(
                "{}: {:?} {}",
                time,
                queue,
                queue.iter().map(|job| job.rem_size).sum::<f64>()
            );
            std::io::stdin()
                .read_line(&mut String::new())
                .expect("Present");
        }
        let (num_in_service, first_sharing) = if queue.len() <= num_servers {
            (queue.len(), queue.len().saturating_sub(1))
        } else {
            let service_threshold = queue[num_servers - 1].rem_size;
            let mut service_count = num_servers;
            for i in num_servers..queue.len() {
                if queue[i].rem_size >= service_threshold - EPSILON {
                    service_count += 1;
                } else {
                    break;
                }
            }
            let mut first_sharing = queue.len();
            for i in 0..=num_servers {
                if queue[i].rem_size <= service_threshold + EPSILON {
                    first_sharing = i;
                    break;
                }
            }
            assert!(first_sharing < queue.len(), "First sharing updated");
            (service_count, first_sharing)
        };
        let smallest_in_service = if num_in_service == 0 {
            INFINITY
        } else {
            queue[num_in_service - 1].rem_size
        };
        let smallest_unshared = if first_sharing == 0 {
            INFINITY
        } else {
            queue[first_sharing - 1].rem_size
        };
        let smallest_out_of_service = if num_in_service == queue.len() {
            0.0
        } else {
            queue[num_in_service].rem_size
        };
        let shared_service_rate = (num_servers.min(num_in_service) - first_sharing) as f64
            / num_servers as f64
            / (num_in_service - first_sharing) as f64;
        let service_duration = if queue.len() == 0 {
            INFINITY
        } else if num_in_service <= num_servers {
            (smallest_in_service - smallest_out_of_service) * num_servers as f64
        } else {
            ((smallest_in_service - smallest_out_of_service) / shared_service_rate).min(
                (smallest_unshared - smallest_in_service)
                    / (1.0 / num_servers as f64 - shared_service_rate),
            )
        };
        let event_duration = (next_arrival_time - time).min(service_duration);
        let was_arrival = event_duration < service_duration;
        time += event_duration;
        let mut total_rate = 0.0;
        for i in 0..num_in_service {
            if i < first_sharing {
                queue[i].rem_size -= event_duration / num_servers as f64;
                total_rate += 1.0/num_servers as f64;
            } else {
                queue[i].rem_size -= event_duration * shared_service_rate;
                total_rate += shared_service_rate;
            }
        }
        assert!(total_rate <= 1.0 + EPSILON);
        for i in (0..num_in_service).rev() {
            if queue[i].rem_size < EPSILON {
                let _job = queue.remove(i);
                num_completions += 1;
            }
        }
        if was_arrival {
            let work: f64 = queue.iter().map(|job| job.rem_size).sum();
            total_work += work;
            let job = Job {
                rem_size: size_dist.sample(&mut rng),
            };
            queue.push(job);
            num_arrivals += 1;
            next_arrival_time = time + arrival_dist.sample(&mut rng);
        }
    }
    total_work / num_arrivals as f64
}

fn main() {
    let num_jobs = 100_000_000;
    let num_servers = 2;
    for seed in 0..10 {
        for load in vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95] {
            let mean_work = sim(num_jobs, num_servers, load, seed);
            println!("{}; {}", load, mean_work);
        }
    }
}
