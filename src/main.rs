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
    let mut total_number = 0;
    let mut time = 0.0;
    let mut rng = StdRng::seed_from_u64(seed);
    let arrival_dist = Exp::new(load).unwrap();
    let size_dist = Exp::new(1.0).unwrap();
    let mut next_arrival_time = arrival_dist.sample(&mut rng);
    while num_completions < num_jobs {
        if false { //num_completions == 0 || num_completions.is_power_of_two() {
            println!("{}: {:?}", num_completions, queue);
        }
        queue.sort_by_key(|job| -n64(job.rem_size));
        let num_in_service = if queue.len() <= num_servers {
            queue.len()
        } else {
            let service_threshold = queue[num_servers - 1].rem_size;
            let mut out = num_servers;
            for i in num_servers..queue.len() {
                if queue[i].rem_size >= service_threshold - EPSILON {
                    out += 1;
                } else {
                    break;
                }
            }
            out
        };
        let smallest_in_service = if num_in_service == 0 {
            INFINITY
        } else {
            queue[num_in_service - 1].rem_size
        };
        let smallest_out_of_service = if num_in_service == queue.len() {
            0.0
        } else {
            queue[num_in_service].rem_size
        };
        let service_rate = num_in_service.max(num_servers);
        let service_duration = if queue.len() == 0 {
            INFINITY
        } else {
            (smallest_in_service - smallest_out_of_service) * service_rate as f64
        };
        let event_duration = (next_arrival_time - time).min(service_duration);
        let was_arrival = event_duration < service_duration;
        time += event_duration;
        for i in 0..num_in_service {
            queue[i].rem_size -= event_duration / service_rate as f64;
        }
        for i in (0..num_in_service).rev() {
            if queue[i].rem_size < EPSILON {
                let _job = queue.remove(i);
                num_completions += 1;
            }
        }
        if was_arrival {
            let work: f64 = queue.iter().map(|job| job.rem_size).sum();
            total_work += work;
            total_number += queue.len();
            let job = Job {
                rem_size: size_dist.sample(&mut rng),
            };
            queue.push(job);
            num_arrivals += 1;
            next_arrival_time = time + arrival_dist.sample(&mut rng);
        }
    }
    println!("{}", total_number as f64 / num_arrivals as f64);
    total_work / num_arrivals as f64
}

fn main() {
    let num_jobs = 10_000_000;
    let num_servers = 2;
    let seed = 0;
    for load in vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95] {
        let mean_work = sim(num_jobs, num_servers, load, seed);
        println!("{}; {}", load, mean_work);
    }
}
