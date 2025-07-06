use geth_mikoshi::wal::{LogEntries, LogEntry};
use opentelemetry::metrics::{Counter, Histogram, UpDownCounter};
use tokio::sync::OnceCell;

#[derive(Debug, Clone)]
pub struct Metrics {
    pub programs_total: Counter<u64>,
    pub programs_active_total: UpDownCounter<f64>,
    pub subscriptions_total: Counter<u64>,
    pub subscriptions_active_total: UpDownCounter<f64>,
    pub client_errors_total: Counter<u64>,
    pub server_errors_total: Counter<u64>,
    pub read_size_bytes: Histogram<f64>,
    pub read_entry_total: Counter<u64>,
    pub read_error_total: Counter<u64>,
    pub index_cache_hits_total: Counter<u64>,
    pub index_cache_miss_total: Counter<u64>,
    pub index_read_error_total: Counter<u64>,
    pub index_write_error_total: Counter<u64>,
    pub write_size_bytes: Histogram<f64>,
    pub write_propose_event_total: Counter<u64>,
    pub write_error_total: Counter<u64>,
}

impl Metrics {
    pub fn observe_read_log_entry(&self, entry: &LogEntry) {
        self.read_size_bytes
            .record(entry.payload_size() as f64, &[]);

        self.read_entry_total.add(1, &[]);
    }

    pub fn observe_read_error(&self) {
        self.read_error_total.add(1, &[]);
    }

    pub fn observe_index_cache_hit(&self) {
        self.index_cache_hits_total.add(1, &[]);
    }

    pub fn observe_index_cache_miss(&self) {
        self.index_cache_miss_total.add(1, &[]);
    }

    pub fn observe_index_read_error(&self) {
        self.index_read_error_total.add(1, &[]);
    }

    pub fn observe_index_write_error(&self) {
        self.index_write_error_total.add(1, &[]);
    }

    pub fn observe_subscription_new(&self) {
        self.subscriptions_total.add(1, &[]);
        self.subscriptions_active_total.add(1.0, &[]);
    }

    pub fn observe_subscription_terminated(&self, count: usize) {
        self.subscriptions_active_total.add(-(count as f64), &[]);
    }

    pub fn observe_program_new(&self) {
        self.programs_total.add(1, &[]);
        self.programs_active_total.add(1.0, &[]);
    }

    pub fn observe_program_terminated(&self) {
        self.programs_active_total.add(-1.0, &[]);
    }

    pub fn observe_written_propose_event<L: LogEntries>(&self, entries: &L) {
        self.write_size_bytes
            .record(entries.current_entry_size() as f64, &[]);
        self.write_propose_event_total.add(1, &[]);
    }

    pub fn observe_write_error(&self) {
        self.write_error_total.add(1, &[]);
    }

    pub fn observe_client_error(&self) {
        self.client_errors_total.add(1, &[]);
    }

    pub fn observe_server_error(&self) {
        self.server_errors_total.add(1, &[]);
    }
}

static METRICS: OnceCell<Metrics> = OnceCell::const_new();

pub fn get_metrics() -> Metrics {
    METRICS.get().unwrap().clone()
}

pub fn configure_metrics() {
    METRICS.set(init_meter()).expect("not to be configured yet");
}

fn init_meter() -> Metrics {
    let meter = opentelemetry::global::meter("geth-engine");

    Metrics {
        programs_total: meter
            .u64_counter("geth_programs_total")
            .with_description("Total number of programs")
            .with_unit("programs")
            .build(),

        programs_active_total: meter
            .f64_up_down_counter("geth_programs_active_total")
            .with_description("Total number of active programs")
            .with_unit("programs")
            .build(),

        subscriptions_total: meter
            .u64_counter("geth_subscriptions_total")
            .with_description("Total number of subcriptions")
            .with_unit("subscriptions")
            .build(),

        client_errors_total: meter
            .u64_counter("geth_client_errors_total")
            .with_description("Total number of client errors")
            .with_unit("errors")
            .build(),

        server_errors_total: meter
            .u64_counter("geth_server_errors_total")
            .with_description("Total number of server errors")
            .with_unit("errors")
            .build(),

        read_entry_total: meter
            .u64_counter("geth_read_entry_total")
            .with_description("Total number of read entries")
            .with_unit("entries")
            .build(),

        read_error_total: meter
            .u64_counter("geth_read_error_total")
            .with_description("Total number of read errors")
            .with_unit("errors")
            .build(),

        read_size_bytes: meter
            .f64_histogram("geth_read_size_bytes")
            .with_description("Distribution of the reads size")
            .with_unit("bytes")
            .build(),

        index_cache_hits_total: meter
            .u64_counter("geth_index_cache_hits_total")
            .with_description("Total number of index cache hits")
            .with_unit("hits")
            .build(),

        index_cache_miss_total: meter
            .u64_counter("geth_index_cache_miss_total")
            .with_description("Total number of index cache misses")
            .with_unit("misses")
            .build(),

        index_read_error_total: meter
            .u64_counter("geth_index_read_error_total")
            .with_description("Total number of index read errors")
            .with_unit("errors")
            .build(),

        index_write_error_total: meter
            .u64_counter("geth_index_write_error_total")
            .with_description("Total number of index write errors")
            .with_unit("errors")
            .build(),

        write_size_bytes: meter
            .f64_histogram("geth_write_size_bytes")
            .with_description("Distribution of the writes size")
            .with_unit("bytes")
            .build(),

        write_propose_event_total: meter
            .u64_counter("geth_write_propose_event_total")
            .with_description("Total number of written propose events")
            .with_unit("events")
            .build(),

        write_error_total: meter
            .u64_counter("geth_write_error_total")
            .with_description("Total number of write errors")
            .with_unit("errors")
            .build(),

        subscriptions_active_total: meter
            .f64_up_down_counter("geth_subscriptions_active_total")
            .with_description("Total number of active subscriptions")
            .with_unit("subscriptions")
            .build(),
    }
}
