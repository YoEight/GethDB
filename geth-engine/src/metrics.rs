use opentelemetry::metrics::{Counter, Histogram, UpDownCounter};

pub struct Metrics {
    pub programs_total: Counter<u64>,
    pub programs_active_total: UpDownCounter<f64>,
    pub subscriptions_total: Counter<u64>,
    pub client_errors_total: Counter<u64>,
    pub server_errors_total: Counter<u64>,
    pub read_size_bytes: Histogram<f64>,
    pub write_size_bytes: Histogram<f64>,
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

        read_size_bytes: meter
            .f64_histogram("geth_read_size_bytes")
            .with_description("Distribution of the reads size")
            .with_unit("bytes")
            .build(),

        write_size_bytes: meter
            .f64_histogram("geth_write_size_bytes")
            .with_description("Distribution of the writes size")
            .with_unit("bytes")
            .build(),
    }
}
