pub mod google {
    pub mod rpc {
        include!(concat!(env!("OUT_DIR"), "/google.rpc.rs"));
    }
}

pub mod geth {
    pub mod protocol {
        include!(concat!(env!("OUT_DIR"), "/event_store.client.rs"));
        pub mod server {
            include!(concat!(env!("OUT_DIR"), "/event_store.client.streams.rs"));
        }
    }
}
