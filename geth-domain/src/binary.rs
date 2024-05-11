pub mod events {
    use uuid::Uuid;

    pub use events::Event;

    include!(concat!(env!("OUT_DIR"), "/model.rs"));

    impl From<Uuid> for Id {
        fn from(value: Uuid) -> Self {
            let (most, least) = value.as_u64_pair();

            Self { most, least }
        }
    }

    impl From<Id> for Uuid {
        fn from(value: Id) -> Self {
            Uuid::from_u64_pair(value.most, value.least)
        }
    }
}
