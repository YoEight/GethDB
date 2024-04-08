use flatbuffers::FlatBufferBuilder;

pub struct TFLog {
    builder: FlatBufferBuilder<'static>,
}

impl TFLog {
    pub fn new() -> Self {
        Self {
            builder: FlatBufferBuilder::with_capacity(4_096),
        }
    }
}
