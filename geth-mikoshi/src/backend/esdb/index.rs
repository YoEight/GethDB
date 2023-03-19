use std::collections::HashMap;

pub struct StreamIndex {
    inner: HashMap<String, Vec<i64>>,
}

impl StreamIndex {
    pub fn new() -> Self {
        Self {
            inner: Default::default(),
        }
    }

    pub fn index(&mut self, stream_id: &String, log_position: i64) {
        if let Some(values) = self.inner.get_mut(stream_id) {
            values.push(log_position);
        } else {
            self.inner.insert(stream_id.clone(), vec![log_position]);
        }
    }

    pub fn stream_current_revision(&self, stream_id: &String) -> u64 {
        self.inner
            .get(stream_id)
            .map_or(0u64, |values| values.len() as u64)
    }

    pub fn stream_events(&self, stream_id: &String) -> Option<&Vec<i64>> {
        self.inner.get(stream_id)
    }
}
