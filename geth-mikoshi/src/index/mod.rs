use std::io;

mod rannoch;

pub trait IteratorIO {
    type Item;

    fn next(&mut self) -> io::Result<Option<Self::Item>>;
}
