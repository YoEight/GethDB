use std::io;

mod rannoch;

pub trait IteratorIO {
    type Item;

    fn next(&mut self) -> io::Result<Option<Self::Item>>;

    fn map<F, A>(self, func: F) -> Map<F, Self>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> A,
    {
        Map { func, inner: self }
    }
}

pub struct Map<F, I> {
    func: F,
    inner: I,
}

impl<F, A, I> IteratorIO for Map<F, I>
where
    I: IteratorIO,
    F: FnMut(I::Item) -> A,
{
    type Item = A;

    fn next(&mut self) -> io::Result<Option<Self::Item>> {
        if let Some(item) = self.inner.next()? {
            return Ok(Some((self.func)(item)));
        }

        Ok(None)
    }
}
