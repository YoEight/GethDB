use std::io;

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

    fn map_io<F, A>(self, func: F) -> MapIO<F, Self>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> io::Result<A>,
    {
        MapIO { func, inner: self }
    }

    fn filter<F>(self, func: F) -> Filter<F, Self>
    where
        Self: Sized,
        F: FnMut(&Self::Item) -> bool,
    {
        Filter { func, inner: self }
    }

    fn last(mut self) -> io::Result<Option<Self::Item>>
    where
        Self: Sized,
    {
        let mut result = None;

        while let Some(value) = self.next()? {
            result = Some(value);
        }

        Ok(result)
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

pub struct MapIO<F, I> {
    func: F,
    inner: I,
}

impl<F, I, A> IteratorIO for MapIO<F, I>
where
    I: IteratorIO,
    F: FnMut(I::Item) -> io::Result<A>,
{
    type Item = A;

    fn next(&mut self) -> io::Result<Option<Self::Item>> {
        if let Some(item) = self.inner.next()? {
            return Ok(Some((self.func)(item)?));
        }

        Ok(None)
    }
}

pub struct Filter<F, I> {
    func: F,
    inner: I,
}

impl<F, I> IteratorIO for Filter<F, I>
where
    I: IteratorIO,
    F: FnMut(&I::Item) -> bool,
{
    type Item = I::Item;

    fn next(&mut self) -> io::Result<Option<Self::Item>> {
        loop {
            if let Some(item) = self.inner.next()? {
                if (self.func)(&item) {
                    return Ok(Some(item));
                }
            } else {
                return Ok(None);
            }
        }
    }
}

pub struct Lift<I> {
    inner: I,
}

impl<I> IteratorIO for Lift<I>
where
    I: Iterator,
{
    type Item = I::Item;

    fn next(&mut self) -> io::Result<Option<Self::Item>> {
        Ok(self.inner.next())
    }
}

impl<I> IteratorIO for Box<I>
where
    I: IteratorIO + ?Sized,
{
    type Item = I::Item;

    fn next(&mut self) -> io::Result<Option<Self::Item>> {
        self.as_mut().next()
    }
}

pub trait IteratorIOExt: Sized {
    fn lift(self) -> Lift<Self>;
}

impl<I> IteratorIOExt for I
where
    I: Sized,
    I: Iterator,
{
    fn lift(self) -> Lift<Self> {
        Lift { inner: self }
    }
}
