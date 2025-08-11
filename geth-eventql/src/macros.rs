macro_rules! bail {
    ($pos:expr, $err:expr $(,)?) => {
        return $crate::private::Err($crate::error::Error {
            pos: $pos,
            kind: $err.into(),
        })
    };
}
