use pin_project::pin_project;
use std::{
    future::Future,
    mem::MaybeUninit,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, Result as IoResult};

pub trait VecAsyncReadExt: AsyncRead {
    /// Read data from this reader until the given future resolves.
    fn allow_read_until<F>(self, until: F) -> AllowReadUntil<Self, F>
    where
        Self: Sized,
        F: Future<Output = ()>,
    {
        AllowReadUntil {
            reader: self,
            until,
        }
    }
}

impl<S> VecAsyncReadExt for S where S: AsyncRead {}

/// A AsyncRead combinator which reads from a reader until a future resolves.
#[pin_project]
#[derive(Clone, Debug)]
pub struct AllowReadUntil<S, F> {
    #[pin]
    reader: S,
    #[pin]
    until: F,
}

impl<S, F> AllowReadUntil<S, F> {
    pub fn get_ref(&self) -> &S {
        &self.reader
    }
}

impl<S, F> AsyncRead for AllowReadUntil<S, F>
where
    S: AsyncRead,
    F: Future<Output = ()>,
{
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context, buf: &mut [u8]) -> Poll<IoResult<usize>> {
        let this = self.project();
        match this.until.poll(cx) {
            Poll::Ready(_) => Poll::Ready(Ok(0)),
            Poll::Pending => this.reader.poll_read(cx, buf),
        }
    }

    unsafe fn prepare_uninitialized_buffer(&self, buf: &mut [MaybeUninit<u8>]) -> bool {
        self.reader.prepare_uninitialized_buffer(buf)
    }
}
