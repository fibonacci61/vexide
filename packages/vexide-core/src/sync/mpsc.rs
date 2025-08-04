//! Multiple-producer single-consumer asynchronous channels.

use alloc::rc::Rc;
use core::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use super::channel::{Channel, SendError, TryRecvError};

type Mpsc<T> = Channel<Option<Waker>, (), T>;

/// The sending-half of a [`channel`].
///
/// Messages can be sent through this channel with [`Sender::send`].
#[derive(Clone)]
pub struct Sender<T> {
    inner: Rc<RefCell<Mpsc<T>>>,
}

/// The receiving-half of a [`channel`]. This half is exclusive and cannot be cloned.
///
/// Messages sent to this channel can be received using [`Receiver::recv`].
pub struct Receiver<T> {
    inner: Rc<RefCell<Mpsc<T>>>,
}

/// An attempt to receive from a channel.
pub struct RecvFuture<'a, T> {
    inner: &'a Rc<RefCell<Mpsc<T>>>,
}

/// Creates a new multiple-producer single-consumer (MPSC) asynchronous channel, returning the
/// sending/receiving halves.
///
/// All data sent on the [`Sender`] will become available on the [`Receiver`] in the order it was
/// sent.
///
/// The [`Sender`] can be cloned to send to the same channel across multiple tasks, but only one
/// [`Receiver`] is supported.
///
/// If the [`Receiver`] is disconnected while trying to send with the [`Sender`], [`Sender::send`]
/// will return a [`SendError`]. Similarly, if the [`Sender`] disconnected while trying to receive,
/// [`Receiver::recv`] will return [`None`].
#[must_use]
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let channel = Rc::new(RefCell::new(Channel::new(())));

    let sender = Sender {
        inner: Rc::clone(&channel),
    };
    let receiver = Receiver { inner: channel };

    (sender, receiver)
}

impl<T> Sender<T> {
    /// Attempts to send a message through the channel, returning it back if it could not be sent.
    ///
    /// # Errors
    ///
    /// [`SendError`] will be returned if the receiver has been disconnected.
    pub fn send(&self, msg: T) -> Result<(), SendError<T>> {
        RefCell::borrow_mut(&self.inner).send(msg)
    }
}

impl<T> Receiver<T> {
    /// Attempts to receive a message from the channel.
    ///
    /// # Errors
    ///
    /// If all [`Sender`]s have been disconnected, [`TryRecvError::Disconnected`] will be returned.
    /// Otherwise, if the channel is currently empty, [`TryRecError::Empty`] will be returned.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        RefCell::borrow_mut(&self.inner).try_recv()
    }

    /// Attempts to receive a message from the channel, blocking until one is available, or until
    /// all [`Sender`]s have been disconnected, in which case [`None`] will be returned.
    pub async fn recv(&self) -> Option<T> {
        RecvFuture { inner: &self.inner }.await
    }
}

impl<T> Future for RecvFuture<'_, T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut channel = RefCell::borrow_mut(self.inner);
        let msg = channel.try_recv();

        match msg {
            Ok(msg) => Poll::Ready(Some(msg)),
            Err(TryRecvError::Disconnected) => Poll::Ready(None),
            Err(TryRecvError::Empty) => {
                channel.register_waker(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if Rc::strong_count(&self.inner) == 2 {
            RefCell::borrow_mut(&self.inner).close();
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        RefCell::borrow_mut(&self.inner).close();
    }
}
