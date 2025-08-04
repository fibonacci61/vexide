//! Multiple-producer multiple-consumer asynchronous channels.

use alloc::{collections::VecDeque, rc::Rc};
use core::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use super::channel::{Channel, SendError, TryRecvError};

struct MpmcCounter {
    receivers: usize,
}

type Mpmc<T> = Channel<VecDeque<Waker>, MpmcCounter, T>;

/// The sending-half of a [`channel`].
///
/// Messages can be sent through this channel with [`Sender::send`].
#[derive(Clone)]
pub struct Sender<T> {
    inner: Rc<RefCell<Mpmc<T>>>,
}

/// The receiving-half of a [`channel`].
///
/// Messages sent to this channel can be received using [`Receiver::recv`].
pub struct Receiver<T> {
    inner: Rc<RefCell<Mpmc<T>>>,
}

/// An attempt to receive from a channel.
pub struct RecvFuture<'a, T> {
    inner: &'a Rc<RefCell<Mpmc<T>>>,
}

/// Creates a new multiple-producer multiple-consumer (MPMC) asynchronous channel, returning the
/// sending/receiving halves.
///
/// All data sent on the [`Sender`] will become available on the [`Receiver`] in the order it was
/// sent.
///
/// Both the [`Sender`] and the [`Receiver`] can be cloned to send and receive from the same
/// channel across multiple tasks.
///
/// If all [`Receiver`]s are disconnected while trying to send with a [`Sender`], [`Sender::send`]
/// will return a [`SendError`]. Similarly, if all [`Sender`]s are disconnected while trying to
/// receive, [`Receiver::recv`] will return [`None`].
#[must_use]
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let channel = Rc::new(RefCell::new(Channel::new(MpmcCounter { receivers: 1 })));

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
    /// [`SendError`] will be returned if all receivers have been disconnected.
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

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        RefCell::borrow_mut(&self.inner).counter().receivers += 1;
        Self {
            inner: Rc::clone(&self.inner),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut channel = RefCell::borrow_mut(&self.inner);
        let receivers = channel.counter().receivers;
        // senders + receivers = strong_count
        let senders = Rc::strong_count(&self.inner) - receivers;

        // if this is the last sender
        if senders == 1 {
            channel.close();
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut channel = RefCell::borrow_mut(&self.inner);
        // if this is the last receiver
        if channel.counter().receivers == 1 {
            channel.close();
        }

        channel.counter().receivers -= 1;
    }
}
