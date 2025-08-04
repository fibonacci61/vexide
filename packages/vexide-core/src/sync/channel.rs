use alloc::collections::VecDeque;
use core::task::Waker;

use snafu::Snafu;

pub trait WakeQueue: Sized {
    fn new() -> Self;
    fn register(&mut self, waker: Waker);
    fn wake(&mut self);
}

impl WakeQueue for VecDeque<Waker> {
    fn new() -> Self {
        Self::new()
    }

    fn register(&mut self, waker: Waker) {
        self.push_back(waker);
    }

    fn wake(&mut self) {
        if let Some(waker) = self.pop_front() {
            waker.wake();
        }
    }
}

impl WakeQueue for Option<Waker> {
    fn new() -> Self {
        None
    }

    fn register(&mut self, waker: Waker) {
        self.replace(waker);
    }

    fn wake(&mut self) {
        if let Some(waker) = self.take() {
            waker.wake();
        }
    }
}

enum Queue<T> {
    Open(VecDeque<T>),
    Closed,
}

pub struct Channel<W: WakeQueue, C, T> {
    queue: Queue<T>,
    wake_queue: W,
    counter: C,
}

/// An error returned from a send operation on a channel.
///
/// A send operation can only fail if the receiving end of a channel is disconnected, implying that
/// the data could never be received. The error contains the data being sent as a payload so it can
/// be recovered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SendError<T>(pub T);

/// An error returned from a try-receive operation on a channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Snafu)]
pub enum TryRecvError {
    /// All senders of the channel have disconnected.
    #[snafu(display("Senders disconnected"))]
    Disconnected,

    /// The channel is currently empty.
    #[snafu(display("Channel empty"))]
    Empty,
}

impl<W: WakeQueue, C, T> Channel<W, C, T> {
    pub fn new(counters: C) -> Self {
        Self {
            queue: Queue::Open(VecDeque::new()),
            wake_queue: W::new(),
            counter: counters,
        }
    }

    pub fn close(&mut self) {
        self.queue = Queue::Closed;
    }

    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        match self.queue {
            Queue::Open(ref mut deque) => match deque.pop_front() {
                Some(msg) => Ok(msg),
                None => Err(TryRecvError::Empty),
            },
            Queue::Closed => Err(TryRecvError::Disconnected),
        }
    }

    pub fn send(&mut self, msg: T) -> Result<(), SendError<T>> {
        match self.queue {
            Queue::Open(ref mut deque) => {
                deque.push_back(msg);
                self.wake_queue.wake();
                Ok(())
            }
            Queue::Closed => Err(SendError(msg)),
        }
    }

    pub fn register_waker(&mut self, waker: Waker) {
        self.wake_queue.register(waker);
    }

    pub const fn counter(&mut self) -> &mut C {
        &mut self.counter
    }
}
