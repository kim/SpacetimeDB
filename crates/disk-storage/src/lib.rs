#![no_std]

use futures_util::Stream;

#[cfg(test)]
extern crate alloc;

pub mod bitset;
pub mod checksum;
pub mod datafile;
pub mod free_set;
pub mod grid;
pub mod manifest;
pub mod state;

pub trait QueueSender<T: 'static> {
    type Permit<'a>: SendPermit<'a, T>
    where
        Self: 'a;

    fn reserve<'a>(&'a self) -> impl Future<Output = Self::Permit<'a>>;
}

pub trait SendPermit<'a, T> {
    fn submit(self, io: T);
}

pub trait QueueReceiver<T>: Stream<Item = T> + Unpin {}
