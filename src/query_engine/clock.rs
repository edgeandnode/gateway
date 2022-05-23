use crate::prelude::*;
#[cfg(test)]
use std::{cell::RefCell, rc::Rc};

pub trait Clock {
    fn now(&self) -> Instant;
}

#[derive(Debug)]
pub struct SystemClock;

impl Clock for SystemClock {
    #[inline(always)]
    fn now(&self) -> Instant {
        Instant::now()
    }
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub struct MockClock {
    current_time: Rc<RefCell<Instant>>,
}

#[cfg(test)]
impl MockClock {
    pub fn new() -> Self {
        Self {
            current_time: Rc::new(RefCell::new(Instant::now())),
        }
    }

    pub fn advance_time(&mut self, amount: Duration) {
        let mut l = self.current_time.borrow_mut();
        *l += amount;
    }
}

#[cfg(test)]
impl Clock for MockClock {
    fn now(&self) -> Instant {
        self.current_time.borrow().clone()
    }
}
