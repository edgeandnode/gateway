use crate::*;
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

#[derive(Clone, Debug)]
pub struct MockClock {
    current_time: Rc<RefCell<Instant>>,
}

impl Default for MockClock {
    fn default() -> Self {
        Self {
            current_time: Rc::new(RefCell::new(Instant::now())),
        }
    }
}

impl MockClock {
    pub fn advance_time(&mut self, amount: Duration) {
        let mut l = self.current_time.borrow_mut();
        *l += amount;
    }
}

impl Clock for MockClock {
    fn now(&self) -> Instant {
        *self.current_time.borrow()
    }
}
