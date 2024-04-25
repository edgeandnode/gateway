/// Whether the checked resource is blocked or allowed.
#[derive(Clone)]
pub enum BlockState {
    /// The resource is allowed.
    Allowed,
    /// The resource is blocked.
    Blocked,
}

impl BlockState {
    /// Check if the resource is allowed.
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed)
    }

    /// Check if the resource is blocked.
    pub fn is_blocked(&self) -> bool {
        matches!(self, Self::Blocked)
    }
}
