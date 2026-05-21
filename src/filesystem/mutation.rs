use crate::{filesystem::tree::NodeOperation, port::OperationBroadcaster};

#[must_use]
pub struct TreeMutation<T> {
    result: T,
    operations: Vec<NodeOperation>,
}

impl<T> TreeMutation<T> {
    pub fn new(result: T, operations: Vec<NodeOperation>) -> Self {
        Self { result, operations }
    }

    pub async fn broadcast(self, sink: &impl OperationBroadcaster) -> T {
        sink.broadcast(self.operations).await;
        self.result
    }
}
