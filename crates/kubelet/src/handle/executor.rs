use crate::exec::Command;

/// A [`ExecHandler`] is used to handle executing commands in a process
#[async_trait::async_trait]
pub trait ExecHandler {
    /// Exec a command
    async fn exec(&mut self, command: Command) -> anyhow::Result<String>;
}
