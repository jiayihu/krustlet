//! `exec` contains convenient wrappers around executing commands from the Kubernetes API.

/// Exec query options
#[derive(Debug)]
pub struct CommandOptions {
    /// The command to execute
    pub command: Command,
    /// Determines whether stdin is supported
    pub stdin: bool,
    /// Determines whether stderr is supported
    pub stderr: bool,
    /// Determines whether stdout is supported
    pub stdout: bool,
    /// Determines whether tty is supported
    pub tty: bool,
}

/// Command to run
#[derive(Debug)]
pub struct Command {
    /// Name of the function to execute
    pub function: String,
    /// Arguments passed to the executed function
    pub args: Vec<String>,
}
