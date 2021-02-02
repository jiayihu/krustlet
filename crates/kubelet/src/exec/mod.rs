//! `exec` contains convenient wrappers around executing commands from the Kubernetes API.

use anyhow::anyhow;
use serde::Deserialize;

/// Exec query options
#[derive(Debug, Deserialize)]
pub struct Options {
    /// The command to execute
    pub command: String,
    /// Determines whether stdin is supported
    #[serde(default)]
    pub stdin: bool,
    /// Determines whether stderr is supported
    #[serde(default)]
    pub stderr: bool,
    /// Determines whether stdout is supported
    #[serde(default)]
    pub stdout: bool,
    /// Determines whether tty is supported
    #[serde(default)]
    pub tty: bool,
}

/// Command to run
pub struct Command {
    /// Name of the function to execute
    pub function: String,
    /// Arguments passed to the executed function
    pub args: Vec<String>,
}

/// Parse an exec command
pub fn parse_command(command: &String) -> anyhow::Result<Command> {
    let tokens: Vec<&str> = command.split(" ").collect();
    let function = tokens
        .get(0)
        .ok_or_else(|| anyhow!("No function specified in the command"))?
        .to_string();
    let args = tokens
        .get(1..)
        .map(|xs| xs.to_vec().into_iter().map(|x| x.to_string()).collect())
        .unwrap_or(vec![]);

    Ok(Command { function, args })
}
