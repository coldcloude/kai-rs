pub mod error;
pub mod reverse_line_reader;

pub use error::Error;
pub use reverse_line_reader::{ReverseLineReader, LineWithPosition};

#[cfg(test)]
mod tests {

}
