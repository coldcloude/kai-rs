pub mod error;
pub mod reverse_line_reader;
pub mod line_reader;
pub mod index;

pub use error::Error;
pub use reverse_line_reader::{ReverseLineReader, LineWithPosition};
pub use line_reader::LineReader;
pub use index::FileIndexContext;

#[cfg(test)]
mod tests {

}
