use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use crate::error::Result;
use crate::reverse_line_reader::LineWithPosition;

const BUFFER_SIZE: usize = 4096;

pub struct LineReader {
    file: File,
    end_pos: u64,
    current_pos: u64,
    line_start_pos: u64,
    buffer: Vec<u8>,
    buffer_len: usize,
    leftover: Vec<u8>,
}

impl LineReader {
    pub async fn new(
        path: impl AsRef<std::path::Path>,
        start_pos: Option<u64>,
        end_pos: Option<u64>,
    ) -> Result<Self> {
        let mut file = File::open(path).await?;
        let file_size = file.seek(SeekFrom::End(0)).await?;
        let end_pos = end_pos.map(|e| e.min(file_size)).unwrap_or(file_size);
        let start_pos = start_pos.map(|s| s.min(end_pos)).unwrap_or(0);
        file.seek(SeekFrom::Start(start_pos)).await?;
        Ok(Self {
            file,
            end_pos,
            current_pos: start_pos,
            line_start_pos: start_pos,
            buffer: vec![0; BUFFER_SIZE],
            buffer_len: 0,
            leftover: Vec::new(),
        })
    }

    pub async fn next_line(&mut self) -> Result<Option<LineWithPosition>> {
        loop {
            if self.buffer_len == 0 {
                let remaining = self.end_pos - self.current_pos;
                if remaining == 0 {
                    if self.leftover.is_empty() {
                        return Ok(None);
                    }
                    let line_data = std::mem::take(&mut self.leftover);
                    let start_pos = self.line_start_pos;
                    let end_pos = self.current_pos;
                    let line = String::from_utf8(line_data)?;
                    self.line_start_pos = self.current_pos;
                    return Ok(Some(LineWithPosition { line, start_pos, end_pos }));
                }
                let read_size = std::cmp::min(BUFFER_SIZE, remaining as usize);
                self.file.read_exact(&mut self.buffer[..read_size]).await?;
                self.buffer_len = read_size;
            }

            for i in 0..self.buffer_len {
                if self.buffer[i] == b'\n' {
                    let line_start_pos = self.line_start_pos;
                    let line_end_pos = self.current_pos + i as u64 + 1;

                    let mut line_data = if self.leftover.is_empty() {
                        self.buffer[..i].to_vec()
                    } else {
                        let mut combined = std::mem::take(&mut self.leftover);
                        combined.extend_from_slice(&self.buffer[..i]);
                        combined
                    };

                    // 去掉结尾 \r（\r\n 的情况）
                    if line_data.last() == Some(&b'\r') {
                        line_data.pop();
                    }

                    let bytes_consumed = i + 1;
                    self.current_pos += bytes_consumed as u64;
                    self.buffer.copy_within(bytes_consumed.., 0);
                    self.buffer_len -= bytes_consumed;
                    self.line_start_pos = self.current_pos;

                    let line = String::from_utf8(line_data)?;
                    return Ok(Some(LineWithPosition {
                        line,
                        start_pos: line_start_pos,
                        end_pos: line_end_pos,
                    }));
                }
            }

            // 当前 buffer 中无换行符 → 整块移入 leftover，继续读
            self.leftover.extend_from_slice(&self.buffer[..self.buffer_len]);
            self.current_pos += self.buffer_len as u64;
            self.buffer_len = 0;
        }
    }

    pub fn current_pos(&self) -> u64 {
        self.current_pos
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    use tempfile::tempdir;

    async fn write_file(dir: &tempfile::TempDir, name: &str, content: &[u8]) -> std::path::PathBuf {
        let path = dir.path().join(name);
        let mut file = File::create(&path).await.unwrap();
        file.write_all(content).await.unwrap();
        path
    }

    #[tokio::test]
    async fn test_line_reader_basic() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "basic.txt", b"line 1\nline 2\r\nline 3\nline 4").await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 1".to_string(),
            start_pos: 0, end_pos: 7,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 2".to_string(),
            start_pos: 7, end_pos: 15,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 3".to_string(),
            start_pos: 15, end_pos: 22,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 4".to_string(),
            start_pos: 22, end_pos: 28,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_empty_file() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "empty.txt", b"").await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_single_line() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "single.txt", b"single line").await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "single line".to_string(),
            start_pos: 0, end_pos: 11,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_ends_with_newline() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "ends_newline.txt", b"line 1\nline 2\n").await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 1".to_string(),
            start_pos: 0, end_pos: 7,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 2".to_string(),
            start_pos: 7, end_pos: 14,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_ends_with_crlf() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "ends_crlf.txt", b"line 1\r\nline 2\r\n").await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 1".to_string(),
            start_pos: 0, end_pos: 8,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 2".to_string(),
            start_pos: 8, end_pos: 16,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_ends_with_double_newline() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "ends_double_nl.txt", b"line 1\nline 2\n\n").await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 1".to_string(), start_pos: 0, end_pos: 7,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 2".to_string(), start_pos: 7, end_pos: 14,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(), start_pos: 14, end_pos: 15,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_ends_with_double_crlf() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "ends_double_crlf.txt", b"line 1\r\nline 2\r\n\r\n").await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 1".to_string(), start_pos: 0, end_pos: 8,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 2".to_string(), start_pos: 8, end_pos: 16,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(), start_pos: 16, end_pos: 18,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_with_empty_lines_in_middle() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "empty_mid.txt", b"line 1\n\nline 2\nline 3").await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 1".to_string(), start_pos: 0, end_pos: 7,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(), start_pos: 7, end_pos: 8,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 2".to_string(), start_pos: 8, end_pos: 15,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 3".to_string(), start_pos: 15, end_pos: 21,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_only_newline() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "only_nl.txt", b"\n").await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(), start_pos: 0, end_pos: 1,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_only_two_newlines() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "only_two_nl.txt", b"\n\n").await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(), start_pos: 0, end_pos: 1,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(), start_pos: 1, end_pos: 2,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_only_crlf() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "only_crlf.txt", b"\r\n").await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(), start_pos: 0, end_pos: 2,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_only_two_crlfs() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "only_two_crlfs.txt", b"\r\n\r\n").await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(), start_pos: 0, end_pos: 2,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(), start_pos: 2, end_pos: 4,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_with_empty_lines_in_middle_crlf() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "empty_mid_crlf.txt", b"line 1\r\n\r\nline 2\r\nline 3").await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 1".to_string(), start_pos: 0, end_pos: 8,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(), start_pos: 8, end_pos: 10,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 2".to_string(), start_pos: 10, end_pos: 18,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 3".to_string(), start_pos: 18, end_pos: 24,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_custom_start() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "cs.txt", "第一行\n第二行\r\n第三行\n第四行".as_bytes()).await;
        let mut reader = LineReader::new(&path, Some(13), None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "二行".to_string(), start_pos: 13, end_pos: 21,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "第三行".to_string(), start_pos: 21, end_pos: 31,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "第四行".to_string(), start_pos: 31, end_pos: 40,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_custom_end() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "ce.txt", "第一行\n第二行\r\n第三行\n第四行".as_bytes()).await;
        let mut reader = LineReader::new(&path, None, Some(27)).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "第一行".to_string(), start_pos: 0, end_pos: 10,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "第二行".to_string(), start_pos: 10, end_pos: 21,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "第三".to_string(), start_pos: 21, end_pos: 27,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_custom_start_end() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "cse.txt", "第一行\n第二行\r\n第三行\n第四行".as_bytes()).await;
        let mut reader = LineReader::new(&path, Some(13), Some(27)).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "二行".to_string(), start_pos: 13, end_pos: 21,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "第三".to_string(), start_pos: 21, end_pos: 27,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_long_line() {
        let dir = tempdir().unwrap();
        let long = "A".repeat(BUFFER_SIZE * 2 + 100);
        let path = write_file(&dir, "long.txt", format!("short\n{}\nend", long).as_bytes()).await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        let first = reader.next_line().await.unwrap().unwrap();
        assert_eq!(first.line, "short");
        let second = reader.next_line().await.unwrap().unwrap();
        assert_eq!(second.line.len(), BUFFER_SIZE * 2 + 100);
        assert_eq!(second.line, long);
        let third = reader.next_line().await.unwrap().unwrap();
        assert_eq!(third.line, "end");
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_chinese() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "chinese.txt", "第一行\n第二行\r\n第三行\n第四行".as_bytes()).await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "第一行".to_string(), start_pos: 0, end_pos: 10,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "第二行".to_string(), start_pos: 10, end_pos: 21,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "第三行".to_string(), start_pos: 21, end_pos: 31,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "第四行".to_string(), start_pos: 31, end_pos: 40,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }
}
