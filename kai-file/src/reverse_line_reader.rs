use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use crate::error::{Error, Result};

const BUFFER_SIZE: usize = 4096;

pub struct ReverseLineReader {
    file: File,
    file_size: u64,
    current_pos: u64,
    buffer: Vec<u8>,
    buffer_start: usize,
    buffer_end: usize,
    leftover: Vec<u8>,
}

impl ReverseLineReader {
    pub async fn new(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let mut file = File::open(path).await?;
        let file_size = file.seek(SeekFrom::End(0)).await?;
        let mut current_pos = file_size;

        // 忽略文件结尾的一个换行符
        if file_size > 0 {
            let mut temp_buffer = [0u8; 2];
            let check_len = std::cmp::min(2, file_size as usize);
            let check_pos = file_size - check_len as u64;
            
            file.seek(SeekFrom::Start(check_pos)).await?;
            file.read_exact(&mut temp_buffer[..check_len]).await?;
            
            // 检查是否以\n结尾
            if temp_buffer[check_len - 1] == b'\n' {
                if check_len >= 2 && temp_buffer[check_len - 2] == b'\r' {
                    // 以\r\n结尾，跳过两个字符
                    current_pos -= 2;
                } else {
                    // 以\n结尾，跳过一个字符
                    current_pos -= 1;
                }
            }
        }

        Ok(Self {
            file,
            file_size,
            current_pos,
            buffer: vec![0; BUFFER_SIZE],
            buffer_start: 0,
            buffer_end: 0,
            leftover: Vec::new(),
        })
    }

    pub async fn next_line(&mut self) -> Result<Option<String>> {
        loop {
            for i in (self.buffer_start..self.buffer_end).rev() {
                if self.buffer[i] == b'\n' {
                    let line_start = i + 1;
                    let line_end = self.buffer_end;

                    let mut line = Vec::with_capacity(self.leftover.len() + (line_end - line_start));
                    line.extend_from_slice(&self.buffer[line_start..line_end]);
                    line.extend_from_slice(&self.leftover);

                    self.leftover.clear();
                    self.buffer_end = i;

                    if !line.is_empty() && line[line.len() - 1] == b'\r' {
                        line.pop();
                    }

                    return Ok(Some(String::from_utf8(line)?));
                }
            }

            if self.current_pos == 0 {
                if !self.buffer.is_empty() && self.buffer_start < self.buffer_end {
                    let mut line = Vec::with_capacity(self.leftover.len() + (self.buffer_end - self.buffer_start));
                    line.extend_from_slice(&self.buffer[self.buffer_start..self.buffer_end]);
                    line.extend_from_slice(&self.leftover);
                    self.leftover.clear();
                    self.buffer_end = self.buffer_start;

                    if !line.is_empty() && line[line.len() - 1] == b'\r' {
                        line.pop();
                    }

                    return Ok(Some(String::from_utf8(line)?));
                } else if !self.leftover.is_empty() {
                    let mut line = std::mem::take(&mut self.leftover);

                    if !line.is_empty() && line[line.len() - 1] == b'\r' {
                        line.pop();
                    }

                    return Ok(Some(String::from_utf8(line)?));
                } else {
                    return Ok(None);
                }
            }

            self.leftover.splice(0..0, self.buffer[self.buffer_start..self.buffer_end].iter().cloned());

            let read_len = std::cmp::min(BUFFER_SIZE, self.current_pos as usize);
            self.current_pos -= read_len as u64;

            self.file.seek(SeekFrom::Start(self.current_pos)).await?;
            self.file.read_exact(&mut self.buffer[..read_len]).await?;

            self.buffer_start = 0;
            self.buffer_end = read_len;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_reverse_line_reader() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");

        let mut file = File::create(&file_path).await.unwrap();
        file.write_all(b"line 1\nline 2\r\nline 3\nline 4").await.unwrap();

        let mut reader = ReverseLineReader::new(&file_path).await.unwrap();

        assert_eq!(reader.next_line().await.unwrap(), Some("line 4".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), Some("line 3".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), Some("line 2".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), Some("line 1".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_empty_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("empty.txt");
        File::create(&file_path).await.unwrap();

        let mut reader = ReverseLineReader::new(&file_path).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_single_line() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("single.txt");

        let mut file = File::create(&file_path).await.unwrap();
        file.write_all(b"single line").await.unwrap();

        let mut reader = ReverseLineReader::new(&file_path).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some("single line".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_ends_with_newline() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("ends_with_newline.txt");

        let mut file = File::create(&file_path).await.unwrap();
        file.write_all(b"line 1\nline 2\n").await.unwrap();

        let mut reader = ReverseLineReader::new(&file_path).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some("line 2".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), Some("line 1".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_ends_with_crlf() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("ends_with_crlf.txt");

        let mut file = File::create(&file_path).await.unwrap();
        file.write_all(b"line 1\r\nline 2\r\n").await.unwrap();

        let mut reader = ReverseLineReader::new(&file_path).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some("line 2".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), Some("line 1".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_ends_with_double_newline() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("ends_with_double_newline.txt");

        let mut file = File::create(&file_path).await.unwrap();
        file.write_all(b"line 1\nline 2\n\n").await.unwrap();

        let mut reader = ReverseLineReader::new(&file_path).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some("".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), Some("line 2".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), Some("line 1".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_ends_with_double_crlf() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("ends_with_double_crlf.txt");

        let mut file = File::create(&file_path).await.unwrap();
        file.write_all(b"line 1\r\nline 2\r\n\r\n").await.unwrap();

        let mut reader = ReverseLineReader::new(&file_path).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some("".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), Some("line 2".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), Some("line 1".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_with_empty_lines_in_middle() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("empty_lines_in_middle.txt");

        let mut file = File::create(&file_path).await.unwrap();
        file.write_all(b"line 1\n\nline 2\nline 3").await.unwrap();

        let mut reader = ReverseLineReader::new(&file_path).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some("line 3".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), Some("line 2".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), Some("".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), Some("line 1".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_with_empty_lines_in_middle_crlf() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("empty_lines_in_middle_crlf.txt");

        let mut file = File::create(&file_path).await.unwrap();
        file.write_all(b"line 1\r\n\r\nline 2\r\nline 3").await.unwrap();

        let mut reader = ReverseLineReader::new(&file_path).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some("line 3".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), Some("line 2".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), Some("".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), Some("line 1".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_only_newline() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("only_newline.txt");

        let mut file = File::create(&file_path).await.unwrap();
        file.write_all(b"\n").await.unwrap();

        let mut reader = ReverseLineReader::new(&file_path).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_only_crlf() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("only_crlf.txt");

        let mut file = File::create(&file_path).await.unwrap();
        file.write_all(b"\r\n").await.unwrap();

        let mut reader = ReverseLineReader::new(&file_path).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_mixed_newlines() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("mixed_newlines.txt");

        let mut file = File::create(&file_path).await.unwrap();
        file.write_all(b"line1\nline2\r\nline3\nline4\r\n").await.unwrap();

        let mut reader = ReverseLineReader::new(&file_path).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some("line4".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), Some("line3".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), Some("line2".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), Some("line1".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }
}
