use std::collections::LinkedList;

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use crate::error::Result;

const BUFFER_SIZE: usize = 4096;

pub struct ReverseLineReader {
    file: File,
    current_pos: u64,
    buffer: Vec<u8>,
    buffer_end: usize,
    splits: LinkedList<Vec<u8>>,
}

impl ReverseLineReader {
    pub async fn new(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let mut file = File::open(path).await?;
        let file_size = file.seek(SeekFrom::End(0)).await?;

        Ok(Self {
            file,
            current_pos: file_size,
            buffer: vec![0; BUFFER_SIZE],
            buffer_end: 0,
            splits: LinkedList::new(),
        })
    }

    fn save_buffer(&mut self, start: usize, end: usize) {
        self.splits.push_front(self.buffer[start..end].to_vec());
    }

    fn pop_line_without_newline(&mut self) -> Result<Option<String>> {
        if self.splits.is_empty() {
            return Ok(None);
        }
        else {
            //计算总长
            let mut len = 0 as usize;
            for split in self.splits.iter_mut() {
                len += split.len();
            }
            //填入字节
            let mut line = Vec::with_capacity(len);
            while let Some(split) = self.splits.pop_front() {
                //最后一个split移除结尾换行符
                let mut end = split.len();
                if self.splits.is_empty() {
                    if end > 0 && split[end - 1] == b'\n' {
                        end -= 1;
                        if end > 0 && split[end - 1] == b'\r' {
                            end -= 1;
                        }
                    }
                }
                line.extend_from_slice(&split[..end]);
            }
            let result = String::from_utf8(line)?;
            Ok(Some(result))
        }
    }

    pub async fn next_line(&mut self) -> Result<Option<String>> {
        loop {
            //搜索buffer，先跳过结尾\n后再搜索前一个\n
            let search_end = if self.splits.is_empty() && self.buffer_end > 0 && self.buffer[self.buffer_end - 1] == b'\n' { self.buffer_end - 1 } else { self.buffer_end };
            for i in (0..search_end).rev() {
                if self.buffer[i] == b'\n' {
                    self.save_buffer(i + 1, self.buffer_end);
                    self.buffer_end = i + 1;
                    let line = self.pop_line_without_newline()?;
                    return Ok(line);
                }
            }

            //buffer已搜索完，文件到达开头，输出剩余buffer
            if self.current_pos == 0 {
                if self.buffer_end > 0 {
                    self.save_buffer(0, self.buffer_end);
                }
                self.buffer_end = 0;
                return self.pop_line_without_newline();
            }

            //buffer已搜索完，继续读取文件
            let read_len = std::cmp::min(BUFFER_SIZE, self.current_pos as usize);
            self.current_pos -= read_len as u64;
            self.file.seek(SeekFrom::Start(self.current_pos)).await?;
            self.file.read_exact(&mut self.buffer[..read_len]).await?;
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
        assert_eq!(reader.next_line().await.unwrap(), Some("".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_only_crlf() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("only_crlf.txt");

        let mut file = File::create(&file_path).await.unwrap();
        file.write_all(b"\r\n").await.unwrap();

        let mut reader = ReverseLineReader::new(&file_path).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some("".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_only_two_newlines() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("only_two_newlines.txt");

        let mut file = File::create(&file_path).await.unwrap();
        file.write_all(b"\n\n").await.unwrap();

        let mut reader = ReverseLineReader::new(&file_path).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some("".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), Some("".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_only_two_crlfs() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("only_two_crlfs.txt");

        let mut file = File::create(&file_path).await.unwrap();
        file.write_all(b"\r\n\r\n").await.unwrap();

        let mut reader = ReverseLineReader::new(&file_path).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some("".to_string()));
        assert_eq!(reader.next_line().await.unwrap(), Some("".to_string()));
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
