use std::collections::LinkedList;

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use crate::error::Result;

const BUFFER_SIZE: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineWithPosition {
    pub line: String,
    pub start_pos: u64,
    pub end_pos: u64,
}

pub struct ReverseLineReader {
    file: File,
    file_start_pos: u64,
    file_end_pos: u64,
    current_pos: u64,
    buffer: Vec<u8>,
    buffer_end: usize,
    splits: LinkedList<(Vec<u8>, u64, u64)>,
}

impl ReverseLineReader {
    pub async fn new(path: impl AsRef<std::path::Path>, file_start_pos: Option<u64>, file_end_pos: Option<u64>) -> Result<Self> {
        let mut file = File::open(path).await?;
        let file_size = file.seek(SeekFrom::End(0)).await?;
        let file_end_pos = if let Some(file_end_pos) = file_end_pos { if file_end_pos > file_size { file_size } else { file_end_pos } } else { file_size };
        let file_start_pos = if let Some(file_start_pos) = file_start_pos { if file_start_pos <= file_end_pos { file_start_pos } else { file_end_pos } } else { 0 };

        Ok(Self {
            file,
            file_start_pos,
            file_end_pos,
            current_pos: file_end_pos,
            buffer: vec![0; BUFFER_SIZE],
            buffer_end: 0,
            splits: LinkedList::new(),
        })
    }

    fn save_buffer(&mut self, start: usize, end: usize) {
        let start_pos = self.current_pos + start as u64;
        let end_pos = self.current_pos + end as u64;
        self.splits.push_front((self.buffer[start..end].to_vec(), start_pos, end_pos));
    }

    fn pop_line_without_newline(&mut self) -> Result<Option<LineWithPosition>> {
        if self.splits.is_empty() {
            return Ok(None);
        }
        else {
            //计算总长
            let mut len = 0 as usize;
            let mut start_pos = self.file_end_pos;
            let mut end_pos = self.file_start_pos;
            for (split, s, e) in self.splits.iter() {
                len += split.len();
                if *s < start_pos {
                    start_pos = *s;
                }
                if *e > end_pos {
                    end_pos = *e;
                }
            }
            let mut line = Vec::with_capacity(len);
            while let Some(split) = self.splits.pop_front() {
                //最后一个split移除结尾换行符，但pos应包含换行符
                let mut end = split.0.len();
                if self.splits.is_empty() {
                    if end > 0 && split.0[end - 1] == b'\n' {
                        end -= 1;
                        if end > 0 && split.0[end - 1] == b'\r' {
                            end -= 1;
                        }
                    }
                }
                line.extend_from_slice(&split.0[..end]);
            }
            let result = String::from_utf8(line)?;
            Ok(Some(LineWithPosition {
                line: result,
                start_pos,
                end_pos,
            }))
        }
    }

    pub async fn next_line(&mut self) -> Result<Option<LineWithPosition>> {
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

            //buffer已搜索完，无换行符：整块是当前行的一部分（行跨 chunk），
            //先保存为片段，再继续往前读，避免覆盖丢失
            if self.buffer_end > 0 {
                self.save_buffer(0, self.buffer_end);
                self.buffer_end = 0;
            }

            //文件到达开头，输出剩余buffer
            if self.current_pos == self.file_start_pos {
                return self.pop_line_without_newline();
            }

            //buffer已搜索完，继续读取文件
            let read_len = std::cmp::min(BUFFER_SIZE, (self.current_pos - self.file_start_pos) as usize);
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
    use tempfile::tempdir;

    /// 测试辅助：同步写文件。tokio 异步 write_all 与随后的 open 存在可见性竞态
    /// （偶发读到空文件，见 race probe），测试必须用同步写保证写入完成后读方能观察到数据
    fn write_test_file(path: &std::path::Path, content: &[u8]) {
        std::fs::write(path, content).unwrap();
    }

    #[tokio::test]
    async fn test_reverse_line_reader() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");

        write_test_file(&file_path, b"line 1\nline 2\r\nline 3\nline 4");

        let mut reader = ReverseLineReader::new(&file_path, None, None).await.unwrap();

        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 4".to_string(),
            start_pos: 22,
            end_pos: 28,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 3".to_string(),
            start_pos: 15,
            end_pos: 22,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 2".to_string(),
            start_pos: 7,
            end_pos: 15,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 1".to_string(),
            start_pos: 0,
            end_pos: 7,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_empty_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("empty.txt");
        std::fs::File::create(&file_path).unwrap();
        let mut reader = ReverseLineReader::new(&file_path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_single_line() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("single.txt");

        write_test_file(&file_path, b"single line");

        let mut reader = ReverseLineReader::new(&file_path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "single line".to_string(),
            start_pos: 0,
            end_pos: 11,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_ends_with_newline() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("ends_with_newline.txt");

        write_test_file(&file_path, b"line 1\nline 2\n");

        let mut reader = ReverseLineReader::new(&file_path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 2".to_string(),
            start_pos: 7,
            end_pos: 14,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 1".to_string(),
            start_pos: 0,
            end_pos: 7,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_ends_with_crlf() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("ends_with_crlf.txt");

        write_test_file(&file_path, b"line 1\r\nline 2\r\n");

        let mut reader = ReverseLineReader::new(&file_path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 2".to_string(),
            start_pos: 8,
            end_pos: 16,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 1".to_string(),
            start_pos: 0,
            end_pos: 8,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_ends_with_double_newline() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("ends_with_double_newline.txt");

        write_test_file(&file_path, b"line 1\nline 2\n\n");

        let mut reader = ReverseLineReader::new(&file_path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(),
            start_pos: 14,
            end_pos: 15,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 2".to_string(),
            start_pos: 7,
            end_pos: 14,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 1".to_string(),
            start_pos: 0,
            end_pos: 7,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_ends_with_double_crlf() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("ends_with_double_crlf.txt");

        write_test_file(&file_path, b"line 1\r\nline 2\r\n\r\n");

        let mut reader = ReverseLineReader::new(&file_path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(),
            start_pos: 16,
            end_pos: 18,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 2".to_string(),
            start_pos: 8,
            end_pos: 16,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 1".to_string(),
            start_pos: 0,
            end_pos: 8,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_with_empty_lines_in_middle() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("empty_lines_in_middle.txt");

        write_test_file(&file_path, b"line 1\n\nline 2\nline 3");

        let mut reader = ReverseLineReader::new(&file_path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 3".to_string(),
            start_pos: 15,
            end_pos: 21,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 2".to_string(),
            start_pos: 8,
            end_pos: 15,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(),
            start_pos: 7,
            end_pos: 8,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 1".to_string(),
            start_pos: 0,
            end_pos: 7,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_with_empty_lines_in_middle_crlf() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("empty_lines_in_middle_crlf.txt");

        write_test_file(&file_path, b"line 1\r\n\r\nline 2\r\nline 3");

        let mut reader = ReverseLineReader::new(&file_path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 3".to_string(),
            start_pos: 18,
            end_pos: 24,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 2".to_string(),
            start_pos: 10,
            end_pos: 18,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(),
            start_pos: 8,
            end_pos: 10,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 1".to_string(),
            start_pos: 0,
            end_pos: 8,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_only_newline() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("only_newline.txt");

        write_test_file(&file_path, b"\n");

        let mut reader = ReverseLineReader::new(&file_path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(),
            start_pos: 0,
            end_pos: 1,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_only_crlf() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("only_crlf.txt");

        write_test_file(&file_path, b"\r\n");

        let mut reader = ReverseLineReader::new(&file_path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(),
            start_pos: 0,
            end_pos: 2,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_only_two_newlines() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("only_two_newlines.txt");

        write_test_file(&file_path, b"\n\n");

        let mut reader = ReverseLineReader::new(&file_path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(),
            start_pos: 1,
            end_pos: 2,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(),
            start_pos: 0,
            end_pos: 1,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_only_two_crlfs() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("only_two_crlfs.txt");

        write_test_file(&file_path, b"\r\n\r\n");

        let mut reader = ReverseLineReader::new(&file_path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(),
            start_pos: 2,
            end_pos: 4,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(),
            start_pos: 0,
            end_pos: 2,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_custom_start() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");

        write_test_file(&file_path, "第一行\n第二行\r\n第三行\n第四行".as_bytes());

        let mut reader = ReverseLineReader::new(&file_path, Some(13), None).await.unwrap();

        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "第四行".to_string(),
            start_pos: 31,
            end_pos: 40,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "第三行".to_string(),
            start_pos: 21,
            end_pos: 31,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "二行".to_string(),
            start_pos: 13,
            end_pos: 21,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_custom_end() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");

        write_test_file(&file_path, "第一行\n第二行\r\n第三行\n第四行".as_bytes());

        let mut reader = ReverseLineReader::new(&file_path, None, Some(27)).await.unwrap();

        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "第三".to_string(),
            start_pos: 21,
            end_pos: 27,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "第二行".to_string(),
            start_pos: 10,
            end_pos: 21,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "第一行".to_string(),
            start_pos: 0,
            end_pos: 10,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_custom_start_end() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");

        write_test_file(&file_path, "第一行\n第二行\r\n第三行\n第四行".as_bytes());

        let mut reader = ReverseLineReader::new(&file_path, Some(13), Some(27)).await.unwrap();

        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "第三".to_string(),
            start_pos: 21,
            end_pos: 27,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "二行".to_string(),
            start_pos: 13,
            end_pos: 21,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    /// 回归测试：行跨越 BUFFER_SIZE(4096) chunk 边界时，反向读取不能丢失/截断片段
    #[tokio::test]
    async fn test_lines_crossing_chunk_boundary() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("chunk_cross.txt");

        // 每行 300 字节，共 20 行（6000 字节 > BUFFER_SIZE 4096），
        // 行边界会落在 chunk 边界两侧（第 4096 字节落在第 14 行内部）
        let line_len = 300usize;
        let mut content: Vec<u8> = Vec::new();
        let mut expected: Vec<String> = Vec::new();
        for i in 0..20 {
            let mut line = format!("L{:02}-{}", i, "x".repeat(line_len - 5));
            line.push('\n');
            content.extend_from_slice(line.as_bytes());
            expected.push(line.trim_end().to_string());
        }
        write_test_file(&file_path, &content);
        // 保证确实跨越了 chunk 边界
        assert!(content.len() > BUFFER_SIZE);
        let mut pos = 0u64;
        let mut expected_pos: Vec<(String, u64, u64)> = Vec::new();
        for e in &expected {
            let end = pos + (e.len() + 1) as u64; // +1 换行符
            expected_pos.push((e.clone(), pos, end));
            pos = end;
        }

        let mut reader = ReverseLineReader::new(&file_path, None, None).await.unwrap();
        let mut got: Vec<(String, u64, u64)> = Vec::new();
        while let Some(lp) = reader.next_line().await.unwrap() {
            got.push((lp.line, lp.start_pos, lp.end_pos));
        }

        // 反向读取应与正向内容顺序相反，内容与位置完全一致
        expected_pos.reverse();
        assert_eq!(got.len(), expected_pos.len());
        for (g, e) in got.iter().zip(expected_pos.iter()) {
            assert_eq!(g, e, "line mismatch: got {:?} expected {:?}", g, e);
        }
    }

    /// 回归测试：从中间位置（end_byte）反向读取，行同样可能跨 chunk 边界
    #[tokio::test]
    async fn test_lines_crossing_chunk_boundary_with_end() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("chunk_cross_end.txt");

        let line_len = 300usize;
        let mut content: Vec<u8> = Vec::new();
        let mut lines: Vec<String> = Vec::new();
        for i in 0..20 {
            let mut line = format!("L{:02}-{}", i, "y".repeat(line_len - 5));
            line.push('\n');
            content.extend_from_slice(line.as_bytes());
            lines.push(line.trim_end().to_string());
        }
        write_test_file(&file_path, &content);
        // 截断到第 15 行末尾（含换行）：每行 line_len=300 字节，前 15 行共 4500 字节
        let line_size = line_len as u64; // "Lxx-" 4 字符 + (line_len-5) 填充 + '\n' = line_len 字节
        let end_byte = 15 * line_size;
        assert!(end_byte > BUFFER_SIZE as u64);

        let mut reader = ReverseLineReader::new(&file_path, None, Some(end_byte)).await.unwrap();
        let mut got: Vec<String> = Vec::new();
        while let Some(lp) = reader.next_line().await.unwrap() {
            got.push(lp.line);
        }
        // 期望：前 15 行倒序
        let expected: Vec<String> = lines[..15].iter().rev().cloned().collect();
        assert_eq!(got, expected);
    }
}
