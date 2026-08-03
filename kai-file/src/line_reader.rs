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
    use tempfile::tempdir;

    async fn write_file(dir: &tempfile::TempDir, name: &str, content: &[u8]) -> std::path::PathBuf {
        let path = dir.path().join(name);
        // 同步写：tokio 异步 write_all 与随后的 open 存在可见性竞态（偶发读到空文件），
        // 测试必须用同步写保证写入完成后读方能观察到数据
        std::fs::write(&path, content).unwrap();
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

    /// 回归测试：行跨越 BUFFER_SIZE(4096) chunk 边界时，正向读取内容与位置完整不丢失
    #[tokio::test]
    async fn test_lines_crossing_chunk_boundary() {
        let dir = tempdir().unwrap();

        // 每行 300 字节（"Lxx-" 4 字符 + 295 填充 + '\n'），共 20 行 6000 字节 > BUFFER_SIZE，
        // 行边界会落在 chunk 边界两侧（第 4096 字节落在某行内部）
        let line_len = 300usize;
        let mut content: Vec<u8> = Vec::new();
        let mut expected: Vec<(String, u64, u64)> = Vec::new();
        for i in 0..20 {
            let line = format!("L{:02}-{}", i, "a".repeat(line_len - 5));
            let start = content.len() as u64;
            content.extend_from_slice(line.as_bytes());
            content.push(b'\n');
            let end = content.len() as u64;
            expected.push((line, start, end));
        }
        assert!(content.len() > BUFFER_SIZE);
        let path = write_file(&dir, "chunk_cross.txt", &content).await;

        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        let mut got: Vec<(String, u64, u64)> = Vec::new();
        while let Some(lp) = reader.next_line().await.unwrap() {
            got.push((lp.line, lp.start_pos, lp.end_pos));
        }
        assert_eq!(got, expected, "跨块读取结果与预期不一致");
    }

    /// 回归测试：换行符恰好落在 chunk 边界（文件总长 = BUFFER_SIZE，末字节为 \n）
    #[tokio::test]
    async fn test_lines_ending_at_chunk_boundary() {
        let dir = tempdir().unwrap();

        // 每行 256 字节，16 行共 4096 字节：第 16 行的 '\n' 恰好是文件最后一个字节
        let line_len = 256usize;
        let mut content: Vec<u8> = Vec::new();
        let mut expected: Vec<(String, u64, u64)> = Vec::new();
        for i in 0..16 {
            let line = format!("B{:02}-{}", i, "b".repeat(line_len - 5));
            let start = content.len() as u64;
            content.extend_from_slice(line.as_bytes());
            content.push(b'\n');
            let end = content.len() as u64;
            expected.push((line, start, end));
        }
        assert_eq!(content.len(), BUFFER_SIZE);
        let path = write_file(&dir, "chunk_boundary.txt", &content).await;

        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        let mut got: Vec<(String, u64, u64)> = Vec::new();
        while let Some(lp) = reader.next_line().await.unwrap() {
            got.push((lp.line, lp.start_pos, lp.end_pos));
        }
        assert_eq!(got, expected, "边界对齐行读取结果与预期不一致");
    }

    /// 回归测试：从非块边界位置（8 行 * 256 = 2048）开始读取，后续行内容与位置完整
    #[tokio::test]
    async fn test_custom_start_at_chunk_boundary() {
        let dir = tempdir().unwrap();

        let line_len = 256usize;
        let mut content: Vec<u8> = Vec::new();
        let mut expected: Vec<(String, u64, u64)> = Vec::new();
        for i in 0..16 {
            let line = format!("C{:02}-{}", i, "c".repeat(line_len - 5));
            content.extend_from_slice(line.as_bytes());
            content.push(b'\n');
            expected.push((line, (i * line_len) as u64, ((i + 1) * line_len) as u64));
        }
        let path = write_file(&dir, "chunk_start.txt", &content).await;

        // 从 2048 开始读（位于第二个 chunk 内），验证跨 chunk 的 start_pos 语义
        let start = 8 * line_len as u64;
        let mut reader = LineReader::new(&path, Some(start), None).await.unwrap();
        let mut got: Vec<(String, u64, u64)> = Vec::new();
        while let Some(lp) = reader.next_line().await.unwrap() {
            got.push((lp.line, lp.start_pos, lp.end_pos));
        }
        assert_eq!(got, expected[8..], "从 chunk 中间位置开始读取结果与预期不一致");
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
