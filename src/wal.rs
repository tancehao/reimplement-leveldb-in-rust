use crate::io::Storage;
use crate::utils::crc::crc32;
use crate::LError;
use bytes::{BufMut, Bytes, BytesMut};
use std::collections::VecDeque;

const BLOCK_SIZE: usize = 32 * 1024;
const RECORD_TYPE_FULL: u8 = 1;
const RECORD_TYPE_FIRST: u8 = 2;
const RECORD_TYPE_MIDDLE: u8 = 3;
const RECORD_TYPE_LAST: u8 = 4;

pub struct WalReader<T: Storage> {
    f: T,
    total_size: u64,
    read_size: u64,
    fname: String,
    buf: Bytes,
    read_times: usize,
}

impl<T: Storage> WalReader<T> {
    pub fn new(f: T, fname: String) -> Result<Self, LError> {
        Ok(Self {
            total_size: f.size()?,
            f: f,
            read_size: 0,
            fname,
            buf: Bytes::new(),
            read_times: 0,
        })
    }

    pub fn next(&mut self) -> Result<Option<Vec<u8>>, LError> {
        let mut log = vec![];
        loop {
            if self.buf.is_empty() {
                if self.read_size >= self.total_size {
                    break;
                }
                let mut buf = BytesMut::with_capacity(BLOCK_SIZE);
                buf.resize(BLOCK_SIZE, 0);
                self.f.read(&mut buf)?;
                // println!("buf after read from wal: {:?}", buf);
                self.buf = buf.freeze();
                self.read_times += 1;
                self.read_size += BLOCK_SIZE as u64;
            }
            if self.buf.len() < 7 {
                let _ = self.buf.split_to(self.buf.len());
                continue;
            }
            let checksum = u32::from_le_bytes(self.buf[..4].as_ref().try_into().unwrap());
            let length = u16::from_le_bytes(self.buf[4..6].as_ref().try_into().unwrap());
            let t = self.buf[6];

            let _ = self.buf.split_to(7);
            if length == 0 {
                continue;
            }
            let data = self.buf.split_to(length as usize);
            // println!("self.buf length after split_to: {}, t: {}, data: {:?}", self.buf.len(), t, data);
            if crc32(data.as_ref()) != checksum {
                return Err(LError::InvalidFile(format!(
                    "data in file {} is malformed",
                    self.fname
                )));
            }
            match t {
                RECORD_TYPE_FULL => return Ok(Some(data.as_ref().to_vec())),
                RECORD_TYPE_FIRST | RECORD_TYPE_MIDDLE => log.extend_from_slice(data.as_ref()),
                RECORD_TYPE_LAST => {
                    log.extend_from_slice(data.as_ref());
                    return Ok(Some(log));
                }
                _ => {
                    return Err(LError::InvalidFile(format!(
                        "data in file {} is malformed",
                        self.fname
                    )))
                }
            }
        }
        Ok(None)
    }
}

pub(crate) struct WalWriter<T: Storage> {
    f: T,
    buf: VecDeque<BytesMut>,
}

impl<S: Storage> Drop for WalWriter<S> {
    fn drop(&mut self) {
        let _ = self.f.flush();
    }
}

impl<T: Storage> WalWriter<T> {
    pub(crate) fn new(f: T) -> Self {
        Self {
            f: f,
            buf: VecDeque::new(),
        }
    }

    fn write_buf(&mut self) -> Result<(), LError> {
        while let Some(mut buf) = self.buf.pop_front() {
            if buf.len() > 0 {
                // println!("writing buf to file: {:?}", buf);
                self.f.write_all(buf.as_ref())?;
                let _ = buf.split_to(buf.len());
            }
            if buf.capacity() > 0 {
                self.buf.push_front(buf);
                break;
            }
        }
        Ok(())
    }

    pub(crate) fn append(&mut self, log: &[u8]) -> Result<(), LError> {
        let mut start = 0;
        while start < log.len() {
            let s = self.append_record(log[start..].as_ref(), start == 0);
            start += s;
        }
        self.write_buf()
    }

    fn append_record(&mut self, data: &[u8], is_first: bool) -> usize {
        let length = data.len();
        let need_new_buf = match self.buf.back() {
            None => true,
            Some(b) => b.capacity() == b.len(),
        };
        if need_new_buf {
            self.buf.push_back(BytesMut::with_capacity(BLOCK_SIZE));
        }
        let buf = self.buf.back_mut().unwrap();
        let space_left = buf.capacity() - buf.len();
        let s = if length + 7 <= space_left {
            buf.put_u32_le(crc32(data));
            buf.put_u16_le(length as u16);
            buf.put_u8(if is_first {
                RECORD_TYPE_FULL
            } else {
                RECORD_TYPE_LAST
            });
            buf.put_slice(data);
            data.len()
        } else {
            let data = data[..(space_left - 7)].as_ref();
            buf.put_u32_le(crc32(data));
            buf.put_u16_le(data.len() as u16);
            buf.put_u8(if is_first {
                RECORD_TYPE_FIRST
            } else {
                RECORD_TYPE_MIDDLE
            });
            buf.put_slice(data);
            data.len()
        };
        let space_left = buf.capacity() - buf.len();
        if space_left <= 7 && space_left > 0 {
            buf.extend_from_slice((0..space_left).map(|_x| 0).collect::<Vec<u8>>().as_slice());
        }
        s
    }

    pub(crate) fn flush(&mut self) -> Result<(), LError> {
        self.f.flush()
    }
}

#[cfg(test)]
mod test {
    use crate::io::{MemFS, StorageSystem};
    use crate::wal::{WalReader, WalWriter};

    #[test]
    fn test_wal() {
        let fs = MemFS::default();
        let f = fs.create("test_wal").unwrap();
        let _buf: Vec<u8> = vec![];
        let mut ww = WalWriter::new(f);
        let lens = vec![
            2u64, 5, 10, 100, 1000, 3000, 5000, 10000, 30000, 50000, 100000,
        ];
        let logs = lens
            .into_iter()
            .map(|x| (0..x).map(|_| b'a').collect::<Vec<u8>>())
            .collect::<Vec<Vec<u8>>>();
        for log in logs.iter() {
            ww.append(log.as_ref()).unwrap();
        }
        let file = fs.open("test_wal").unwrap();
        let mut wr = WalReader::new(file, "".to_string()).unwrap();
        for log in logs.into_iter() {
            assert_eq!(log, wr.next().unwrap().unwrap())
        }
    }
}
