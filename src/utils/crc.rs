use crc::{Crc, CRC_32_ISCSI};
pub const CASTAGNOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

pub fn crc32(data: &[u8]) -> u32 {
    CASTAGNOLI.checksum(data)
}
