use crate::compare::{Comparator, ComparatorImpl};
use crate::key::InternalKeyKind::{Delete, Set};
use crate::LError;
use bytes::{BufMut, Bytes, BytesMut};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};

// TODO: need to refactory

#[derive(Copy, Clone, Debug)]
pub(crate) struct InternalKeyRef<'a> {
    pub ukey: &'a [u8],
    pub k: InternalKeyKind,
    pub seq_num: u64,
}

impl<'a, 'b: 'a> From<(&'b [u8], u64)> for InternalKeyRef<'a> {
    fn from(value: (&'b [u8], u64)) -> Self {
        InternalKeyRef {
            ukey: value.0[..].as_ref(),
            seq_num: value.1,
            k: Set,
        }
    }
}

impl<'a> From<&'a [u8]> for InternalKeyRef<'a> {
    fn from(value: &'a [u8]) -> Self {
        let l = value.len();
        let sk = u64::from_le_bytes(value[l - 8..].to_vec().try_into().unwrap());
        let kind = sk & ((1 << 8) - 1);
        Self {
            ukey: value[..(l - 8)].as_ref(),
            k: if kind == 0 { Delete } else { Set },
            seq_num: sk >> 8,
        }
    }
}

impl InternalKey {
    #[allow(unused)]
    pub(crate) fn borrow(&self) -> InternalKeyRef {
        InternalKeyRef {
            ukey: self.ukey().as_ref(),
            seq_num: self.seq_num(),
            k: self.kind(),
        }
    }
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq)]
pub struct InternalKey(Bytes);

impl Display for InternalKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let uk = match String::from_utf8(self.ukey().to_vec()) {
            Ok(u) => u,
            Err(_) => format!("{:?}", self.ukey()),
        };
        f.write_fmt(format_args!(
            "{}-{}-{}",
            uk,
            self.seq_num(),
            self.kind() as u8
        ))
    }
}

impl TryFrom<Bytes> for InternalKey {
    type Error = LError;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        let l = value.len();
        let sk = u64::from_le_bytes(value[l - 8..].to_vec().try_into().unwrap());
        let kind = sk & ((1 << 8) - 1);
        if kind > 1 || sk > (1u64 << 56 - 1) {
            return Err(LError::InvalidInternalKey(value.to_vec()));
        }
        Ok(InternalKey(value))
    }
}

impl Into<Bytes> for InternalKey {
    fn into(self) -> Bytes {
        self.0
    }
}

impl<'a> InternalKeyRef<'a> {
    pub(crate) fn to_owned(&self) -> InternalKey {
        let mut ik = BytesMut::from(self.ukey);
        ik.put_u64_le((self.seq_num << 8) | (self.k as u64));
        InternalKey(ik.freeze())
    }
}

impl AsRef<[u8]> for InternalKey {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl InternalKey {
    pub(crate) fn ukey(&self) -> &[u8] {
        self.as_ref()[..self.0.len() - 8].as_ref()
    }

    pub(crate) fn kind(&self) -> InternalKeyKind {
        let sk = u64::from_le_bytes(
            self.0.as_ref()[self.0.len() - 8..]
                .to_vec()
                .try_into()
                .unwrap(),
        );
        match sk & ((1 << 8) - 1) {
            0 => Delete,
            1 => Set,
            _ => panic!("invalid internal key kind"),
        }
    }

    pub(crate) fn is_set(&self) -> bool {
        self.kind() == Set
    }

    pub(crate) fn seq_num(&self) -> u64 {
        let sk = u64::from_le_bytes(
            self.0.as_ref()[self.0.len() - 8..]
                .to_vec()
                .try_into()
                .unwrap(),
        );
        sk >> 8
    }

    #[allow(unused)]
    pub(crate) fn borrow_inner(&self) -> &Bytes {
        &self.0
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum InternalKeyKind {
    Delete = 0,
    Set = 1,
}

impl Default for InternalKeyKind {
    fn default() -> Self {
        Set
    }
}

#[derive(Copy, Clone, Debug)]
pub struct InternalKeyComparator {
    u: ComparatorImpl,
}

impl Comparator for InternalKeyComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        if a.len() < 8 || b.len() < 8 {
            panic!("invalid internal keys for comparing")
        }
        let ikra = InternalKeyRef::from(a);
        let ikrb = InternalKeyRef::from(b);
        self.compare_keyref(&ikra, &ikrb)
    }

    fn name(&self) -> &'static str {
        "leveldb.InternalKeyComparator"
    }
}

impl From<ComparatorImpl> for InternalKeyComparator {
    fn from(u: ComparatorImpl) -> Self {
        Self { u }
    }
}

impl InternalKeyComparator {
    pub(crate) fn compare_keyref(&self, a: &InternalKeyRef, b: &InternalKeyRef) -> Ordering {
        match self.u.compare(a.ukey, b.ukey) {
            Ordering::Equal => match a.seq_num.cmp(&b.seq_num) {
                Ordering::Equal => (a.k as u8).cmp(&(b.k as u8)),
                // an internal key A is greater than another one B if they have the same user's part
                // while A's sequence number is less than that of B
                Ordering::Greater => Ordering::Less,
                Ordering::Less => Ordering::Greater,
            },
            o => o,
        }
    }

    #[allow(unused)]
    fn name(&self) -> &'static str {
        "leveldb.InternalKeyComparator"
    }
}

#[cfg(test)]
mod test {
    use crate::compare::BYTEWISE_COMPARATOR;
    use crate::key::{InternalKeyComparator, InternalKeyKind, InternalKeyRef};
    use bytes::Bytes;
    use std::cmp::Ordering;

    #[test]
    fn test_internal_key() {
        let key_nums = vec![
            ("a", 2u64),
            ("a", 1),
            ("aa", 3),
            ("ab", 4),
            ("long", 1 << 24),
            ("long", 1 << 8),
        ]
        .iter()
        .map(|(k, n)| (Bytes::from(*k), *n))
        .collect::<Vec<(Bytes, u64)>>();
        let ikeys = key_nums
            .iter()
            .map(|(k, n)| InternalKeyRef::from((k.as_ref(), *n)))
            .collect::<Vec<InternalKeyRef>>();
        let ic = InternalKeyComparator::from(BYTEWISE_COMPARATOR);
        for i in 0..key_nums.len() {
            assert_eq!(key_nums[i].0.as_ref(), ikeys[i].ukey);
            assert_eq!(key_nums[i].1, ikeys[i].seq_num);
            if i > 0 {
                assert_eq!(
                    ic.compare_keyref(&ikeys[i], &ikeys[i - 1]),
                    Ordering::Greater
                );
            }
        }
    }

    #[test]
    fn test_internal_key_ref() {
        let keys = vec![
            (b"a", 1u64, InternalKeyKind::Set),
            (b"b", 2, InternalKeyKind::Delete),
            (b"b", 3, InternalKeyKind::Set),
        ];
        let mut refs = vec![];
        for (d, n, k) in keys.iter() {
            let mut r = InternalKeyRef::from((d.as_ref(), *n));
            r.k = *k;
            refs.push(r);
        }
        for (i, r) in refs.iter().enumerate() {
            let ik = r.to_owned();
            assert_eq!(ik.ukey(), keys[i].0.as_ref());
            assert_eq!(ik.seq_num(), keys[i].1);
            assert_eq!(ik.kind(), keys[i].2);
        }
    }
}
