use core::fmt;

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// Either an AEGIS128L MAC, or a BLAKE3 hash.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, FromBytes, IntoBytes, KnownLayout, Immutable)]
pub struct Checksum([u8; 32]);
const _: () = assert!(size_of::<Checksum>() == 32);

impl Checksum {
    pub const ZERO: Self = Self([0; 32]);
    const LEN: usize = size_of::<Self>();

    pub fn from_bytes(b: &[u8]) -> Self {
        Self(*blake3::hash(b).as_bytes())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn incremental() -> Incremental {
        Incremental(blake3::Hasher::new())
    }

    pub fn to_hex<'a>(&self, buf: &'a mut [u8; 2 * Self::LEN]) -> &'a str {
        const TABLE: &[u8] = b"0123456789abcdef";
        let mut i = 0;
        for &b in self.0.iter() {
            buf[i] = TABLE[(b >> 4) as usize];
            i += 1;
            buf[i] = TABLE[(b & 0xf) as usize];
            i += 1;
        }
        unsafe { str::from_utf8_unchecked(&buf[..]) }
    }
}

impl fmt::Display for Checksum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.to_hex(&mut [0u8; 2 * Self::LEN]))
    }
}

pub struct Incremental(blake3::Hasher);

impl Incremental {
    pub fn update(&mut self, b: &[u8]) {
        self.0.update(b);
    }

    pub fn finalize(self) -> Checksum {
        Checksum(self.0.finalize().into())
    }
}
