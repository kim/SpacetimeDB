use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

pub use zerocopy::little_endian::{U16, U32, U64};

#[repr(C)]
#[derive(Debug, PartialEq, FromBytes, IntoBytes, KnownLayout, Immutable)]
pub struct U256([u8; 32]);

const _: () = assert!(size_of::<U256>() == 32);

impl From<ethnum::U256> for U256 {
    fn from(value: ethnum::U256) -> Self {
        Self(value.to_le_bytes())
    }
}

impl From<U256> for ethnum::U256 {
    fn from(value: U256) -> Self {
        Self::from_le_bytes(value.0)
    }
}
