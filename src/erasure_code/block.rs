use bytes::BytesMut;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block(BytesMut);

impl Block {
    pub fn zero(block_size: usize) -> Self {
        Self(BytesMut::zeroed(block_size))
    }

    pub fn zero_n(n: usize, block_size: usize) -> Vec<Self> {
        let mut buf = BytesMut::zeroed(n * block_size);
        (0..n)
            .map(|_| Self::split_from_buf(&mut buf, block_size))
            .collect()
    }

    /// Create a block splitted from a [`BytesMut`] buffer.
    ///
    /// # Panics
    /// - If `block_size > buf.len()`
    pub fn split_from_buf(buf: &mut BytesMut, block_size: usize) -> Self {
        Self(buf.split_to(block_size))
    }

    pub fn block_size(&self) -> usize {
        self.0.len()
    }
}

impl From<BytesMut> for Block {
    fn from(value: BytesMut) -> Self {
        Self(value)
    }
}

impl AsRef<[u8]> for Block {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl AsMut<[u8]> for Block {
    fn as_mut(&mut self) -> &mut [u8] {
        self.0.as_mut()
    }
}

impl std::ops::Deref for Block {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl std::ops::DerefMut for Block {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.deref_mut()
    }
}
