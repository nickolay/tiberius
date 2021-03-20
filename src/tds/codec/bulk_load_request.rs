use super::{
    BaseMetaDataColumn, ColumnData, Encode, MetaDataColumn, TokenColMetaData, TokenDone, TokenRow,
};
pub use super::{ColumnFlag, FixedLenType, TypeInfo};
use crate::Result;
use bytes::BytesMut;
pub use enumflags2::BitFlags;

/// COLMETADATA
#[derive(Debug, Clone)]
pub struct BulkLoadMetadata(Vec<MetaDataColumn>);

impl<'a> BulkLoadMetadata {
    pub fn new() -> Self {
        Self(vec![])
    }
    pub fn add_column(&mut self, name: &str, ty: TypeInfo, flags: BitFlags<ColumnFlag>) {
        self.0.push(MetaDataColumn {
            base: BaseMetaDataColumn { flags, ty },
            col_name: name.to_owned(),
        });
    }
    pub fn get_insert_bulk_cols(&self) -> String {
        "content tinyint".to_owned() // TODO hardcoded
    }
}

impl Encode<BytesMut> for BulkLoadMetadata {
    fn encode(self, dst: &mut BytesMut) -> Result<()> {
        let cmd = TokenColMetaData { columns: self.0 };
        cmd.encode(dst)?;
        Ok(())
    }
}

// TODO: the data is currently hardcoded
pub struct BulkLoadRequest {
    col_metadata: BulkLoadMetadata,
}

impl BulkLoadRequest {
    pub fn new(col_metadata: &BulkLoadMetadata) -> Self {
        Self {
            col_metadata: col_metadata.clone(),
        }
    }
}

impl Encode<BytesMut> for BulkLoadRequest {
    fn encode(self, dst: &mut BytesMut) -> Result<()> {
        // COLMETADATA
        self.col_metadata.encode(dst)?;

        // ROW
        let row = TokenRow {
            data: vec![ColumnData::U8(Some(7))],
        };
        row.encode(dst)?;

        // ROW
        let row = TokenRow {
            data: vec![ColumnData::U8(Some(19))],
        };
        row.encode(dst)?;

        // DONE
        TokenDone::default().encode(dst)?;

        Ok(())
    }
}
