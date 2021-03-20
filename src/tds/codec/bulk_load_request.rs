use super::{
    BaseMetaDataColumn, ColumnData, ColumnFlag, Encode, FixedLenType, MetaDataColumn,
    TokenColMetaData, TokenDone, TokenRow, TypeInfo,
};
use crate::Result;
use bytes::BytesMut;
use enumflags2::BitFlags;

// TODO: everything is currently hardcoded
pub struct BulkLoadRequest();

impl BulkLoadRequest {
    pub fn new() -> Self {
        Self()
    }
}

impl Encode<BytesMut> for BulkLoadRequest {
    fn encode(self, dst: &mut BytesMut) -> Result<()> {
        // COLMETADATA
        let base = BaseMetaDataColumn {
            flags: BitFlags::<ColumnFlag>::empty(),
            ty: TypeInfo::FixedLen(FixedLenType::Int1),
        };
        let mdc = MetaDataColumn {
            base,
            col_name: "content".into(),
        };
        let cmd = TokenColMetaData { columns: vec![mdc] };
        cmd.encode(dst)?;

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
