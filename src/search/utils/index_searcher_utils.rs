use crate::common::errors::IndexSearcherError;
use roaring::RoaringBitmap;
use std::sync::Arc;

pub struct FFiIndexSearcherUtils;

impl FFiIndexSearcherUtils {
    pub fn intersect_with_range(
        rowid_bitmap: Arc<RoaringBitmap>,
        lrange: u64,
        rrange: u64,
    ) -> Result<Arc<RoaringBitmap>, IndexSearcherError> {
        let lrange_u32: u32 = lrange
            .try_into()
            .map_err(|_| IndexSearcherError::BitmapOverflowError("`lrange > u32`".to_string()))?;
        let rrange_u32: u32 = rrange
            .try_into()
            .map_err(|_| IndexSearcherError::BitmapOverflowError("`rrange > u32`".to_string()))?;
        let rrange_plus_one_u32 =
            rrange_u32
                .checked_add(1)
                .ok_or(IndexSearcherError::BitmapOverflowError(
                    "`rrange+1` > `u32`".to_string(),
                ))?;

        let mut row_id_range = RoaringBitmap::new();
        row_id_range.insert_range(lrange_u32..rrange_plus_one_u32);
        row_id_range &= Arc::as_ref(&rowid_bitmap);
        Ok(Arc::new(row_id_range))
    }
}
