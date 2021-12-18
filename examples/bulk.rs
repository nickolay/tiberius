use arrow2::array::{Utf8Array, Array, Int32Array, Int64Array};
use arrow2::datatypes::DataType;
use arrow2::record_batch::RecordBatch;
use enumflags2::BitFlags;
use indicatif::ProgressBar;
use once_cell::sync::Lazy;
use std::env;
use tiberius::{BulkLoadMetadata, Client, ColumnFlag, Config, IntoSql, TokenRow, TypeInfo};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

static CONN_STR: Lazy<String> = Lazy::new(|| {
    env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or_else(|_| {
        "server=tcp:localhost,1433;IntegratedSecurity=true;TrustServerCertificate=true".to_owned()
    })
});

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let config = Config::from_ado_string(&CONN_STR)?;

    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    let mut client = Client::connect(config, tcp.compat_write()).await?;

    client
        .execute(
            "CREATE TABLE ##bulk_test1 (id INT IDENTITY PRIMARY KEY, foo int null, content VARCHAR(255) null)",
            &[],
        )
        .await?;

    let batch = read_csv().await?;

    let mut meta = BulkLoadMetadata::new();
    // meta.add_column("content", TypeInfo::nvarchar(tiberius::TypeLength::Limited(500)), ColumnFlag::Nullable);
    for f in batch.schema().fields() {
        let ty = match f.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => TypeInfo::nvarchar(tiberius::TypeLength::Limited(100)),
            DataType::Int64 => TypeInfo::int(),  //i32
            unexpected => unreachable!("Unexpected data type {:?}", unexpected)
        };
        dbg!(f.name(), &f.data_type(), &ty);
        meta.add_column(f.name(), ty, ColumnFlag::Nullable);
    }

    let mut req = client.bulk_insert("##bulk_test1", meta).await?;
    let count = batch.num_rows(); //2000i32;
    let pb = ProgressBar::new(count as u64);

    // for i in vec!["aaaaaaaaaaaaaaaaaaaa"; 1000].into_iter() {
    //     let mut row = TokenRow::new();
    //     row.push(i.into_sql());
    //     req.send(row).await?;
    //     pb.inc(1);
    // }

    for (a,b) in batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().into_iter().zip(
                    batch.column(1).as_any().downcast_ref::<Utf8Array<i32>>().unwrap().into_iter()
                ) {
        let mut row = TokenRow::new();
        use std::convert::TryFrom;
        row.push(i32::try_from(*a.unwrap()).unwrap().into_sql());
        row.push(b.unwrap().to_string().into_sql());  // XXX to_string() to avoid holding up `batch` for 'static
        dbg!(&row);
        req.send(row).await?;
        pb.inc(1);
    }

    pb.finish_with_message("waiting...");

    let res = req.finalize().await?;
    dbg!(res);
    Ok(())
}

use std::sync::Arc;

use tokio::fs::File;
use tokio_util::compat::*;

use arrow2::io::csv::read_async::*;

// #[tokio::main(flavor = "current_thread")]
// async fn main() -> anyhow::Result<()> {

// }


async fn read_csv() -> anyhow::Result<RecordBatch> {
    let args: Vec<String> = env::args().collect();

    let file_path = "example.csv"; //&args[1];

    let file = File::open(file_path).await?.compat();

    let mut reader = AsyncReaderBuilder::new().create_reader(file);

    let schema = Arc::new(infer_schema(&mut reader, None, true, &infer).await?);

    let mut rows = vec![ByteRecord::default(); 100];
    let rows_read = read_rows(&mut reader, 0, &mut rows).await?;

    let batch = deserialize_batch(
        &rows[..rows_read],
        schema.fields(),
        None,
        0,
        deserialize_column,
    )?;
    // println!("{}", batch.column(0));
    // Ok(())
    Ok(batch)
}


// /// Creates serializers that iterate over each column of `batch` and serialize each item according
// /// to `options`.
// fn new_serializers<'a>(
//     batch: &'a RecordBatch,
//     options: &'a SerializeOptions,
// ) -> Result<Vec<Box<dyn StreamingIterator<Item = [u8]> + 'a>>> {
//     batch
//         .columns()
//         .iter()
//         .map(|column| new_serializer(column.as_ref(), options))
//         .collect()
// }

// /// Serializes a [`RecordBatch`] as vector of `ByteRecord`.
// /// The vector is guaranteed to have `batch.num_rows()` entries.
// /// Each `ByteRecord` is guaranteed to have `batch.num_columns()` fields.
// pub fn serialize(batch: &RecordBatch, options: &SerializeOptions) -> Result<Vec<ByteRecord>> {
//     let mut serializers = new_serializers(batch, options)?;

//     let mut records = vec![ByteRecord::with_capacity(0, batch.num_columns()); batch.num_rows()];
//     records.iter_mut().for_each(|record| {
//         serializers
//             .iter_mut()
//             // `unwrap` is infalible because `array.len()` equals `num_rows` on a `RecordBatch`
//             .for_each(|iter| record.push_field(iter.next().unwrap()));
//     });
//     Ok(records)
// }

// /// Writes the data in a `RecordBatch` to `writer` according to the serialization options `options`.
// pub fn write_batch<W: Write>(
//     writer: &mut Writer<W>,
//     batch: &RecordBatch,
//     options: &SerializeOptions,
// ) -> Result<()> {
//     let mut serializers = new_serializers(batch, options)?;

//     let mut record = ByteRecord::with_capacity(0, batch.num_columns());

//     // this is where the (expensive) transposition happens: the outer loop is on rows, the inner on columns
//     (0..batch.num_rows()).try_for_each(|_| {
//         serializers
//             .iter_mut()
//             // `unwrap` is infalible because `array.len()` equals `num_rows` on a `RecordBatch`
//             .for_each(|iter| record.push_field(iter.next().unwrap()));
//         writer.write_byte_record(&record)?;
//         record.clear();
//         Result::Ok(())
//     })?;
//     Ok(())
// }