use arrow2::array::{Utf8Array, Array, Int64Array};
use arrow2::datatypes::{DataType, Schema};
use arrow2::record_batch::RecordBatch;
use indicatif::ProgressBar;
use once_cell::sync::Lazy;
use std::env;
use tiberius::{BulkLoadMetadata, Client, ColumnFlag, Config, IntoSql, TokenRow, TypeInfo, ColumnData, BulkLoadRequest};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use std::convert::TryFrom;


use std::sync::Arc;

use tokio::fs::File;
use tokio_util::compat::*;

use arrow2::io::csv::read_async::*;



static CONN_STR: Lazy<String> = Lazy::new(|| {
    env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or_else(|_| {
        "server=tcp:localhost,1433;IntegratedSecurity=true;TrustServerCertificate=true".to_owned()
    })
});

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    // let args: Vec<String> = env::args().collect();
    // let file_path = &args[1];
    // let file_path = "large-ex.csv";
    let file_path = "example.csv";
    let file = File::open(file_path).await?.compat();
    let mut reader = AsyncReaderBuilder::new().create_reader(file);
    let max_rows = Some(1000);  // avoid parsing the whole CSV twice.
    let schema = Arc::new(infer_schema(&mut reader, max_rows, true, &infer).await?);

    let meta = bulkload_metadata_from_arrow_schema(&schema);
    // println!("{}", meta.column_descriptions().collect::<Vec<String>>().join(", "));  // is not public and does not include `NOT NULL`

    let mut client = {
        let config = Config::from_ado_string(&CONN_STR)?;
        let tcp = TcpStream::connect(config.get_addr()).await?;
        tcp.set_nodelay(true)?;
        Client::connect(config, tcp.compat_write()).await?
    };
    client
        .execute(
            "CREATE TABLE ##bulk_test1 (foo int, content VARCHAR(255))",
            &[],
        )
        .await?;
    let mut req = client.bulk_insert("##bulk_test1", meta).await?;
    // let count = batch.num_rows(); //2000i32;
    // let pb = ProgressBar::new(count as u64);

    // for i in vec!["aaaaaaaaaaaaaaaaaaaa"; 1000].into_iter() {
    //     let mut row = TokenRow::new();
    //     row.push(i.into_sql());
    //     req.send(row).await?;
    //     pb.inc(1);
    // }

    const BATCH_ROWS: usize = 100;
    let mut rows = vec![ByteRecord::default(); BATCH_ROWS];
    loop {  // batches of BATCH_ROWS rows
        let rows_read = read_rows(&mut reader, 0, &mut rows).await?;
        if rows_read == 0 { break; }

        let batch = deserialize_batch(
            &rows[..rows_read],
            schema.fields(),
            None,
            0,
            deserialize_column,
        )?;

        send_batch(&mut req, &batch).await?;
    }

    // pb.finish_with_message("waiting...");

    let res = req.finalize().await?;
    dbg!(res);
    Ok(())
}


fn bulkload_metadata_from_arrow_schema(schema: &Schema) -> BulkLoadMetadata<'_>
{

    let mut meta = BulkLoadMetadata::new();
    for f in schema.fields() {
        let ty = match f.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => TypeInfo::nvarchar(tiberius::TypeLength::Limited(100)),
            DataType::Int64 => TypeInfo::int(),  //i32
            unexpected => unreachable!("Unexpected data type {:?}", unexpected)
        };
        dbg!(f.name(), &f.data_type(), &ty);
        meta.add_column(f.name(), ty, ColumnFlag::Nullable); //enumflags2::BitFlags::empty());
    }
    meta
}

async fn send_batch(req: &mut BulkLoadRequest<'_, Compat<TcpStream>>, batch: &RecordBatch) -> anyhow::Result<()> {
    let mut column_serializers : Vec<Box<dyn Iterator<Item = ColumnData<'static>> + '_>> = batch
        .columns()
        .iter()
        .map(|column| new_serializer(column.as_ref()))
        .collect();
                                        // println!("sizeof EE::A: {}", std::mem::size_of::<ColumnData>()); //48
    for _ in 0..batch.num_rows() {
        let mut row = TokenRow::new();  // shouldn't have to construct the intermediate Vec<ColumnData> for each row
        column_serializers
            .iter_mut()
            // Every column is supposed to yield the same number of elements
            // (`batch.num_rows()`), so unwrap() should always succeed:
            .for_each(|column_iter| row.push(column_iter.next().unwrap()));
        // dbg!(&row);
        req.send(row).await?;
        // pb.inc(1);  // doing this for every row is slow
    }
    Ok(())
}


pub fn new_serializer<'a>(
    array: &'a dyn Array
) -> Box<dyn Iterator<Item = ColumnData<'static>> + 'a> {
    match array.data_type() {
        DataType::Int64 => {
            let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Box::new(array.iter().map(|a| {
                let v: ColumnData<'static> = i32::try_from(*a.unwrap()).unwrap().into_sql();
                v
            }))
        }
        DataType::Utf8 => {
            let array = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            Box::new(array.iter().map(|a| {
                let v: ColumnData<'static> = a.unwrap().to_string().into_sql();  // XXX to_string() to avoid holding up `batch` for 'static
                v
            }))
        }
        _ => unreachable!()
    }
}
