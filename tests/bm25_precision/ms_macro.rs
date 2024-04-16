use hdf5::File;

// TODO simulate for MyScale multi parts.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file = File::open("/mnt/860evo/vector-db-benchmark/datasets/downloaded/ms-macro2-768-full-cosine/ms-macro2-768-full-cosine.hdf5")?;

    let dataset = file.dataset("dataset_name")?;

    let data: Vec<f32> = dataset.read_raw()?;

    println!("Data: {:?}", data);

    Ok(())
}