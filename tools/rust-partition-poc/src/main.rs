use hugegraph_partition_poc::{validate, Partition};
fn main() {
    let baseline = vec![Partition{start:0,end:10,version:1}, Partition{start:10,end:20,version:1}];
    validate(&baseline, 20).expect("baseline oracle");
    println!("PASS partition invariant oracle: {:?}", baseline);
}
