use crate::SUResult;

struct BenchUpdate {}

impl super::CoordinatorCmds for BenchUpdate {
    fn exec(self: Box<Self>) -> SUResult<()> {
        // const CH_SIZE: usize = 32;
        // let request_list = self.config.request_queue_list.clone();
        // let worker_id_range = 1..request_list.len() + 1;
        // let response_list = self.config.response_queue.clone();
        // let block_size = self.config.block_size;
        // let mut block_num = self.config.block_num;
        // let (k, p) = self.config.k_p;
        // let n = k + p;
        // let stripe_num = block_num.div_ceil(n);
        // if block_num % n != 0 {
        //     println!("ec-n [{n}] cannot divide block num [{block_num}], round up stripe number to {stripe_num}");
        //     block_num = stripe_num * n;
        // }
        // print configuration
        // println!(
        //     "block size: {block_size}
        //     block num: {block_num}
        //     worker num: {}
        //     k: {k}
        //     p: {p}
        //     stripe num: {stripe_num}",
        //     worker_id_range.len()
        // );

        todo!()
    }
}
