use std::{num::NonZeroUsize, path::PathBuf};

use crate::{
    erasure_code::{
        ErasureCode, HitchhikerCode, HitchhikerXor, PartialStripe, ReedSolomon, Stripe,
    },
    SUResult,
};

#[derive(Debug, Default)]
pub struct HitchhikerBench {
    block_size: Option<usize>,
    block_num: Option<usize>,
    dev_path: Option<Vec<PathBuf>>,
    k_p: Option<(usize, usize)>,
    test_num: Option<usize>,
    out_dir_path: Option<PathBuf>,
}

impl HitchhikerBench {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn block_size(&mut self, block_size: usize) -> &mut Self {
        self.block_size = Some(block_size);
        self
    }

    pub fn block_num(&mut self, block_num: usize) -> &mut Self {
        self.block_num = Some(block_num);
        self
    }

    pub fn dev_path(&mut self, dev_path: impl Into<Vec<PathBuf>>) -> &mut Self {
        self.dev_path = Some(dev_path.into());
        self
    }

    pub fn k_p(&mut self, k: usize, p: usize) -> &mut Self {
        self.k_p = Some((k, p));
        self
    }

    pub fn test_load(&mut self, num: usize) -> &mut Self {
        self.test_num = Some(num);
        self
    }

    pub fn out_dir_path(&mut self, out_dir_path: impl Into<PathBuf>) -> &mut Self {
        self.out_dir_path = Some(out_dir_path.into());
        self
    }

    pub fn run(&self) -> SUResult<()> {
        let mut total_hh_encode_time = std::time::Duration::ZERO;
        let mut total_hh_repair_time = std::time::Duration::ZERO;
        for _ in 0..self.test_num.unwrap() {
            let (encode_time, repair_time) = self.do_hh_test()?;
            total_hh_encode_time += encode_time;
            total_hh_repair_time += repair_time;
        }
        let mut total_rs_encode_time = std::time::Duration::ZERO;
        let mut total_rs_repair_time = std::time::Duration::ZERO;
        for _ in 0..self.test_num.unwrap() {
            let (encode_time, repair_time) = self.do_rs_test()?;
            total_rs_encode_time += encode_time;
            total_rs_repair_time += repair_time;
        }
        println!("test load: {}", self.test_num.unwrap());
        fn display_duration(duration: std::time::Duration) -> String {
            let sec = duration.as_secs();
            let ms = u64::try_from(duration.as_millis()).unwrap() - sec * 1000;
            format!("{sec}s {ms}ms")
        }
        println!(
            "hitchhiker encode time: {}",
            display_duration(total_hh_encode_time)
        );
        println!(
            "hitchhiker repair time: {}",
            display_duration(total_hh_repair_time)
        );
        println!("rs encode time: {}", display_duration(total_rs_encode_time));
        println!("rs repair time: {}", display_duration(total_rs_repair_time));
        Ok(())
    }

    pub fn do_hh_test(&self) -> SUResult<(std::time::Duration, std::time::Duration)> {
        let (k, p) = self.k_p.unwrap();
        let xor_code = HitchhikerXor::try_from_k_p(
            NonZeroUsize::new(k).unwrap(),
            NonZeroUsize::new(p).unwrap(),
        )?;
        let block_size = self.block_size.unwrap() / 2;
        let random_stripe = || {
            use rand::Rng;
            let mut stripe = Stripe::zero(
                NonZeroUsize::new(k).unwrap(),
                NonZeroUsize::new(p).unwrap(),
                NonZeroUsize::new(block_size).unwrap(),
            );
            stripe.iter_mut_source().for_each(|source_block| {
                source_block
                    .iter_mut()
                    .for_each(|b| *b = rand::thread_rng().gen())
            });
            stripe
        };
        let epoch = std::time::Instant::now();
        // encode
        let mut sub_stripes = (0..2).map(|_| random_stripe()).collect::<Vec<_>>();
        xor_code.encode_stripe(&mut sub_stripes)?;
        let encode_time = epoch.elapsed();
        // corrupt a block
        let mut partial_stripe = sub_stripes
            .into_iter()
            .map(|s| {
                let mut s = PartialStripe::from(s);
                let _ = s.replace_block(0, None);
                s
            })
            .collect::<Vec<_>>();
        // repair
        let epoch = std::time::Instant::now();
        xor_code.repair(&mut partial_stripe)?;
        let repair_time = epoch.elapsed();
        Ok((encode_time, repair_time))
    }

    fn do_rs_test(&self) -> SUResult<(std::time::Duration, std::time::Duration)> {
        let (k, p) = self.k_p.unwrap();
        let rs_code =
            ReedSolomon::from_k_p(NonZeroUsize::new(k).unwrap(), NonZeroUsize::new(p).unwrap());
        let block_size = self.block_size.unwrap();
        let random_stripe = || {
            use rand::Rng;
            let mut stripe = Stripe::zero(
                NonZeroUsize::new(k).unwrap(),
                NonZeroUsize::new(p).unwrap(),
                NonZeroUsize::new(block_size).unwrap(),
            );
            stripe.iter_mut_source().for_each(|source_block| {
                source_block
                    .iter_mut()
                    .for_each(|b| *b = rand::thread_rng().gen())
            });
            stripe
        };
        let epoch = std::time::Instant::now();
        // encode
        let mut stripe = random_stripe();
        rs_code.encode_stripe(&mut stripe)?;
        let encode_time = epoch.elapsed();
        // corrupt a block
        let mut partial_stripe = {
            let mut s = PartialStripe::from(stripe);
            let _ = s.replace_block(0, None);
            s
        };
        // repair
        let epoch = std::time::Instant::now();
        rs_code.decode(&mut partial_stripe)?;
        let repair_time = epoch.elapsed();
        Ok((encode_time, repair_time))
    }
}
