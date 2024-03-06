use std::{
    io::{Read, Write},
    num::NonZeroUsize,
    path::PathBuf,
};

use bytes::BytesMut;

use crate::{
    erasure_code::{
        Block, ErasureCode, HitchhikerCode, HitchhikerXor, PartialStripe, ReedSolomon, Stripe,
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

    fn warming_up(&self) -> SUResult<()> {
        let dev_map = self.dev_path.as_ref().unwrap();
        dev_map.iter().for_each(|dev| {
            std::fs::remove_dir_all(dev).unwrap();
            std::fs::create_dir_all(format!("{}/a-hh", dev.display())).unwrap();
            std::fs::create_dir_all(format!("{}/b-hh", dev.display())).unwrap();
            std::fs::create_dir_all(format!("{}/rs", dev.display())).unwrap();
        });
        Ok(())
    }

    pub fn run(&self) -> SUResult<()> {
        // prepare data
        self.warming_up()?;
        // test hh
        self.do_hh_test()?;
        // test rs
        self.do_rs_test()?;
        Ok(())
    }

    pub fn do_hh_test(&self) -> SUResult<()> {
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
        let test_load = self.test_num.unwrap();
        let dev_map = self.dev_path.as_ref().unwrap();
        assert_eq!(dev_map.len(), k + p);
        let mut encode_time = std::time::Duration::ZERO;
        let mut repair_time = std::time::Duration::ZERO;
        let mut read_time = std::time::Duration::ZERO;

        // encode
        for test_n in 0..test_load {
            let epoch = std::time::Instant::now();
            let mut sub_stripes = (0..2).map(|_| random_stripe()).collect::<Vec<_>>();
            xor_code.encode_stripe(&mut sub_stripes)?;
            encode_time += epoch.elapsed();
            let (a, b) = sub_stripes.split_first().unwrap();
            let (b, _) = b.split_first().unwrap();
            a.iter_source()
                .chain(a.iter_parity())
                .enumerate()
                .for_each(|(i, blk)| {
                    let mut f = std::fs::OpenOptions::new()
                        .create_new(true)
                        .write(true)
                        .open(format!("{}/a-hh/{}", dev_map[i].display(), test_n))
                        .unwrap();
                    f.write(blk).unwrap();
                });
            b.iter_source()
                .chain(b.iter_parity())
                .enumerate()
                .for_each(|(i, blk)| {
                    let mut f = std::fs::OpenOptions::new()
                        .create_new(true)
                        .write(true)
                        .open(format!("{}/b-hh/{}", dev_map[i].display(), test_n))
                        .unwrap();
                    f.write(blk).unwrap();
                });
        }

        // repair
        for test_n in 0..test_load {
            // read from disk
            let epoch = std::time::Instant::now();
            let a = (0..k + p)
                .map(|i| {
                    std::path::PathBuf::from(format!("{}/a-hh/{test_n}", dev_map[i].display()))
                })
                .map(|f| std::fs::File::open(f).unwrap())
                .map(|mut f| {
                    let mut v = BytesMut::zeroed(block_size);
                    f.read_exact(&mut v).unwrap();
                    v
                })
                .map(Block::from)
                .collect::<Vec<_>>();
            let a = PartialStripe::from(Stripe::from_vec(
                a,
                NonZeroUsize::new(k).unwrap(),
                NonZeroUsize::new(p).unwrap(),
            ));
            let b = (0..k + p)
                .map(|i| {
                    std::path::PathBuf::from(format!("{}/b-hh/{test_n}", dev_map[i].display()))
                })
                .map(|f| std::fs::File::open(f).unwrap())
                .map(|mut f| {
                    let mut v = BytesMut::zeroed(block_size);
                    f.read_exact(&mut v).unwrap();
                    v
                })
                .map(Block::from)
                .collect::<Vec<_>>();
            let b = PartialStripe::from(Stripe::from_vec(
                b,
                NonZeroUsize::new(k).unwrap(),
                NonZeroUsize::new(p).unwrap(),
            ));
            read_time += epoch.elapsed();
            // corrupt a block
            let sub_stripes = vec![a, b];
            let mut partial_stripe = sub_stripes
                .into_iter()
                .map(|s| {
                    let mut s = PartialStripe::from(s);
                    let _ = s.replace_block(0, None);
                    s
                })
                .collect::<Vec<_>>();
            let epoch = std::time::Instant::now();
            xor_code.repair(&mut partial_stripe)?;
            repair_time += epoch.elapsed();
        }
        println!("hitchhiker encode time: {}", display_duration(encode_time));
        println!("hitchhiker read time: {}", display_duration(read_time));
        println!("hitchhiker repair time: {}", display_duration(repair_time));
        Ok(())
    }

    fn do_rs_test(&self) -> SUResult<()> {
        let (k, p) = self.k_p.unwrap();
        let rs_code =
            ReedSolomon::from_k_p(NonZeroUsize::new(k).unwrap(), NonZeroUsize::new(p).unwrap());
        let block_size = self.block_size.unwrap();
        let test_load = self.test_num.unwrap();
        let dev_map = self.dev_path.as_ref().unwrap();
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
        let mut encode_time = std::time::Duration::ZERO;
        let mut read_time = std::time::Duration::ZERO;
        let mut repair_time = std::time::Duration::ZERO;
        // encode
        for test_n in 0..test_load {
            let epoch = std::time::Instant::now();
            let mut stripe = random_stripe();
            rs_code.encode_stripe(&mut stripe)?;
            encode_time += epoch.elapsed();
            stripe
                .iter_source()
                .chain(stripe.iter_parity())
                .enumerate()
                .for_each(|(i, blk)| {
                    let mut f = std::fs::OpenOptions::new()
                        .create_new(true)
                        .write(true)
                        .open(format!("{}/rs/{}", dev_map[i].display(), test_n))
                        .unwrap();
                    f.write(blk).unwrap();
                });
        }
        // repair
        for test_n in 0..test_load {
            let epoch = std::time::Instant::now();
            let survivors_range = 1..k;
            let survivors = survivors_range
                .clone()
                .map(|i| std::path::PathBuf::from(format!("{}/rs/{test_n}", dev_map[i].display())))
                .map(|f| std::fs::File::open(f).unwrap())
                .map(|mut f| {
                    let mut v = BytesMut::zeroed(block_size);
                    f.read_exact(&mut v).unwrap();
                    v
                })
                .map(Block::from)
                .collect::<Vec<_>>();
            let mut s = PartialStripe::make_absent_from_k_p(
                NonZeroUsize::new(k).unwrap(),
                NonZeroUsize::new(p).unwrap(),
                NonZeroUsize::new(block_size).unwrap(),
            );
            survivors_range.zip(survivors).for_each(|(i, blk)| {
                let _ = s.replace_block(i, Some(blk));
            });
            read_time += epoch.elapsed();
            let _ = s.replace_block(0, None);
            let epoch = std::time::Instant::now();
            rs_code.decode(&mut s)?;
            repair_time += epoch.elapsed();
        }
        println!("rs encode time: {}", display_duration(encode_time));
        println!("rs read time: {}", display_duration(read_time));
        println!("rs repair time: {}", display_duration(repair_time));
        Ok(())
    }
}

fn display_duration(duration: std::time::Duration) -> String {
    let sec = duration.as_secs();
    let ms = u64::try_from(duration.as_millis()).unwrap() - sec * 1000;
    format!("{sec}s {ms}ms")
}
