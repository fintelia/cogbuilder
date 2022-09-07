use std::io::{self, Read, Seek, SeekFrom, Write};

pub const TILE_SIZE: u32 = 1024;

const NUM_TAGS: u32 = 11;
const OFFSETS_TAG_INDEX: u64 = 8;
const LENGTHS_TAG_INDEX: u64 = 9;

pub fn compress_tile(data: &[u8]) -> Vec<u8> {
    weezl::encode::Encoder::with_tiff_size_switch(weezl::BitOrder::Msb, 8)
        .encode(data)
        .unwrap()
}

pub struct CogBuilder<F> {
    file: F,
    widths: Vec<u32>,
    heights: Vec<u32>,
    tile_counts: Vec<u32>,
    file_size: u64,
}

impl<F: Read + Write + Seek> CogBuilder<F> {
    pub fn new(
        mut file: F,
        width: u32,
        height: u32,
        bpp: Vec<u8>,
        signed: bool,
        _nodata: &str,
    ) -> Result<Self, io::Error> {
        let mut file_size = file.seek(SeekFrom::End(0))?;

        let mut widths = Vec::new();
        let mut heights = Vec::new();
        let mut tile_counts = Vec::new();
        let mut overview_width = width;
        let mut overview_height = height;
        loop {
            widths.push(overview_width);
            heights.push(overview_height);

            let tiles_width = (overview_width + TILE_SIZE - 1) / TILE_SIZE;
            let tiles_height = (overview_height + TILE_SIZE - 1) / TILE_SIZE;
            tile_counts.push(tiles_width.checked_mul(tiles_height).unwrap());

            if overview_width <= TILE_SIZE && overview_height <= TILE_SIZE {
                break;
            }
            overview_width = (overview_width + 1) / 2;
            overview_height = (overview_height + 1) / 2;
        }

        let total_tiles = tile_counts.iter().map(|&c| c as u64).sum::<u64>();
        if file_size < 1024 * tile_counts.len() as u64 + 16 * total_tiles {
            file.seek(SeekFrom::Start(0))?;

            let mut data = Vec::new();
            let mut indexes_offset = tile_counts.len() as u64 * 1024;
            data.extend_from_slice(&[73, 73, 43, 0, 8, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0]);
            for level in 0..tile_counts.len() {
                let mut ifd = Vec::new();

                // Number of tags
                ifd.extend_from_slice((NUM_TAGS as u64).to_le_bytes().as_slice());

                // TIFF new SubfileType
                ifd.extend_from_slice(&[0xFE, 0, 4, 0, 1, 0, 0, 0, 0, 0, 0, 0]);
                if level == 0 {
                    ifd.extend_from_slice(&[0; 8]);
                } else {
                    ifd.extend_from_slice(&[1, 0, 0, 0, 0, 0, 0, 0]);
                }

                // TIFF width
                ifd.extend_from_slice(&[0, 1, 4, 0, 1, 0, 0, 0, 0, 0, 0, 0]);
                ifd.extend_from_slice((widths[level] as u64).to_le_bytes().as_slice());

                // TIFF height
                ifd.extend_from_slice(&[1, 1, 4, 0, 1, 0, 0, 0, 0, 0, 0, 0]);
                ifd.extend_from_slice((heights[level] as u64).to_le_bytes().as_slice());

                // TIFF bits per sample
                assert!(!bpp.is_empty() && bpp.len() <= 8);
                ifd.extend_from_slice(&[2, 1, 1, 0, bpp.len() as u8, 0, 0, 0, 0, 0, 0, 0]);
                ifd.extend_from_slice(&bpp);
                ifd.extend_from_slice(&[0; 8][..8 - bpp.len()]);

                // TIFF compression
                ifd.extend_from_slice(&[3, 1, 4, 0, 1, 0, 0, 0, 0, 0, 0, 0]);
                ifd.extend_from_slice(5u64.to_le_bytes().as_slice());

                // TIFF photometric interpretation
                ifd.extend_from_slice(&[6, 1, 4, 0, 1, 0, 0, 0, 0, 0, 0, 0]);
                if bpp.len() == 3 {
                    ifd.extend_from_slice(2u64.to_le_bytes().as_slice());
                } else {
                    ifd.extend_from_slice(1u64.to_le_bytes().as_slice());
                }

                // TIFF samples per pixel
                ifd.extend_from_slice(&[0x15, 1, 4, 0, 1, 0, 0, 0, 0, 0, 0, 0]);
                ifd.extend_from_slice((bpp.len() as u64).to_le_bytes().as_slice());

                // TIFF tile width + height
                ifd.extend_from_slice(&[0x42, 1, 4, 0, 1, 0, 0, 0, 0, 0, 0, 0]);
                ifd.extend_from_slice((TILE_SIZE as u64).to_le_bytes().as_slice());
                ifd.extend_from_slice(&[0x43, 1, 4, 0, 1, 0, 0, 0, 0, 0, 0, 0]);
                ifd.extend_from_slice((TILE_SIZE as u64).to_le_bytes().as_slice());

                // TIFF tile offsets
                ifd.extend_from_slice(&[0x44, 1, 16, 0]);
                ifd.extend_from_slice((tile_counts[level] as u64).to_le_bytes().as_slice());
                if tile_counts[level] > 1 {
                    ifd.extend_from_slice(indexes_offset.to_le_bytes().as_slice());
                } else {
                    ifd.extend_from_slice(1u64.to_le_bytes().as_slice());
                }

                // TIFF tile sizes
                ifd.extend_from_slice(&[0x45, 1, 16, 0]);
                ifd.extend_from_slice((tile_counts[level] as u64).to_le_bytes().as_slice());
                if tile_counts[level] > 1 {
                    ifd.extend_from_slice(
                        (indexes_offset + tile_counts[level] as u64 * 8)
                            .to_le_bytes()
                            .as_slice(),
                    );
                } else {
                    ifd.extend_from_slice(&[0; 8]);
                }

                // TIFF sample format
                ifd.extend_from_slice(&[0x53, 1, 3, 0, 1, 0, 0, 0, 0, 0, 0, 0]);
                if signed {
                    ifd.extend_from_slice(2u64.to_le_bytes().as_slice());
                } else {
                    ifd.extend_from_slice(1u64.to_le_bytes().as_slice());
                }

                // // GDAL nodata
                // assert!(nodata.len() < 8);
                // ifd.extend_from_slice(&[0x81, 0xA4, 2, 0, nodata.len() as u8 + 1, 0, 0, 0, 0, 0, 0, 0]);
                // ifd.extend_from_slice(nodata.as_bytes());
                // ifd.extend_from_slice(&[0; 8][..8 - nodata.len()]);

                // Next IFD
                if level < tile_counts.len() - 1 {
                    ifd.extend_from_slice(((level + 1) as u64 * 1024).to_le_bytes().as_slice());
                } else {
                    ifd.extend_from_slice(&[0; 8]);
                }

                assert!(ifd.len() <= 1000);
                data.extend_from_slice(&ifd);
                data.extend_from_slice(&vec![0; 1024 - (data.len() % 1024)]);

                indexes_offset += tile_counts[level] as u64 * 16;
            }

            file.write_all(&data)?;
            for &tiles in &tile_counts {
                file.write_all(
                    &*1u64
                        .to_le_bytes()
                        .as_slice()
                        .iter()
                        .cycle()
                        .take(tiles as usize * 8)
                        .copied()
                        .collect::<Vec<u8>>(),
                )?;
                file.write_all(&vec![0; tiles as usize * 8])?;
            }

            file_size = 1024 * tile_counts.len() as u64 + 16 * total_tiles;
        }

        Ok(CogBuilder {
            file,
            widths,
            heights,
            tile_counts,
            file_size,
        })
    }

    pub fn width(&self, level: u32) -> u32 {
        self.widths[level as usize]
    }
    pub fn height(&self, level: u32) -> u32 {
        self.heights[level as usize]
    }

    fn offset_size_locations(&self, level: u32, tile_index: u32) -> (u64, u64) {
        if self.tile_counts[level as usize] > 1 {
            let offset_location = self.tile_counts.len() as u64 * 1024
                + self.tile_counts[0..(level as usize)]
                    .iter()
                    .map(|&c| c as u64)
                    .sum::<u64>()
                    * 16
                + u64::from(tile_index) * 8;
            let size_location = offset_location + self.tile_counts[level as usize] as u64 * 8;
            (offset_location, size_location)
        } else if level == 0 {
            (
                16 + 8 + OFFSETS_TAG_INDEX * 20 + 12,
                16 + 8 + LENGTHS_TAG_INDEX * 20 + 12,
            )
        } else {
            (
                1024 * level as u64 + 8 + OFFSETS_TAG_INDEX * 20 + 12,
                1024 * level as u64 + 8 + LENGTHS_TAG_INDEX * 20 + 12,
            )
        }
    }

    pub fn valid_mask(&mut self, level: u32) -> Result<Vec<bool>, io::Error> {
        let start = self.offset_size_locations(level, 0).0;

        let mut data = vec![0u64; self.tile_counts[level as usize] as usize];
        self.file.seek(SeekFrom::Start(start))?;
        self.file.read_exact(bytemuck::cast_slice_mut(&mut data))?;

        Ok(data.into_iter().map(|offset| offset != 1).collect())
    }

    pub fn write_tile(&mut self, level: u32, index: u32, tile: &[u8]) -> Result<(), io::Error> {
        let file_end = self.file.seek(SeekFrom::End(0))?;
        assert_eq!(self.file_size, file_end);

        let (offset_location, size_location) = self.offset_size_locations(level, index);

        self.file.write_all(tile)?;

        self.file.seek(SeekFrom::Start(size_location))?;
        self.file.write_all(&(tile.len() as u64).to_le_bytes())?;
        self.file.flush()?;

        self.file.seek(SeekFrom::Start(offset_location))?;
        self.file.write_all(&(file_end as u64).to_le_bytes())?;
        self.file.flush()?;

        self.file_size += tile.len() as u64;
        Ok(())
    }

    pub fn write_nodata_tile(&mut self, level: u32, index: u32) -> Result<(), io::Error> {
        let offset_location = self.offset_size_locations(level, index).0;

        self.file.seek(SeekFrom::Start(offset_location))?;
        self.file.write_all(&0u64.to_le_bytes())?;
        self.file.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let file = std::fs::File::create("test.tiff").unwrap();
        let mut builder = CogBuilder::new(file, 4096, 4096, 8).unwrap();
        let compressed = compress_tile(&vec![255u8; 1024 * 1024]);
        let compressed2 = compress_tile(&vec![127u8; 1024 * 1024]);

        for level in 0..3 {
            for i in 0..(4u32 >> level).pow(2) {
                if i % 2 == (i / (4 >> level)) % 2 {
                    builder.write_tile(level, i, &compressed).unwrap();
                } else {
                    builder.write_tile(level, i, &compressed2).unwrap();
                }
            }
        }
    }
}