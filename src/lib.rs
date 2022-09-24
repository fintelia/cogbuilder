use std::{
    borrow::BorrowMut,
    cell::{RefCell, RefMut},
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use thread_local::ThreadLocal;

pub const TILE_SIZE: u32 = 1024;

const NUM_TAGS: u32 = 12;
const OFFSETS_TAG_INDEX: u64 = 9;
const LENGTHS_TAG_INDEX: u64 = 10;

pub fn compress_tile(data: &[u8]) -> Vec<u8> {
    weezl::encode::Encoder::with_tiff_size_switch(weezl::BitOrder::Msb, 8)
        .encode(data)
        .unwrap()
}

pub fn decompress_tile(data: &[u8]) -> Result<Vec<u8>, anyhow::Error> {
    Ok(weezl::decode::Decoder::with_tiff_size_switch(weezl::BitOrder::Msb, 8).decode(data)?)
}

pub struct CogBuilder {
    path: PathBuf,
    files: ThreadLocal<RefCell<File>>,
    widths: Vec<u32>,
    heights: Vec<u32>,
    tile_counts: Vec<u32>,
    file_size: u64,
}

impl CogBuilder {
    fn get_file(
        files: &ThreadLocal<RefCell<File>>,
        path: impl AsRef<Path>,
    ) -> Result<RefMut<File>, std::io::Error> {
        let refcell: &RefCell<_> = files.get_or_try(|| -> Result<RefCell<_>, std::io::Error> {
            Ok(RefCell::new(
                File::options()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(path)?,
            ))
        })?;
        Ok(refcell.borrow_mut())
    }
    fn file(&self) -> Result<RefMut<File>, std::io::Error> {
        Self::get_file(&self.files, &self.path)
    }

    pub fn new(
        path: PathBuf,
        width: u32,
        height: u32,
        bpp: Vec<u8>,
        signed: bool,
        _nodata: &str,
    ) -> Result<Self, anyhow::Error> {
        let files = ThreadLocal::new();
        let mut  file = Self::get_file(&files, &path)?;
        let original_file_size = file.seek(SeekFrom::End(0))?;

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

        file.seek(SeekFrom::Start(0))?;

        let mut single_offset_size = (1u64, 0u64);
        let total_tiles = tile_counts.iter().map(|&c| c as u64).sum::<u64>();
        let new_file_size =
            (1024 * tile_counts.len() as u64 + 16 * total_tiles).max(original_file_size);
        if original_file_size >= 1024 * tile_counts.len() as u64 {
            let mut ifd_buffers = vec![0; 1024 * tile_counts.len()];
            file.read_exact(&mut ifd_buffers)?;
            file.seek(SeekFrom::Start(0))?;

            let mut ifd_offset = 16;
            for i in 0..tile_counts.len() {
                let num_tags =
                    u64::from_le_bytes(ifd_buffers[ifd_offset..][..8].try_into().unwrap()) as usize;
                for tag in 0..num_tags {
                    let kind = u16::from_le_bytes(
                        ifd_buffers[ifd_offset + 8 + 20 * tag..][..2]
                            .try_into()
                            .unwrap(),
                    );
                    let value = u64::from_le_bytes(
                        ifd_buffers[ifd_offset + 8 + 20 * tag + 12..][..8]
                            .try_into()
                            .unwrap(),
                    );

                    match kind {
                        0x144 if tile_counts[i] == 1 => single_offset_size.0 = value,
                        0x145 if tile_counts[i] == 1 => single_offset_size.1 = value,
                        _ => (),
                    }
                }

                ifd_offset = 1024 * (i + 1);
            }
        }

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

            // TIFF tile width
            ifd.extend_from_slice(&[0x42, 1, 4, 0, 1, 0, 0, 0, 0, 0, 0, 0]);
            ifd.extend_from_slice((TILE_SIZE as u64).to_le_bytes().as_slice());

            // TIFF tile height
            ifd.extend_from_slice(&[0x43, 1, 4, 0, 1, 0, 0, 0, 0, 0, 0, 0]);
            ifd.extend_from_slice((TILE_SIZE as u64).to_le_bytes().as_slice());

            // TIFF tile offsets
            ifd.extend_from_slice(&[0x44, 1, 16, 0]);
            ifd.extend_from_slice((tile_counts[level] as u64).to_le_bytes().as_slice());
            if tile_counts[level] > 1 {
                ifd.extend_from_slice(indexes_offset.to_le_bytes().as_slice());
            } else {
                ifd.extend_from_slice(single_offset_size.0.to_le_bytes().as_slice());
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
                ifd.extend_from_slice(single_offset_size.1.to_le_bytes().as_slice());
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

        if original_file_size < new_file_size {
            if original_file_size > data.len() as u64 {
                file.seek(SeekFrom::Start(original_file_size))?;
            }

            let mut tile_data = Vec::new();
            for &tiles in &tile_counts {
                tile_data.extend_from_slice(&vec![1u64; tiles as usize]);
                tile_data.extend_from_slice(&vec![0u64; tiles as usize]);
            }
            let buf = bytemuck::cast_slice(&tile_data);
            file.write_all(&buf[(original_file_size as usize).saturating_sub(data.len())..])?;
        }

        drop(file);
        Ok(CogBuilder {
            path,
            files,
            widths,
            heights,
            tile_counts,
            file_size: new_file_size,
        })
    }

    pub fn width(&self, level: u32) -> u32 {
        self.widths[level as usize]
    }
    pub fn height(&self, level: u32) -> u32 {
        self.heights[level as usize]
    }
    pub fn tiles_across(&self, level: u32) -> u32 {
        (self.widths[level as usize] + TILE_SIZE - 1) / TILE_SIZE
    }
    pub fn tiles_down(&self, level: u32) -> u32 {
        (self.heights[level as usize] + TILE_SIZE - 1) / TILE_SIZE
    }
    pub fn levels(&self) -> u32 {
        self.tile_counts.len() as u32
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
            assert_eq!(tile_index, 0);
            (
                16 + 8 + OFFSETS_TAG_INDEX * 20 + 12,
                16 + 8 + LENGTHS_TAG_INDEX * 20 + 12,
            )
        } else {
            assert_eq!(tile_index, 0);
            (
                1024 * level as u64 + 8 + OFFSETS_TAG_INDEX * 20 + 12,
                1024 * level as u64 + 8 + LENGTHS_TAG_INDEX * 20 + 12,
            )
        }
    }

    pub fn valid_mask(&self, level: u32) -> Result<Vec<bool>, anyhow::Error> {
        let start = self.offset_size_locations(level, 0).0;

        let mut data = vec![0u64; self.tile_counts[level as usize] as usize];
        let mut file = self.file()?;
        file.seek(SeekFrom::Start(start))?;
        file.read_exact(bytemuck::cast_slice_mut(&mut data))?;

        Ok(data.into_iter().map(|offset| offset != 1).collect())
    }

    pub fn write_tile(&mut self, level: u32, index: u32, tile: &[u8]) -> Result<(), anyhow::Error> {
        let file_end = { self.file()?.seek(SeekFrom::End(0))? };
        assert_eq!(self.file_size, file_end);

        let (offset_location, size_location) = self.offset_size_locations(level, index);

        let mut file = self.file()?;
        file.write_all(tile)?;

        file.seek(SeekFrom::Start(size_location))?;
        file.write_all(&(tile.len() as u64).to_le_bytes())?;
        file.flush()?;

        file.seek(SeekFrom::Start(offset_location))?;
        file.write_all(&(file_end as u64).to_le_bytes())?;
        file.flush()?;
        drop(file);

        self.file_size += tile.len() as u64;
        Ok(())
    }

    pub fn write_nodata_tile(&mut self, level: u32, index: u32) -> Result<(), anyhow::Error> {
        let offset_location = self.offset_size_locations(level, index).0;

        let mut file = self.file()?;
        file.seek(SeekFrom::Start(offset_location))?;
        file.write_all(&0u64.to_le_bytes())?;
        Ok(file.flush()?)
    }

    pub fn read_tile(&self, level: u32, index: u32) -> Result<Option<Vec<u8>>, anyhow::Error> {
        if index >= self.tile_counts[level as usize] {
            return Ok(None);
        }
        let (offset_location, size_location) = self.offset_size_locations(level, index);

        let mut offset = [0; 8];
        let mut size = [0; 8];

        let mut file = self.file()?;
        file.seek(SeekFrom::Start(size_location))?;
        file.read_exact(size.as_mut_slice())?;

        file.seek(SeekFrom::Start(offset_location))?;
        file.read_exact(offset.as_mut_slice())?;

        let offset = u64::from_le_bytes(offset);
        let size = u64::from_le_bytes(size);

        if size == 0 {
            return Ok(None);
        }

        let mut tile = vec![0; size as usize];
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut tile)?;

        Ok(Some(tile))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let mut builder = CogBuilder::new("test.tiff".into(), 4096, 4096, vec![8], false, "0").unwrap();
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
