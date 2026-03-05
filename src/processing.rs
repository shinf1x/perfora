use std::fs;
use std::io::{BufWriter, Seek, Write};
use std::path::Path;

use image::{DynamicImage, ImageBuffer, ImageFormat, Rgb, RgbImage};
use tiff::encoder::colortype;
use tiff::tags::Tag;

use crate::geometry::{FrameQuad, frame_quads_for_strip};
use crate::io_utils::{is_dynamic_image_16bit, output_format_and_extension_for_export};
use crate::model::{ExportMetadata, ExportSettings, ImageSettings, OutputBitDepth};

#[derive(Clone)]
struct FrameJob {
    strip_number: usize,
    frame_number: usize,
    quad: FrameQuad,
}

type Rgb16Image = ImageBuffer<Rgb<u16>, Vec<u16>>;

trait FrameChannel: Copy + 'static {
    const MAX_F32: f32;
    const HISTOGRAM_BINS: usize;

    fn as_f32(self) -> f32;
    fn from_f32(value: f32) -> Self;
    fn as_hist_index(self) -> usize;
    fn from_hist_index(index: usize) -> Self;
    fn invert(self) -> Self;
}

macro_rules! impl_frame_channel {
    ($channel:ty) => {
        impl FrameChannel for $channel {
            const MAX_F32: f32 = <$channel>::MAX as f32;
            const HISTOGRAM_BINS: usize = <$channel>::MAX as usize + 1;

            fn as_f32(self) -> f32 {
                self as f32
            }

            fn from_f32(value: f32) -> Self {
                value as $channel
            }

            fn as_hist_index(self) -> usize {
                self as usize
            }

            fn from_hist_index(index: usize) -> Self {
                index as $channel
            }

            fn invert(self) -> Self {
                <$channel>::MAX - self
            }
        }
    };
}

impl_frame_channel!(u8);
impl_frame_channel!(u16);

trait FrameImage: Sized {
    type Channel: FrameChannel;
    type TiffColor: colortype::ColorType<Inner = Self::Channel>;

    fn new(width: u32, height: u32) -> Self;
    fn width(&self) -> u32;
    fn height(&self) -> u32;
    fn get_rgb(&self, x: u32, y: u32) -> [Self::Channel; 3];
    fn put_rgb(&mut self, x: u32, y: u32, rgb: [Self::Channel; 3]);
    fn for_each_pixel_mut<F: FnMut(&mut [Self::Channel; 3])>(&mut self, f: F);
    fn flip_horizontal_in_place(&mut self);
    fn as_raw_slice(&self) -> &[Self::Channel];
    fn into_dynamic(self) -> DynamicImage;
    fn into_jpeg_dynamic(self) -> DynamicImage {
        self.into_dynamic()
    }
}

macro_rules! impl_frame_image {
    (
        image = $image:ty,
        channel = $channel:ty,
        tiff = $tiff_color:ty,
        dynamic = $dynamic_ctor:path
        $(,
            jpeg_dynamic = |$img:ident : $img_ty:ty| $jpeg_body:block
        )?
    ) => {
        impl FrameImage for $image {
            type Channel = $channel;
            type TiffColor = $tiff_color;

            fn new(width: u32, height: u32) -> Self {
                ImageBuffer::new(width, height)
            }

            fn width(&self) -> u32 {
                ImageBuffer::width(self)
            }

            fn height(&self) -> u32 {
                ImageBuffer::height(self)
            }

            fn get_rgb(&self, x: u32, y: u32) -> [Self::Channel; 3] {
                self.get_pixel(x, y).0
            }

            fn put_rgb(&mut self, x: u32, y: u32, rgb: [Self::Channel; 3]) {
                self.put_pixel(x, y, Rgb(rgb));
            }

            fn for_each_pixel_mut<F: FnMut(&mut [Self::Channel; 3])>(&mut self, mut f: F) {
                for px in self.pixels_mut() {
                    f(&mut px.0);
                }
            }

            fn flip_horizontal_in_place(&mut self) {
                image::imageops::flip_horizontal_in_place(self);
            }

            fn as_raw_slice(&self) -> &[Self::Channel] {
                self.as_raw()
            }

            fn into_dynamic(self) -> DynamicImage {
                $dynamic_ctor(self)
            }

            $(
                fn into_jpeg_dynamic(self) -> DynamicImage {
                    let $img: $img_ty = self;
                    $jpeg_body
                }
            )?
        }
    };
}

impl_frame_image!(
    image = RgbImage,
    channel = u8,
    tiff = colortype::RGB8,
    dynamic = DynamicImage::ImageRgb8
);

impl_frame_image!(
    image = Rgb16Image,
    channel = u16,
    tiff = colortype::RGB16,
    dynamic = DynamicImage::ImageRgb16,
    jpeg_dynamic = |frame: Rgb16Image| {
        let rgb8 = DynamicImage::ImageRgb16(frame).to_rgb8();
        DynamicImage::ImageRgb8(rgb8)
    }
);

pub fn process_image_file(
    source_path: &Path,
    settings: &ImageSettings,
    export_settings: &ExportSettings,
    output_folder: &Path,
    global_index: &mut usize,
    on_frame_exported: &mut dyn FnMut(),
) -> Result<usize, String> {
    let decoded = {
        let mut reader = image::ImageReader::open(source_path)
            .map_err(|err| format!("Failed opening file: {err}"))?
            .with_guessed_format()
            .map_err(|err| format!("Failed identifying format: {err}"))?;
        reader.no_limits();
        reader
            .decode()
            .map_err(|err| format!("Failed decoding image: {err}"))?
    };

    let frame_jobs = build_frame_jobs(settings);
    if frame_jobs.is_empty() {
        return Err("No frame jobs configured for this image.".to_owned());
    }

    let (output_format, output_ext) =
        output_format_and_extension_for_export(source_path, export_settings.format)?;
    let source_stem = source_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("scan");

    let source_is_16 = is_dynamic_image_16bit(&decoded);
    let preserve_16 =
        source_is_16 && matches!(export_settings.bit_depth, OutputBitDepth::PreserveSource);

    if preserve_16 {
        let source = decoded.into_rgb16();
        process_frame_jobs(
            &source,
            &frame_jobs,
            output_folder,
            source_stem,
            output_format,
            output_ext,
            export_settings,
            global_index,
            on_frame_exported,
        )
    } else {
        let source = decoded.into_rgb8();
        process_frame_jobs(
            &source,
            &frame_jobs,
            output_folder,
            source_stem,
            output_format,
            output_ext,
            export_settings,
            global_index,
            on_frame_exported,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn process_frame_jobs<I: FrameImage>(
    source: &I,
    frame_jobs: &[FrameJob],
    output_folder: &Path,
    source_stem: &str,
    output_format: ImageFormat,
    output_ext: &str,
    export_settings: &ExportSettings,
    global_index: &mut usize,
    on_frame_exported: &mut dyn FnMut(),
) -> Result<usize, String>
where
    [I::Channel]: tiff::encoder::TiffValue,
{
    let mut written = 0usize;

    for job in frame_jobs {
        let mut frame = sample_frame(source, job.quad)?;

        if export_settings.mirror {
            frame.flip_horizontal_in_place();
        }
        if export_settings.invert_colors {
            invert_frame(&mut frame);
        }

        if export_settings.auto_contrast_enabled {
            apply_percentile_contrast(
                &mut frame,
                export_settings.low_percentile,
                export_settings.high_percentile,
                export_settings.auto_contrast_sample_area_percent,
            );
        }

        let output_name = format!(
            "{:06}__{}__strip{}__frame{:02}.{}",
            *global_index, source_stem, job.strip_number, job.frame_number, output_ext,
        );
        let output_path = output_folder.join(output_name);

        save_frame(
            frame,
            &output_path,
            output_format,
            &export_settings.metadata,
        )?;
        *global_index += 1;
        written += 1;
        on_frame_exported();
    }

    Ok(written)
}

fn sample_frame<I: FrameImage>(source: &I, quad: FrameQuad) -> Result<I, String> {
    let width = quad.width_len().round().max(1.0) as u32;
    let height = quad.height_len().round().max(1.0) as u32;

    if width == 0 || height == 0 {
        return Err("Frame has invalid output dimensions.".to_owned());
    }

    let mut out = I::new(width, height);
    for y in 0..height {
        let v = (y as f32 + 0.5) / height as f32;
        for x in 0..width {
            let u = (x as f32 + 0.5) / width as f32;
            let src = quad.point_from_uv(u, v);
            out.put_rgb(x, y, sample_bilinear(source, src.x, src.y));
        }
    }

    Ok(out)
}

fn sample_bilinear<I: FrameImage>(source: &I, x: f32, y: f32) -> [I::Channel; 3] {
    let w = source.width() as i32;
    let h = source.height() as i32;

    let x = x.clamp(0.0, (w - 1).max(0) as f32);
    let y = y.clamp(0.0, (h - 1).max(0) as f32);

    let x0 = x.floor() as i32;
    let y0 = y.floor() as i32;
    let x1 = (x0 + 1).min(w - 1);
    let y1 = (y0 + 1).min(h - 1);

    let fx = x - x0 as f32;
    let fy = y - y0 as f32;

    let p00 = source.get_rgb(x0 as u32, y0 as u32);
    let p10 = source.get_rgb(x1 as u32, y0 as u32);
    let p01 = source.get_rgb(x0 as u32, y1 as u32);
    let p11 = source.get_rgb(x1 as u32, y1 as u32);

    let mut out = [I::Channel::from_hist_index(0); 3];
    for c in 0..3 {
        let top = p00[c].as_f32() * (1.0 - fx) + p10[c].as_f32() * fx;
        let bottom = p01[c].as_f32() * (1.0 - fx) + p11[c].as_f32() * fx;
        let value = top * (1.0 - fy) + bottom * fy;
        out[c] = I::Channel::from_f32(value.round().clamp(0.0, I::Channel::MAX_F32));
    }

    out
}

fn save_frame<I: FrameImage>(
    frame: I,
    output_path: &Path,
    format: ImageFormat,
    metadata: &ExportMetadata,
) -> Result<(), String>
where
    [I::Channel]: tiff::encoder::TiffValue,
{
    match format {
        ImageFormat::Jpeg => {
            frame
                .into_jpeg_dynamic()
                .save_with_format(output_path, format)
                .map_err(|err| format!("Failed saving {}: {err}", output_path.display()))?;
            embed_metadata_in_file(output_path, format, metadata)
        }
        ImageFormat::Tiff => save_tiff_with_metadata(frame, output_path, metadata),
        ImageFormat::Png => {
            frame
                .into_dynamic()
                .save_with_format(output_path, format)
                .map_err(|err| format!("Failed saving {}: {err}", output_path.display()))?;
            embed_metadata_in_file(output_path, format, metadata)
        }
        _ => frame
            .into_dynamic()
            .save_with_format(output_path, format)
            .map_err(|err| format!("Failed saving {}: {err}", output_path.display())),
    }
}

fn save_tiff_with_metadata<I: FrameImage>(
    frame: I,
    output_path: &Path,
    metadata: &ExportMetadata,
) -> Result<(), String>
where
    [I::Channel]: tiff::encoder::TiffValue,
{
    let file = fs::File::create(output_path)
        .map_err(|err| format!("Failed creating {}: {err}", output_path.display()))?;
    let writer = BufWriter::new(file);
    let mut encoder = tiff::encoder::TiffEncoder::new(writer).map_err(|err| {
        format!(
            "Failed creating TIFF encoder {}: {err}",
            output_path.display()
        )
    })?;
    let mut image = encoder
        .new_image::<I::TiffColor>(frame.width(), frame.height())
        .map_err(|err| {
            format!(
                "Failed creating TIFF image {}: {err}",
                output_path.display()
            )
        })?;
    write_tiff_metadata_tags(image.encoder(), metadata)?;
    image
        .write_data(frame.as_raw_slice())
        .map_err(|err| format!("Failed writing TIFF data {}: {err}", output_path.display()))
}

fn write_tiff_metadata_tags<W: Write + Seek, K: tiff::encoder::TiffKind>(
    encoder: &mut tiff::encoder::DirectoryEncoder<'_, W, K>,
    metadata: &ExportMetadata,
) -> Result<(), String> {
    let software = "Perfora";
    encoder
        .write_tag(Tag::Software, software)
        .map_err(|err| format!("Failed writing TIFF Software tag: {err}"))?;

    if !metadata.camera_make.trim().is_empty() {
        encoder
            .write_tag(Tag::Make, metadata.camera_make.trim())
            .map_err(|err| format!("Failed writing TIFF Make tag: {err}"))?;
    }
    if !metadata.camera_model.trim().is_empty() {
        encoder
            .write_tag(Tag::Model, metadata.camera_model.trim())
            .map_err(|err| format!("Failed writing TIFF Model tag: {err}"))?;
    }
    if !metadata.image_description.trim().is_empty() {
        encoder
            .write_tag(Tag::ImageDescription, metadata.image_description.trim())
            .map_err(|err| format!("Failed writing TIFF ImageDescription tag: {err}"))?;
    }
    if !metadata.author.trim().is_empty() {
        encoder
            .write_tag(Tag::Artist, metadata.author.trim())
            .map_err(|err| format!("Failed writing TIFF Artist tag: {err}"))?;
    }
    if !metadata.scan_datetime.trim().is_empty() {
        let dt = metadata.scan_datetime.trim();
        encoder
            .write_tag(Tag::DateTime, dt)
            .map_err(|err| format!("Failed writing TIFF DateTime tag: {err}"))?;
        encoder
            .write_tag(Tag::Unknown(0x9004), dt)
            .map_err(|err| format!("Failed writing TIFF DateTimeDigitized tag: {err}"))?;
    }

    let comment = metadata_user_comment(metadata);
    if !comment.is_empty() {
        encoder
            .write_tag(Tag::Unknown(0x9286), comment.as_str())
            .map_err(|err| format!("Failed writing TIFF UserComment tag: {err}"))?;
    }

    Ok(())
}

fn embed_metadata_in_file(
    output_path: &Path,
    format: ImageFormat,
    metadata: &ExportMetadata,
) -> Result<(), String> {
    let exif_payload = match build_exif_tiff_payload(metadata) {
        Some(payload) => payload,
        None => return Ok(()),
    };

    match format {
        ImageFormat::Jpeg => inject_exif_into_jpeg(output_path, &exif_payload),
        ImageFormat::Png => inject_exif_into_png(output_path, &exif_payload),
        _ => Ok(()),
    }
}

fn inject_exif_into_jpeg(path: &Path, exif_tiff_payload: &[u8]) -> Result<(), String> {
    let jpeg = fs::read(path).map_err(|err| format!("Failed reading {}: {err}", path.display()))?;
    if jpeg.len() < 2 || jpeg[0] != 0xFF || jpeg[1] != 0xD8 {
        return Err(format!("{} is not a valid JPEG file.", path.display()));
    }

    let payload_len = 6usize.saturating_add(exif_tiff_payload.len());
    let segment_len = payload_len.saturating_add(2);
    if segment_len > u16::MAX as usize {
        return Err(format!(
            "EXIF metadata too large for JPEG APP1 segment in {}.",
            path.display()
        ));
    }

    let mut app1 = Vec::with_capacity(2 + 2 + payload_len);
    app1.extend_from_slice(&[0xFF, 0xE1]);
    app1.extend_from_slice(&(segment_len as u16).to_be_bytes());
    app1.extend_from_slice(b"Exif\0\0");
    app1.extend_from_slice(exif_tiff_payload);

    let mut out = Vec::with_capacity(jpeg.len() + app1.len());
    out.extend_from_slice(&jpeg[..2]);
    out.extend_from_slice(&app1);
    out.extend_from_slice(&jpeg[2..]);

    fs::write(path, out).map_err(|err| format!("Failed writing {}: {err}", path.display()))
}

fn inject_exif_into_png(path: &Path, exif_tiff_payload: &[u8]) -> Result<(), String> {
    let png = fs::read(path).map_err(|err| format!("Failed reading {}: {err}", path.display()))?;
    const PNG_SIG: [u8; 8] = [137, 80, 78, 71, 13, 10, 26, 10];
    if png.len() < 8 || png[..8] != PNG_SIG {
        return Err(format!("{} is not a valid PNG file.", path.display()));
    }

    let mut insert_pos = None;
    let mut pos = 8usize;
    while pos + 12 <= png.len() {
        let len = u32::from_be_bytes([png[pos], png[pos + 1], png[pos + 2], png[pos + 3]]) as usize;
        let chunk_start = pos;
        let chunk_type_start = pos + 4;
        let chunk_data_start = pos + 8;
        let chunk_end = chunk_data_start.saturating_add(len).saturating_add(4);
        if chunk_end > png.len() {
            return Err(format!("{} has a malformed PNG chunk.", path.display()));
        }

        let chunk_type = &png[chunk_type_start..chunk_type_start + 4];
        if chunk_type == b"IHDR" {
            insert_pos = Some(chunk_end);
            break;
        }

        pos = chunk_end;
        if chunk_start == pos {
            break;
        }
    }

    let Some(insert_pos) = insert_pos else {
        return Err(format!("{} PNG has no IHDR chunk.", path.display()));
    };

    let len = exif_tiff_payload.len();
    if len > u32::MAX as usize {
        return Err(format!(
            "EXIF payload too large for PNG in {}.",
            path.display()
        ));
    }

    let mut exif_chunk = Vec::with_capacity(12 + len);
    exif_chunk.extend_from_slice(&(len as u32).to_be_bytes());
    exif_chunk.extend_from_slice(b"eXIf");
    exif_chunk.extend_from_slice(exif_tiff_payload);
    let crc = crc32_ieee(&exif_chunk[4..]);
    exif_chunk.extend_from_slice(&crc.to_be_bytes());

    let mut out = Vec::with_capacity(png.len() + exif_chunk.len());
    out.extend_from_slice(&png[..insert_pos]);
    out.extend_from_slice(&exif_chunk);
    out.extend_from_slice(&png[insert_pos..]);

    fs::write(path, out).map_err(|err| format!("Failed writing {}: {err}", path.display()))
}

fn build_exif_tiff_payload(metadata: &ExportMetadata) -> Option<Vec<u8>> {
    let mut ifd0_entries = Vec::new();

    if !metadata.image_description.trim().is_empty() {
        ifd0_entries.push(IfdEntry::ascii(0x010E, metadata.image_description.trim()));
    }
    if !metadata.camera_make.trim().is_empty() {
        ifd0_entries.push(IfdEntry::ascii(0x010F, metadata.camera_make.trim()));
    }
    if !metadata.camera_model.trim().is_empty() {
        ifd0_entries.push(IfdEntry::ascii(0x0110, metadata.camera_model.trim()));
    }
    if !metadata.author.trim().is_empty() {
        ifd0_entries.push(IfdEntry::ascii(0x013B, metadata.author.trim()));
    }
    if !metadata.scan_datetime.trim().is_empty() {
        ifd0_entries.push(IfdEntry::ascii(0x0132, metadata.scan_datetime.trim()));
    }
    ifd0_entries.push(IfdEntry::ascii(0x0131, "Perfora"));

    let mut exif_entries = Vec::new();
    if !metadata.scan_datetime.trim().is_empty() {
        exif_entries.push(IfdEntry::ascii(0x9004, metadata.scan_datetime.trim()));
    }
    let user_comment = metadata_user_comment(metadata);
    if !user_comment.is_empty() {
        let mut bytes = b"ASCII\0\0\0".to_vec();
        bytes.extend_from_slice(user_comment.as_bytes());
        exif_entries.push(IfdEntry::undefined(0x9286, bytes));
    }
    if !exif_entries.is_empty() {
        exif_entries.push(IfdEntry::undefined(0x9000, b"0230".to_vec()));
    }

    let has_user_content = ifd0_entries.iter().any(|e| e.tag != 0x0131) || !exif_entries.is_empty();
    if !has_user_content {
        return None;
    }

    ifd0_entries.sort_by_key(|e| e.tag);
    exif_entries.sort_by_key(|e| e.tag);

    let ifd0_count = ifd0_entries.len() + usize::from(!exif_entries.is_empty());
    let ifd0_table_len = 2 + ifd0_count * 12 + 4;
    let ifd0_data_len: usize = ifd0_entries
        .iter()
        .map(|e| if e.data.len() > 4 { e.data.len() } else { 0 })
        .sum();

    let exif_ifd_offset = if !exif_entries.is_empty() {
        8 + ifd0_table_len + ifd0_data_len
    } else {
        0
    };

    let exif_table_len = if exif_entries.is_empty() {
        0
    } else {
        2 + exif_entries.len() * 12 + 4
    };
    let exif_data_len: usize = exif_entries
        .iter()
        .map(|e| if e.data.len() > 4 { e.data.len() } else { 0 })
        .sum();

    let total_len = exif_ifd_offset + exif_table_len + exif_data_len;
    let mut out = Vec::with_capacity(total_len.max(8));
    out.extend_from_slice(b"II");
    out.extend_from_slice(&42u16.to_le_bytes());
    out.extend_from_slice(&8u32.to_le_bytes());

    let mut ifd0_data = Vec::with_capacity(ifd0_data_len);
    let mut ifd0_data_cursor = 8 + ifd0_table_len;
    out.extend_from_slice(&(ifd0_count as u16).to_le_bytes());
    for entry in &ifd0_entries {
        write_ifd_entry(
            &mut out,
            &mut ifd0_data,
            &mut ifd0_data_cursor,
            entry.tag,
            entry.typ,
            &entry.data,
        );
    }
    if !exif_entries.is_empty() {
        out.extend_from_slice(&0x8769u16.to_le_bytes());
        out.extend_from_slice(&4u16.to_le_bytes()); // LONG
        out.extend_from_slice(&1u32.to_le_bytes());
        out.extend_from_slice(&(exif_ifd_offset as u32).to_le_bytes());
    }
    out.extend_from_slice(&0u32.to_le_bytes()); // next IFD = none
    out.extend_from_slice(&ifd0_data);

    if !exif_entries.is_empty() {
        let mut exif_data = Vec::with_capacity(exif_data_len);
        let mut exif_data_cursor = exif_ifd_offset + exif_table_len;
        out.extend_from_slice(&(exif_entries.len() as u16).to_le_bytes());
        for entry in &exif_entries {
            write_ifd_entry(
                &mut out,
                &mut exif_data,
                &mut exif_data_cursor,
                entry.tag,
                entry.typ,
                &entry.data,
            );
        }
        out.extend_from_slice(&0u32.to_le_bytes()); // next Exif IFD
        out.extend_from_slice(&exif_data);
    }

    Some(out)
}

fn write_ifd_entry(
    out: &mut Vec<u8>,
    data_section: &mut Vec<u8>,
    data_cursor: &mut usize,
    tag: u16,
    typ: u16,
    data: &[u8],
) {
    out.extend_from_slice(&tag.to_le_bytes());
    out.extend_from_slice(&typ.to_le_bytes());
    out.extend_from_slice(&(data.len() as u32).to_le_bytes());

    if data.len() <= 4 {
        let mut value = [0u8; 4];
        value[..data.len()].copy_from_slice(data);
        out.extend_from_slice(&value);
    } else {
        out.extend_from_slice(&(*data_cursor as u32).to_le_bytes());
        data_section.extend_from_slice(data);
        *data_cursor += data.len();
    }
}

fn metadata_user_comment(metadata: &ExportMetadata) -> String {
    if metadata.film_stock.trim().is_empty() {
        metadata.notes.trim().to_owned()
    } else if metadata.notes.trim().is_empty() {
        format!("FilmStock: {}", metadata.film_stock.trim())
    } else {
        format!(
            "FilmStock: {}; Notes: {}",
            metadata.film_stock.trim(),
            metadata.notes.trim()
        )
    }
}

#[derive(Clone)]
struct IfdEntry {
    tag: u16,
    typ: u16,
    data: Vec<u8>,
}

impl IfdEntry {
    fn ascii(tag: u16, value: &str) -> Self {
        let mut data = value.as_bytes().to_vec();
        data.push(0);
        Self { tag, typ: 2, data }
    }

    fn undefined(tag: u16, data: Vec<u8>) -> Self {
        Self { tag, typ: 7, data }
    }
}

fn crc32_ieee(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &b in bytes {
        crc ^= b as u32;
        for _ in 0..8 {
            if crc & 1 == 1 {
                crc = (crc >> 1) ^ 0xEDB8_8320;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

fn build_frame_jobs(settings: &ImageSettings) -> Vec<FrameJob> {
    let mut jobs = Vec::new();

    for strip_index in 0..settings.strip_count {
        let strip = &settings.strips[strip_index];
        let frame_quads = frame_quads_for_strip(strip);

        for (frame_idx, frame_quad) in frame_quads.into_iter().enumerate() {
            jobs.push(FrameJob {
                strip_number: strip_index + 1,
                frame_number: frame_idx + 1,
                quad: frame_quad,
            });
        }
    }

    jobs
}

pub fn configured_frame_count(settings: &ImageSettings) -> usize {
    build_frame_jobs(settings).len()
}

fn invert_frame<I: FrameImage>(image: &mut I) {
    image.for_each_pixel_mut(|px| {
        px[0] = px[0].invert();
        px[1] = px[1].invert();
        px[2] = px[2].invert();
    });
}

fn apply_percentile_contrast<I: FrameImage>(
    image: &mut I,
    low_pct: f32,
    high_pct: f32,
    sample_area_percent: f32,
) {
    if image.width() == 0 || image.height() == 0 {
        return;
    }

    let (x0, x1, y0, y1) =
        centered_roi_bounds_for_area(image.width(), image.height(), sample_area_percent);
    let mut hist_r = vec![0u32; I::Channel::HISTOGRAM_BINS];
    let mut hist_g = vec![0u32; I::Channel::HISTOGRAM_BINS];
    let mut hist_b = vec![0u32; I::Channel::HISTOGRAM_BINS];
    let mut count = 0u32;

    for y in y0..y1 {
        for x in x0..x1 {
            let px = image.get_rgb(x, y);
            hist_r[px[0].as_hist_index()] += 1;
            hist_g[px[1].as_hist_index()] += 1;
            hist_b[px[2].as_hist_index()] += 1;
            count += 1;
        }
    }

    if count == 0 {
        return;
    }

    let low_r = I::Channel::from_hist_index(percentile_from_hist(&hist_r, count, low_pct));
    let high_r = I::Channel::from_hist_index(percentile_from_hist(&hist_r, count, high_pct));
    let low_g = I::Channel::from_hist_index(percentile_from_hist(&hist_g, count, low_pct));
    let high_g = I::Channel::from_hist_index(percentile_from_hist(&hist_g, count, high_pct));
    let low_b = I::Channel::from_hist_index(percentile_from_hist(&hist_b, count, low_pct));
    let high_b = I::Channel::from_hist_index(percentile_from_hist(&hist_b, count, high_pct));

    image.for_each_pixel_mut(|px| {
        px[0] = stretch_value(px[0], low_r, high_r);
        px[1] = stretch_value(px[1], low_g, high_g);
        px[2] = stretch_value(px[2], low_b, high_b);
    });
}

fn stretch_value<P: FrameChannel>(value: P, low: P, high: P) -> P {
    let value_idx = value.as_hist_index();
    let low_idx = low.as_hist_index();
    let high_idx = high.as_hist_index();

    if high_idx <= low_idx {
        return value;
    }

    if value_idx <= low_idx {
        return P::from_hist_index(0);
    }
    if value_idx >= high_idx {
        return P::from_hist_index(P::HISTOGRAM_BINS.saturating_sub(1));
    }

    let numerator = (value_idx - low_idx) as f32;
    let denominator = (high_idx - low_idx) as f32;
    P::from_f32(
        ((numerator / denominator) * P::MAX_F32)
            .round()
            .clamp(0.0, P::MAX_F32),
    )
}

fn percentile_from_hist(hist: &[u32], total: u32, percentile: f32) -> usize {
    if total == 0 {
        return 0;
    }

    let pct = percentile.clamp(0.0, 100.0) / 100.0;
    let target = (pct * (total.saturating_sub(1) as f32)).round() as u32;

    let mut cumulative = 0u32;
    for (idx, count) in hist.iter().enumerate() {
        cumulative += *count;
        if cumulative > target {
            return idx;
        }
    }

    hist.len().saturating_sub(1)
}

fn centered_roi_bounds_for_area(
    width: u32,
    height: u32,
    sample_area_percent: f32,
) -> (u32, u32, u32, u32) {
    if width == 0 || height == 0 {
        return (0, 0, 0, 0);
    }

    let area_fraction = (sample_area_percent / 100.0).clamp(0.01, 1.0);
    let side_scale = area_fraction.sqrt();
    let roi_w = ((width as f32) * side_scale)
        .round()
        .clamp(1.0, width as f32) as u32;
    let roi_h = ((height as f32) * side_scale)
        .round()
        .clamp(1.0, height as f32) as u32;

    let x0 = (width.saturating_sub(roi_w)) / 2;
    let y0 = (height.saturating_sub(roi_h)) / 2;
    let x1 = (x0 + roi_w).min(width).max(x0 + 1);
    let y1 = (y0 + roi_h).min(height).max(y0 + 1);

    (x0, x1, y0, y1)
}
