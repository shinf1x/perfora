use std::fs;
use std::path::{Path, PathBuf};

use image::{DynamicImage, GenericImageView, ImageFormat};

use crate::constants::{MAX_PREVIEW_EDGE, SUPPORTED_EXTENSIONS};
use crate::model::ExportImageFormat;

pub fn downscale_for_preview(img: DynamicImage) -> DynamicImage {
    let (w, h) = img.dimensions();
    let max_edge = w.max(h);

    if max_edge <= MAX_PREVIEW_EDGE {
        return img;
    }

    let scale = MAX_PREVIEW_EDGE as f32 / max_edge as f32;
    let new_w = (w as f32 * scale).round().max(1.0) as u32;
    let new_h = (h as f32 * scale).round().max(1.0) as u32;

    img.resize_exact(new_w, new_h, image::imageops::FilterType::Triangle)
}

pub fn probe_image_dimensions(path: &Path) -> Result<(u32, u32), String> {
    let reader = image::ImageReader::open(path)
        .map_err(|err| format!("Failed opening {}: {err}", path.display()))?
        .with_guessed_format()
        .map_err(|err| format!("Failed identifying {}: {err}", path.display()))?;

    reader
        .into_dimensions()
        .map_err(|err| format!("Failed reading dimensions {}: {err}", path.display()))
}

pub fn collect_supported_files(folder: &Path) -> Result<Vec<PathBuf>, String> {
    let read_dir = fs::read_dir(folder)
        .map_err(|err| format!("Unable to read folder {}: {err}", folder.display()))?;

    let mut files = Vec::new();

    for entry in read_dir {
        let entry = entry
            .map_err(|err| format!("Failed reading an entry in {}: {err}", folder.display()))?;
        let path = entry.path();

        if path.is_file() && is_supported_file(&path) {
            files.push(path);
        }
    }

    files.sort_by(|a, b| {
        let a_name = a.file_name().and_then(|s| s.to_str()).unwrap_or_default();
        let b_name = b.file_name().and_then(|s| s.to_str()).unwrap_or_default();
        a_name.cmp(b_name)
    });

    Ok(files)
}

fn is_supported_file(path: &Path) -> bool {
    let ext = path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();

    SUPPORTED_EXTENSIONS.contains(&ext.as_str())
}

pub fn output_format_and_extension(
    source_path: &Path,
) -> Result<(ImageFormat, &'static str), String> {
    let ext = source_path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();

    match ext.as_str() {
        "jpg" | "jpeg" => Ok((ImageFormat::Jpeg, "jpg")),
        "png" => Ok((ImageFormat::Png, "png")),
        "tif" | "tiff" => Ok((ImageFormat::Tiff, "tiff")),
        other => Err(format!("Unsupported output format extension: {other}")),
    }
}

pub fn output_format_and_extension_for_export(
    source_path: &Path,
    export_format: ExportImageFormat,
) -> Result<(ImageFormat, &'static str), String> {
    match export_format {
        ExportImageFormat::PreserveSource => output_format_and_extension(source_path),
        ExportImageFormat::Jpeg => Ok((ImageFormat::Jpeg, "jpg")),
        ExportImageFormat::Png => Ok((ImageFormat::Png, "png")),
        ExportImageFormat::Tiff => Ok((ImageFormat::Tiff, "tiff")),
    }
}

pub fn is_dynamic_image_16bit(img: &DynamicImage) -> bool {
    matches!(
        img,
        DynamicImage::ImageLuma16(_)
            | DynamicImage::ImageLumaA16(_)
            | DynamicImage::ImageRgb16(_)
            | DynamicImage::ImageRgba16(_)
    )
}
