use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::PathBuf;

use serde::Deserialize;

use crate::model::{ExportImageFormat, FrameDirection, OutputBitDepth};

const CONFIG_ENV_VAR: &str = "PERFORA_CONFIG";
const DEFAULT_CONFIG_FILENAMES: [&str; 2] = ["perfora.toml", ".perfora.toml"];

#[derive(Clone, Default, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub defaults: ConfigDefaults,
    #[serde(default)]
    pub presets: ConfigPresets,
}

#[derive(Clone, Default, Deserialize)]
pub struct ConfigDefaults {
    pub strip_count: Option<usize>,
    pub frame_direction: Option<String>,
    pub export_format: Option<String>,
    pub bit_depth: Option<String>,
    pub camera_make: Option<String>,
    pub camera_model: Option<String>,
    pub film_stock: Option<String>,
    pub author: Option<String>,
}

#[derive(Clone, Default, Deserialize)]
pub struct ConfigPresets {
    #[serde(default)]
    pub cameras: Vec<CameraPreset>,
    #[serde(default)]
    pub films: Vec<String>,
    #[serde(default)]
    pub authors: Vec<String>,
}

#[derive(Clone, Default, Deserialize)]
pub struct CameraPreset {
    pub make: String,
    pub model: String,
}

#[derive(Clone, Copy)]
pub enum ConfigSource {
    EnvVar,
    BinaryDir,
    CurrentWorkingDir,
}

impl ConfigSource {
    pub fn label(self) -> &'static str {
        match self {
            ConfigSource::EnvVar => "env var",
            ConfigSource::BinaryDir => "binary dir",
            ConfigSource::CurrentWorkingDir => "cwd",
        }
    }
}

pub struct LoadedAppConfig {
    pub path: PathBuf,
    pub source: ConfigSource,
    pub config: AppConfig,
}

impl AppConfig {
    pub fn default_strip_count(&self) -> Option<usize> {
        self.defaults.strip_count.map(|count| count.clamp(1, 2))
    }

    pub fn default_frame_direction(&self) -> Option<FrameDirection> {
        parse_frame_direction(self.defaults.frame_direction.as_deref()?)
    }

    pub fn default_export_format(&self) -> Option<ExportImageFormat> {
        parse_export_format(self.defaults.export_format.as_deref()?)
    }

    pub fn default_bit_depth(&self) -> Option<OutputBitDepth> {
        parse_output_bit_depth(self.defaults.bit_depth.as_deref()?)
    }

    pub fn default_camera_make(&self) -> Option<String> {
        trimmed_non_empty(self.defaults.camera_make.as_deref())
    }

    pub fn default_camera_model(&self) -> Option<String> {
        trimmed_non_empty(self.defaults.camera_model.as_deref())
    }

    pub fn default_film_stock(&self) -> Option<String> {
        trimmed_non_empty(self.defaults.film_stock.as_deref())
    }

    pub fn default_author(&self) -> Option<String> {
        trimmed_non_empty(self.defaults.author.as_deref())
    }

    pub fn camera_make_options(&self) -> Vec<String> {
        collect_unique_sorted(
            self.presets
                .cameras
                .iter()
                .map(|preset| preset.make.as_str()),
        )
    }

    pub fn camera_model_options_for_make(&self, make: &str) -> Vec<String> {
        let make = make.trim();
        let has_make_filter = !make.is_empty();

        collect_unique_sorted(
            self.presets
                .cameras
                .iter()
                .filter(|preset| !has_make_filter || preset.make.trim().eq_ignore_ascii_case(make))
                .map(|preset| preset.model.as_str()),
        )
    }

    pub fn film_options(&self) -> Vec<String> {
        collect_unique_sorted(self.presets.films.iter().map(String::as_str))
    }

    pub fn author_options(&self) -> Vec<String> {
        collect_unique_sorted(self.presets.authors.iter().map(String::as_str))
    }

    pub fn validation_warnings(&self) -> Vec<String> {
        let mut warnings = Vec::new();

        if let Some(strip_count) = self.defaults.strip_count
            && !(1..=2).contains(&strip_count)
        {
            warnings.push(format!(
                "defaults.strip_count={strip_count} is outside supported range 1..=2 and will be clamped."
            ));
        }

        if let Some(raw) = self.defaults.frame_direction.as_deref().map(str::trim)
            && !raw.is_empty()
            && parse_frame_direction(raw).is_none()
        {
            warnings.push(format!(
                "defaults.frame_direction='{raw}' is invalid and will be ignored."
            ));
        }
        if let Some(raw) = self.defaults.export_format.as_deref().map(str::trim)
            && !raw.is_empty()
            && parse_export_format(raw).is_none()
        {
            warnings.push(format!(
                "defaults.export_format='{raw}' is invalid and will be ignored."
            ));
        }
        if let Some(raw) = self.defaults.bit_depth.as_deref().map(str::trim)
            && !raw.is_empty()
            && parse_output_bit_depth(raw).is_none()
        {
            warnings.push(format!(
                "defaults.bit_depth='{raw}' is invalid and will be ignored."
            ));
        }

        let camera_make_default = self.default_camera_make();
        if let Some(make) = camera_make_default.as_deref() {
            let options = self.camera_make_options();
            if !options.is_empty() && !contains_case_insensitive(&options, make) {
                warnings.push(format!(
                    "defaults.camera_make='{make}' is not present in presets.cameras."
                ));
            }
        }

        if let Some(model) = self.default_camera_model() {
            if let Some(make) = camera_make_default.as_deref() {
                let options = self.camera_model_options_for_make(make);
                if !options.is_empty() && !contains_case_insensitive(&options, &model) {
                    warnings.push(format!(
                        "defaults.camera_model='{model}' is not present for make '{make}' in presets.cameras."
                    ));
                }
            } else {
                let options = collect_unique_sorted(
                    self.presets
                        .cameras
                        .iter()
                        .map(|preset| preset.model.as_str()),
                );
                if !options.is_empty() && !contains_case_insensitive(&options, &model) {
                    warnings.push(format!(
                        "defaults.camera_model='{model}' is not present in presets.cameras."
                    ));
                }
            }
        }

        if let Some(film_stock) = self.default_film_stock() {
            let options = self.film_options();
            if !options.is_empty() && !contains_case_insensitive(&options, &film_stock) {
                warnings.push(format!(
                    "defaults.film_stock='{film_stock}' is not present in presets.films."
                ));
            }
        }

        if let Some(author) = self.default_author() {
            let options = self.author_options();
            if !options.is_empty() && !contains_case_insensitive(&options, &author) {
                warnings.push(format!(
                    "defaults.author='{author}' is not present in presets.authors."
                ));
            }
        }

        warnings
    }
}

pub fn load_optional_app_config() -> Result<Option<LoadedAppConfig>, String> {
    if let Some(path) = env_config_path()? {
        return load_config_path(path, ConfigSource::EnvVar).map(Some);
    }

    if let Some(path) = first_config_path_in_dir(binary_dir_path()) {
        return load_config_path(path, ConfigSource::BinaryDir).map(Some);
    }

    if let Some(path) = first_config_path_in_dir(env::current_dir().ok()) {
        return load_config_path(path, ConfigSource::CurrentWorkingDir).map(Some);
    }

    Ok(None)
}

fn env_config_path() -> Result<Option<PathBuf>, String> {
    let raw = match env::var(CONFIG_ENV_VAR) {
        Ok(value) => value,
        Err(env::VarError::NotPresent) => return Ok(None),
        Err(err) => {
            return Err(format!(
                "Failed reading environment variable {CONFIG_ENV_VAR}: {err}"
            ));
        }
    };

    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let path = PathBuf::from(trimmed);
    if !path.is_file() {
        return Err(format!(
            "{CONFIG_ENV_VAR} points to {}, but the file does not exist.",
            path.display()
        ));
    }

    Ok(Some(path))
}

fn binary_dir_path() -> Option<PathBuf> {
    env::current_exe()
        .ok()
        .and_then(|path| path.parent().map(PathBuf::from))
}

fn first_config_path_in_dir(dir: Option<PathBuf>) -> Option<PathBuf> {
    let dir = dir?;
    for filename in DEFAULT_CONFIG_FILENAMES {
        let path = dir.join(filename);
        if path.is_file() {
            return Some(path);
        }
    }
    None
}

fn load_config_path(path: PathBuf, source: ConfigSource) -> Result<LoadedAppConfig, String> {
    let text = fs::read_to_string(&path)
        .map_err(|err| format!("Failed reading config {}: {err}", path.display()))?;

    let config: AppConfig = toml::from_str(&text)
        .map_err(|err| format!("Failed parsing config {}: {err}", path.display()))?;

    Ok(LoadedAppConfig {
        path,
        source,
        config,
    })
}

fn collect_unique_sorted<'a>(values: impl Iterator<Item = &'a str>) -> Vec<String> {
    let mut set = BTreeSet::new();
    for value in values {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            set.insert(trimmed.to_owned());
        }
    }
    set.into_iter().collect()
}

fn contains_case_insensitive(options: &[String], value: &str) -> bool {
    options
        .iter()
        .any(|option| option.eq_ignore_ascii_case(value.trim()))
}

fn trimmed_non_empty(value: Option<&str>) -> Option<String> {
    let value = value?.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_owned())
    }
}

fn parse_frame_direction(raw: &str) -> Option<FrameDirection> {
    match normalize_token(raw).as_str() {
        "lefttoright" | "ltr" => Some(FrameDirection::LeftToRight),
        "righttoleft" | "rtl" => Some(FrameDirection::RightToLeft),
        "toptobottom" | "ttb" | "downtop" | "down" => Some(FrameDirection::TopToBottom),
        "bottomtotop" | "btt" | "up" => Some(FrameDirection::BottomToTop),
        _ => None,
    }
}

fn parse_export_format(raw: &str) -> Option<ExportImageFormat> {
    match normalize_token(raw).as_str() {
        "preserve" | "preservesource" => Some(ExportImageFormat::PreserveSource),
        "jpeg" | "jpg" => Some(ExportImageFormat::Jpeg),
        "png" => Some(ExportImageFormat::Png),
        "tiff" | "tif" => Some(ExportImageFormat::Tiff),
        _ => None,
    }
}

fn parse_output_bit_depth(raw: &str) -> Option<OutputBitDepth> {
    match normalize_token(raw).as_str() {
        "preserve" | "preservesource" => Some(OutputBitDepth::PreserveSource),
        "convert8bit" | "8bit" | "8" => Some(OutputBitDepth::Convert8Bit),
        _ => None,
    }
}

fn normalize_token(raw: &str) -> String {
    raw.chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .map(|ch| ch.to_ascii_lowercase())
        .collect()
}
