use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::Duration;

use eframe::egui::{
    self, Color32, ColorImage, ComboBox, Pos2, Rect, Sense, Stroke, StrokeKind, TextureHandle,
    TextureOptions, Vec2,
};
use image::GenericImageView;

use crate::config::{AppConfig, load_optional_app_config};
use crate::constants::MAX_FRAMES_PER_STRIP;
use crate::geometry::{
    clamp_point_to_image, frame_quads_for_strip, gap_boundaries_along, guess_frame_count_for_strip,
    screen_to_source, source_point_to_screen, split_and_orth_axes, strip_color, strip_quad,
};
use crate::io_utils::{collect_supported_files, downscale_for_preview, probe_image_dimensions};
use crate::model::{
    ExportImageFormat, ExportSettings, FrameDirection, ImageSettings, InteractionMode,
    OutputBitDepth, PointPx, StripSettings,
};
use crate::processing::{configured_frame_count, process_image_file};

const GAP_OFFSET_MIN: f32 = -2.0;
const GAP_OFFSET_MAX: f32 = 2.0;
const GAP_DRAG_HIT_RADIUS_PX: f32 = 10.0;
const CORNER_DRAG_HIT_RADIUS_PX: f32 = 10.0;

struct LoadedPreview {
    path: PathBuf,
    texture: TextureHandle,
    source_size: [usize; 2],
    preview_size: [usize; 2],
}

#[derive(Clone)]
struct CachedPreview {
    source_size: [usize; 2],
    preview_size: [usize; 2],
    rgba: Vec<u8>,
}

type PreviewPayload = ([usize; 2], [usize; 2], Vec<u8>);

#[derive(Clone, Copy, PartialEq, Eq)]
enum Screen {
    Editor,
    Export,
}

struct TaskProgress {
    label: String,
    fraction: f32,
}

struct TaskState<T> {
    progress: Option<TaskProgress>,
    receiver: Option<Receiver<T>>,
}

impl<T> Default for TaskState<T> {
    fn default() -> Self {
        Self {
            progress: None,
            receiver: None,
        }
    }
}

struct ScanDateTimeEditor {
    enabled: bool,
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
}

impl Default for ScanDateTimeEditor {
    fn default() -> Self {
        Self {
            enabled: false,
            year: 2026,
            month: 1,
            day: 1,
            hour: 0,
            minute: 0,
            second: 0,
        }
    }
}

enum PreviewWorkerMessage {
    Progress {
        request_id: u64,
        label: String,
        fraction: f32,
    },
    Ready {
        request_id: u64,
        path: PathBuf,
        source_size: [usize; 2],
        preview_size: [usize; 2],
        rgba: Vec<u8>,
    },
    Failed {
        request_id: u64,
        path: PathBuf,
        error: String,
        source_size: Option<[usize; 2]>,
    },
}

enum PreviewPreloadMessage {
    Cached {
        path: PathBuf,
        source_size: [usize; 2],
        preview_size: [usize; 2],
        rgba: Vec<u8>,
        completed: usize,
        total: usize,
    },
    Failed {
        path: PathBuf,
        error: String,
        completed: usize,
        total: usize,
    },
    Finished,
}

enum ExportWorkerMessage {
    Progress {
        exported_frames: usize,
        total_frames: usize,
        processed_files: usize,
        total_files: usize,
        skipped_files: usize,
    },
    Finished {
        exported_frames: usize,
        skipped_files: usize,
        failures: Vec<String>,
        output_folder: PathBuf,
    },
}

#[derive(Clone, Copy)]
struct GapDragState {
    strip_index: usize,
    gap_index: usize,
}

#[derive(Clone, Copy)]
struct CornerDragState {
    strip_index: usize,
    corner_index: usize,
}

pub struct ScanDividerApp {
    input_folder: Option<PathBuf>,
    output_folder: Option<PathBuf>,
    image_files: Vec<PathBuf>,
    excluded_files: HashSet<PathBuf>,
    selected_index: Option<usize>,
    preview: Option<LoadedPreview>,
    preview_cache: HashMap<PathBuf, CachedPreview>,
    settings_by_image: HashMap<PathBuf, ImageSettings>,
    app_config: AppConfig,
    export_settings: ExportSettings,
    interaction_mode: InteractionMode,
    screen: Screen,
    preview_request_id: u64,
    preview_task: TaskState<PreviewWorkerMessage>,
    preview_preload_task: TaskState<PreviewPreloadMessage>,
    export_task: TaskState<ExportWorkerMessage>,
    active_corner_drag: Option<CornerDragState>,
    active_gap_drag: Option<GapDragState>,
    scan_datetime: ScanDateTimeEditor,
    zoom: f32,
    preview_scroll: Vec2,
    show_controls_help: bool,
    config_notice: Option<String>,
    status_message: String,
}

impl ScanDividerApp {
    pub fn new(_cc: &eframe::CreationContext<'_>) -> Self {
        let (app_config, config_status, config_notice) = match load_optional_app_config() {
            Ok(Some(loaded)) => {
                let warnings = loaded.config.validation_warnings();
                let notice = if warnings.is_empty() {
                    None
                } else {
                    Some(format!("Config warnings: {}", warnings.join(" | ")))
                };
                eprintln!(
                    "[perfora] Loaded config from {} ({})",
                    loaded.path.display(),
                    loaded.source.label()
                );
                (
                    loaded.config,
                    Some(format!(
                        "Loaded config from {} ({}).",
                        loaded.path.display(),
                        loaded.source.label()
                    )),
                    notice,
                )
            }
            Ok(None) => {
                eprintln!(
                    "[perfora] No config found. Search order: PERFORA_CONFIG, binary dir, cwd."
                );
                (AppConfig::default(), None, None)
            }
            Err(err) => {
                eprintln!("[perfora] Config error: {err}");
                (
                    AppConfig::default(),
                    Some(format!(
                        "Config error: {err}. Continuing with built-in defaults."
                    )),
                    None,
                )
            }
        };

        let mut app = Self {
            input_folder: None,
            output_folder: None,
            image_files: Vec::new(),
            excluded_files: HashSet::new(),
            selected_index: None,
            preview: None,
            preview_cache: HashMap::new(),
            settings_by_image: HashMap::new(),
            app_config,
            export_settings: ExportSettings::default(),
            interaction_mode: InteractionMode::None,
            screen: Screen::Editor,
            preview_request_id: 0,
            preview_task: TaskState::default(),
            preview_preload_task: TaskState::default(),
            export_task: TaskState::default(),
            active_corner_drag: None,
            active_gap_drag: None,
            scan_datetime: ScanDateTimeEditor::default(),
            zoom: 1.0,
            preview_scroll: Vec2::ZERO,
            show_controls_help: false,
            config_notice,
            status_message: "Select an input folder to begin.".to_owned(),
        };
        app.apply_config_defaults_to_export_settings();
        app.sync_scan_datetime_picker_from_metadata();
        if let Some(config_status) = config_status {
            app.status_message = format!("{} {config_status}", app.status_message);
        }
        app
    }

    fn current_image_path(&self) -> Option<&PathBuf> {
        self.selected_index
            .and_then(|idx| self.image_files.get(idx))
    }

    fn current_image_settings(&self) -> Option<&ImageSettings> {
        let path = self.current_image_path()?;
        self.settings_by_image.get(path)
    }

    fn ensure_settings(&mut self, path: &Path, source_w: usize, source_h: usize) {
        if self.settings_by_image.contains_key(path) {
            return;
        }

        let mut settings = ImageSettings::new_default(source_w as f32, source_h as f32);
        if let Some(strip_count) = self.app_config.default_strip_count() {
            settings.strip_count = strip_count;
        }
        if let Some(direction) = self.app_config.default_frame_direction() {
            for strip in &mut settings.strips {
                strip.direction = direction;
            }
        }
        for strip in &mut settings.strips {
            strip.ensure_gap_offsets_len();
        }

        self.settings_by_image.insert(path.to_path_buf(), settings);
    }

    fn apply_config_defaults_to_export_settings(&mut self) {
        if let Some(format) = self.app_config.default_export_format() {
            self.export_settings.format = format;
        }
        if let Some(bit_depth) = self.app_config.default_bit_depth() {
            self.export_settings.bit_depth = bit_depth;
        }
        if let Some(make) = self.app_config.default_camera_make() {
            self.export_settings.metadata.camera_make = make;
        }
        if let Some(model) = self.app_config.default_camera_model() {
            self.export_settings.metadata.camera_model = model;
        }
        if let Some(film) = self.app_config.default_film_stock() {
            self.export_settings.metadata.film_stock = film;
        }
        if let Some(author) = self.app_config.default_author() {
            self.export_settings.metadata.author = author;
        }
    }

    fn sync_scan_datetime_picker_from_metadata(&mut self) {
        let raw = self.export_settings.metadata.scan_datetime.trim();
        if raw.is_empty() {
            self.scan_datetime.enabled = false;
            return;
        }

        if let Some((year, month, day, hour, minute, second)) = parse_exif_scan_datetime(raw) {
            self.scan_datetime.year = year;
            self.scan_datetime.month = month;
            self.scan_datetime.day = day;
            self.scan_datetime.hour = hour;
            self.scan_datetime.minute = minute;
            self.scan_datetime.second = second;
            self.scan_datetime.enabled = true;
        } else {
            self.scan_datetime.enabled = false;
            self.export_settings.metadata.scan_datetime.clear();
        }
    }

    fn sync_scan_datetime_metadata_from_picker(&mut self) {
        if !self.scan_datetime.enabled {
            self.export_settings.metadata.scan_datetime.clear();
            return;
        }

        self.scan_datetime.month = self.scan_datetime.month.clamp(1, 12);
        let max_day = days_in_month(self.scan_datetime.year, self.scan_datetime.month);
        self.scan_datetime.day = self.scan_datetime.day.clamp(1, max_day);
        self.scan_datetime.hour = self.scan_datetime.hour.min(23);
        self.scan_datetime.minute = self.scan_datetime.minute.min(59);
        self.scan_datetime.second = self.scan_datetime.second.min(59);

        self.export_settings.metadata.scan_datetime = format_exif_scan_datetime(
            self.scan_datetime.year,
            self.scan_datetime.month,
            self.scan_datetime.day,
            self.scan_datetime.hour,
            self.scan_datetime.minute,
            self.scan_datetime.second,
        );
    }

    fn pick_input_folder(&mut self, ctx: &egui::Context) {
        if let Some(folder) = rfd::FileDialog::new().pick_folder() {
            self.load_folder(folder, ctx);
        }
    }

    fn pick_output_folder(&mut self) {
        if let Some(folder) = rfd::FileDialog::new().pick_folder() {
            self.output_folder = Some(folder.clone());
            self.status_message = format!("Output folder set to {}", folder.display());
        }
    }

    fn load_folder(&mut self, folder: PathBuf, ctx: &egui::Context) {
        self.input_folder = Some(folder.clone());
        self.image_files.clear();
        self.selected_index = None;
        self.preview = None;
        self.preview_cache.clear();
        self.preview_request_id = self.preview_request_id.saturating_add(1);
        self.preview_task.progress = None;
        self.preview_task.receiver = None;
        self.preview_preload_task.progress = None;
        self.preview_preload_task.receiver = None;
        self.zoom = 1.0;
        self.preview_scroll = Vec2::ZERO;
        self.interaction_mode = InteractionMode::None;
        self.active_corner_drag = None;
        self.active_gap_drag = None;

        match collect_supported_files(&folder) {
            Ok(files) => {
                self.image_files = files;
                let available: HashSet<PathBuf> = self.image_files.iter().cloned().collect();
                self.excluded_files.retain(|path| available.contains(path));

                if self.image_files.is_empty() {
                    self.status_message = format!(
                        "No supported images found in {} (.tif/.tiff/.png/.jpg/.jpeg)",
                        folder.display()
                    );
                } else {
                    self.selected_index = Some(0);
                    if let Err(err) = self.start_preview_load(ctx) {
                        self.status_message = err;
                    }
                    self.start_preview_preload();
                }
            }
            Err(err) => {
                self.status_message = err;
            }
        }
    }

    fn refresh_folder(&mut self, ctx: &egui::Context) {
        if let Some(folder) = self.input_folder.clone() {
            let previous_selected = self.current_image_path().cloned();

            // Cancel in-flight preview work before rebuilding the file list.
            self.preview_request_id = self.preview_request_id.saturating_add(1);
            self.preview_task.progress = None;
            self.preview_task.receiver = None;
            self.preview_preload_task.progress = None;
            self.preview_preload_task.receiver = None;
            self.interaction_mode = InteractionMode::None;
            self.active_corner_drag = None;
            self.active_gap_drag = None;

            match collect_supported_files(&folder) {
                Ok(files) => {
                    self.image_files = files;
                    let available: HashSet<PathBuf> = self.image_files.iter().cloned().collect();

                    // Keep state only for files still present after refresh.
                    self.preview_cache
                        .retain(|path, _| available.contains(path));
                    self.settings_by_image
                        .retain(|path, _| available.contains(path));
                    self.excluded_files.retain(|path| available.contains(path));

                    if self.image_files.is_empty() {
                        self.selected_index = None;
                        self.preview = None;
                        self.status_message = format!(
                            "No supported images found in {} (.tif/.tiff/.png/.jpg/.jpeg)",
                            folder.display()
                        );
                        return;
                    }

                    let selected_path = previous_selected
                        .as_ref()
                        .filter(|path| available.contains(*path))
                        .cloned()
                        .unwrap_or_else(|| self.image_files[0].clone());
                    let selection_changed = previous_selected.as_ref() != Some(&selected_path);

                    self.selected_index = self.image_files.iter().position(|p| p == &selected_path);
                    if selection_changed {
                        self.zoom = 1.0;
                        self.preview_scroll = Vec2::ZERO;
                    }

                    let selected_preview_loaded = self
                        .preview
                        .as_ref()
                        .is_some_and(|preview| preview.path == selected_path);

                    if selected_preview_loaded {
                        self.status_message = format!(
                            "Refreshed {} file(s). Cached previews/settings were preserved.",
                            self.image_files.len()
                        );
                    } else if let Some(cached) = self.preview_cache.get(&selected_path).cloned() {
                        self.apply_cached_preview(
                            selected_path,
                            cached,
                            ctx,
                            "Folder refreshed. Reused cached preview.",
                        );
                    } else if let Err(err) = self.start_preview_load(ctx) {
                        self.status_message = err;
                    }

                    self.start_preview_preload();
                }
                Err(err) => {
                    self.status_message = err;
                }
            }
        }
    }

    fn select_file(&mut self, index: usize, ctx: &egui::Context) {
        if self.selected_index == Some(index) {
            return;
        }

        self.selected_index = Some(index);
        self.zoom = 1.0;
        self.preview_scroll = Vec2::ZERO;
        self.interaction_mode = InteractionMode::None;
        self.active_corner_drag = None;
        self.active_gap_drag = None;

        if let Err(err) = self.start_preview_load(ctx) {
            self.status_message = err;
        }
    }

    fn apply_cached_preview(
        &mut self,
        path: PathBuf,
        cached: CachedPreview,
        ctx: &egui::Context,
        status: &str,
    ) {
        let mut rgba = cached.rgba.clone();
        if self.export_settings.invert_colors {
            invert_rgba_bytes_in_place(&mut rgba);
        }

        let color = ColorImage::from_rgba_unmultiplied(cached.preview_size, &rgba);
        let texture_name = format!("preview:{}", path.display());
        let texture = ctx.load_texture(texture_name, color, TextureOptions::LINEAR);
        self.preview = Some(LoadedPreview {
            path: path.clone(),
            texture,
            source_size: cached.source_size,
            preview_size: cached.preview_size,
        });
        self.ensure_settings(&path, cached.source_size[0], cached.source_size[1]);
        self.preview_task.progress = None;
        self.status_message = status.to_owned();
        ctx.request_repaint();
    }

    fn rebuild_current_preview_from_cache(&mut self, ctx: &egui::Context) -> bool {
        let Some(path) = self.current_image_path().cloned() else {
            return false;
        };
        let Some(cached) = self.preview_cache.get(&path).cloned() else {
            return false;
        };

        self.apply_cached_preview(path, cached, ctx, "Preview updated.");
        true
    }

    fn start_preview_preload(&mut self) {
        if self.image_files.is_empty() {
            return;
        }

        let selected = self.current_image_path().cloned();
        let files: Vec<PathBuf> = self
            .image_files
            .iter()
            .filter(|path| {
                selected.as_ref() != Some(*path) && !self.preview_cache.contains_key(*path)
            })
            .cloned()
            .collect();
        let total = files.len();
        if total == 0 {
            return;
        }
        let (tx, rx) = mpsc::channel::<PreviewPreloadMessage>();
        self.preview_preload_task.receiver = Some(rx);
        self.preview_preload_task.progress = Some(TaskProgress {
            label: format!("Preloading previews: 0/{total}"),
            fraction: 0.0,
        });

        thread::spawn(move || {
            for (idx, path) in files.into_iter().enumerate() {
                let completed = idx + 1;
                match load_preview_payload(&path) {
                    Ok((source_size, preview_size, rgba)) => {
                        if tx
                            .send(PreviewPreloadMessage::Cached {
                                path,
                                source_size,
                                preview_size,
                                rgba,
                                completed,
                                total,
                            })
                            .is_err()
                        {
                            return;
                        }
                    }
                    Err(error) => {
                        if tx
                            .send(PreviewPreloadMessage::Failed {
                                path,
                                error,
                                completed,
                                total,
                            })
                            .is_err()
                        {
                            return;
                        }
                    }
                }
            }

            let _ = tx.send(PreviewPreloadMessage::Finished);
        });
    }

    fn start_preview_load(&mut self, ctx: &egui::Context) -> Result<(), String> {
        let Some(index) = self.selected_index else {
            return Err("No file selected.".to_owned());
        };

        let Some(path) = self.image_files.get(index).cloned() else {
            return Err("Selected file index is out of range.".to_owned());
        };

        if let Some(cached) = self.preview_cache.get(&path).cloned() {
            self.apply_cached_preview(path, cached, ctx, "Preview loaded.");
            return Ok(());
        }

        self.preview = None;
        self.preview_request_id = self.preview_request_id.saturating_add(1);
        let request_id = self.preview_request_id;
        let (tx, rx) = mpsc::channel::<PreviewWorkerMessage>();
        self.preview_task.receiver = Some(rx);
        self.preview_task.progress = Some(TaskProgress {
            label: format!(
                "Loading preview: {}",
                path.file_name().and_then(|s| s.to_str()).unwrap_or("image")
            ),
            fraction: 0.0,
        });
        self.status_message = format!("Loading preview for {}...", path.display());

        thread::spawn(move || {
            let _ = tx.send(PreviewWorkerMessage::Progress {
                request_id,
                label: "Opening image".to_owned(),
                fraction: 0.05,
            });

            let _ = tx.send(PreviewWorkerMessage::Progress {
                request_id,
                label: "Decoding image".to_owned(),
                fraction: 0.35,
            });

            let _ = tx.send(PreviewWorkerMessage::Progress {
                request_id,
                label: "Building preview".to_owned(),
                fraction: 0.7,
            });

            match load_preview_payload(&path) {
                Ok((source_size, preview_size, rgba)) => {
                    let _ = tx.send(PreviewWorkerMessage::Ready {
                        request_id,
                        path: path.clone(),
                        source_size,
                        preview_size,
                        rgba,
                    });
                }
                Err(err) => {
                    let source_size = probe_image_dimensions(&path)
                        .ok()
                        .map(|(w, h)| [w as usize, h as usize]);
                    let _ = tx.send(PreviewWorkerMessage::Failed {
                        request_id,
                        path,
                        error: err,
                        source_size,
                    });
                }
            }
        });

        ctx.request_repaint();
        Ok(())
    }

    fn draw_top_bar(&mut self, ui: &mut egui::Ui, ctx: &egui::Context) {
        ui.horizontal_wrapped(|ui| {
            if ui.button("Select Input Folder").clicked() {
                self.pick_input_folder(ctx);
            }

            if ui.button("Select Output Folder").clicked() {
                self.pick_output_folder();
            }
            ui.separator();
            match self.screen {
                Screen::Editor => {
                    if ui
                        .add_enabled(
                            !self.image_files.is_empty(),
                            egui::Button::new("Open Export Screen"),
                        )
                        .clicked()
                    {
                        self.screen = Screen::Export;
                    }
                }
                Screen::Export => {
                    if ui.button("Back To Editor").clicked() {
                        self.screen = Screen::Editor;
                    }
                }
            }
            if ui.button("Controls").clicked() {
                self.show_controls_help = true;
            }

            ui.separator();

            if let Some(folder) = &self.input_folder {
                ui.label(format!("Input: {}", folder.display()));
            } else {
                ui.label("Input: <none>");
            }

            let output = self
                .output_folder
                .clone()
                .or_else(|| self.input_folder.as_ref().map(|input| input.join("output")));
            if let Some(folder) = output {
                ui.label(format!("Output: {}", folder.display()));
            }

            if !self.image_files.is_empty() {
                ui.label(format!("Files: {}", self.image_files.len()));
            }
        });

        ui.separator();

        ui.horizontal_wrapped(|ui| {
            ui.checkbox(&mut self.export_settings.mirror, "Mirror");
            let invert_changed = ui
                .checkbox(&mut self.export_settings.invert_colors, "Invert Colors")
                .changed();

            if invert_changed
                && self.selected_index.is_some()
                && !self.rebuild_current_preview_from_cache(ctx)
                && let Err(err) = self.start_preview_load(ctx)
            {
                self.status_message = err;
            }
        });

        if let Some(config_notice) = self.config_notice.clone() {
            ui.separator();
            ui.horizontal_wrapped(|ui| {
                ui.colored_label(Color32::YELLOW, config_notice);
                if ui.small_button("Dismiss").clicked() {
                    self.config_notice = None;
                }
            });
        }
    }

    fn handle_editor_shortcuts(&mut self, ctx: &egui::Context) {
        if !matches!(self.screen, Screen::Editor) {
            return;
        }

        if ctx.input(|i| i.key_pressed(egui::Key::Escape))
            && !matches!(self.interaction_mode, InteractionMode::None)
        {
            self.interaction_mode = InteractionMode::None;
            self.active_corner_drag = None;
            self.active_gap_drag = None;
            self.status_message = "Cancelled current tool.".to_owned();
            return;
        }

        if ctx.wants_keyboard_input() {
            return;
        }

        let shortcut = ctx.input(|i| {
            if i.key_pressed(egui::Key::Num1) {
                Some((0usize, 0usize))
            } else if i.key_pressed(egui::Key::Num2) {
                Some((0, 1))
            } else if i.key_pressed(egui::Key::Num3) {
                Some((0, 2))
            } else if i.key_pressed(egui::Key::Num4) {
                Some((1, 0))
            } else if i.key_pressed(egui::Key::Num5) {
                Some((1, 1))
            } else if i.key_pressed(egui::Key::Num6) {
                Some((1, 2))
            } else {
                None
            }
        });

        let Some((strip_index, corner_index)) = shortcut else {
            return;
        };

        let Some(path) = self.current_image_path().cloned() else {
            return;
        };

        if !self.settings_by_image.contains_key(&path)
            && let Some(preview) = &self.preview
            && preview.path == path
        {
            self.ensure_settings(&path, preview.source_size[0], preview.source_size[1]);
        }

        let strip_count = self
            .settings_by_image
            .get(&path)
            .map(|settings| settings.strip_count)
            .unwrap_or(0);

        if strip_index >= strip_count {
            self.status_message = format!(
                "Shortcut {} needs Strip {}. Set strip count to at least {} first.",
                corner_shortcut_label(strip_index, corner_index),
                strip_index + 1,
                strip_index + 1
            );
            return;
        }

        self.interaction_mode = InteractionMode::PickStripCorner {
            strip_index,
            corner_index,
        };
        self.active_corner_drag = None;
        self.active_gap_drag = None;
        self.status_message = format!(
            "Shortcut {} armed. Click preview to set Strip {} corner {}.",
            corner_shortcut_label(strip_index, corner_index),
            strip_index + 1,
            corner_index + 1
        );
    }

    fn draw_controls_help_window(&mut self, ctx: &egui::Context) {
        if !self.show_controls_help {
            return;
        }

        egui::Window::new("Controls")
            .open(&mut self.show_controls_help)
            .resizable(true)
            .default_width(430.0)
            .show(ctx, |ui| {
                ui.heading("Mouse");
                ui.label("Left drag on corner points: move corners directly.");
                ui.label("Left drag on gap boundaries: adjust gap position.");
                ui.label("Left click (while a pick tool is armed): set that corner.");
                ui.label("Mouse wheel: vertical pan.");
                ui.label("Shift + wheel: horizontal pan.");
                ui.label("Ctrl + wheel: zoom.");
                ui.label("Middle-button drag: pan preview.");
                ui.label("Hold left mouse: show peeker lens.");

                ui.separator();
                ui.heading("Keyboard");
                ui.label("1 2 3: arm Strip 1 corners 1 2 3.");
                ui.label("4 5 6: arm Strip 2 corners 1 2 3.");
                ui.label("Esc: cancel current tool.");
            });
    }

    fn draw_file_list_panel(&mut self, ui: &mut egui::Ui, ctx: &egui::Context) {
        ui.horizontal(|ui| {
            ui.heading("Scans");
            let can_refresh = self.input_folder.is_some();
            if ui
                .add_enabled(can_refresh, egui::Button::new("Refresh"))
                .clicked()
            {
                self.refresh_folder(ctx);
            }
        });
        ui.separator();
        if let Some(progress) = &self.preview_preload_task.progress {
            ui.add(
                egui::ProgressBar::new(progress.fraction.clamp(0.0, 1.0))
                    .show_percentage()
                    .text(&progress.label),
            );
            ui.separator();
        }

        egui::ScrollArea::vertical().show(ui, |ui| {
            let mut pending_select = None;
            let mut pending_include_updates = Vec::new();

            for (idx, path) in self.image_files.iter().enumerate() {
                let name = path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("<unknown>");
                let selected = self.selected_index == Some(idx);
                let mut include = !self.excluded_files.contains(path);

                ui.horizontal(|ui| {
                    if ui
                        .checkbox(&mut include, "")
                        .on_hover_text("Include in export")
                        .changed()
                    {
                        pending_include_updates.push((path.clone(), include));
                    }

                    let label = if include {
                        name.to_owned()
                    } else {
                        format!("{name} [excluded]")
                    };
                    if ui.selectable_label(selected, label).clicked() {
                        pending_select = Some(idx);
                    }
                });
            }

            for (path, include) in pending_include_updates {
                if include {
                    self.excluded_files.remove(&path);
                } else {
                    self.excluded_files.insert(path);
                }
            }

            if let Some(idx) = pending_select {
                self.select_file(idx, ctx);
            }
        });
    }

    fn draw_settings_panel(&mut self, ui: &mut egui::Ui) {
        ui.heading("Per-Image Settings");
        ui.separator();

        ui.label(format!("Mode: {}", self.interaction_mode.label()));

        if !matches!(self.interaction_mode, InteractionMode::None)
            && ui.button("Cancel Current Tool").clicked()
        {
            self.interaction_mode = InteractionMode::None;
            self.active_corner_drag = None;
            self.active_gap_drag = None;
        }

        ui.separator();

        let Some(path) = self.current_image_path().cloned() else {
            ui.label("Select a file to edit settings.");
            return;
        };

        let mut include_current = !self.excluded_files.contains(&path);
        if ui
            .checkbox(&mut include_current, "Include This Image In Export")
            .changed()
        {
            if include_current {
                self.excluded_files.remove(&path);
            } else {
                self.excluded_files.insert(path.clone());
            }
        }

        let source_size = self
            .preview
            .as_ref()
            .map(|p| p.source_size)
            .unwrap_or([0, 0]);
        self.ensure_settings(&path, source_size[0], source_size[1]);

        let mut requested_mode = None;

        let Some(settings) = self.settings_by_image.get_mut(&path) else {
            ui.label("Unable to load settings.");
            return;
        };

        ui.horizontal(|ui| {
            ui.label("Strip count");
            ui.selectable_value(&mut settings.strip_count, 1, "1");
            ui.selectable_value(&mut settings.strip_count, 2, "2");
        });

        for strip_index in 0..settings.strip_count {
            let strip = &mut settings.strips[strip_index];
            strip.ensure_gap_offsets_len();
            ui.separator();
            ui.collapsing(format!("Strip {}", strip_index + 1), |ui| {
                ui.label("Strip shape from 3 corners:");
                ui.label("Corner 1 = origin, Corner 2 = along strip, Corner 3 = strip width.");

                ui.horizontal(|ui| {
                    if ui.button("Pick Corner 1").clicked() {
                        requested_mode = Some(InteractionMode::PickStripCorner {
                            strip_index,
                            corner_index: 0,
                        });
                    }
                    if ui.button("Pick Corner 2").clicked() {
                        requested_mode = Some(InteractionMode::PickStripCorner {
                            strip_index,
                            corner_index: 1,
                        });
                    }
                    if ui.button("Pick Corner 3").clicked() {
                        requested_mode = Some(InteractionMode::PickStripCorner {
                            strip_index,
                            corner_index: 2,
                        });
                    }
                    if ui.small_button("Clear Corners").clicked() {
                        strip.clear_corners();
                    }
                });

                draw_corner_editor(ui, "Corner 1", &mut strip.corner_origin, source_size);
                draw_corner_editor(ui, "Corner 2", &mut strip.corner_along, source_size);
                draw_corner_editor(ui, "Corner 3", &mut strip.corner_across, source_size);

                ui.horizontal(|ui| {
                    ui.label("Frames");
                    ui.add(
                        egui::DragValue::new(&mut strip.frame_count)
                            .range(1..=MAX_FRAMES_PER_STRIP),
                    );

                    let guessed = guess_frame_count_for_strip(strip, MAX_FRAMES_PER_STRIP);
                    if ui.button("Guess Frames").clicked()
                        && let Some(guess) = guessed
                    {
                        strip.frame_count = guess;
                    }
                });

                ui.horizontal(|ui| {
                    ui.label("Gap % of frame size");
                    ui.add(
                        egui::DragValue::new(&mut strip.gap_percent)
                            .range(0.0..=100.0)
                            .speed(0.1),
                    );
                });

                ComboBox::from_id_salt(format!("direction_{strip_index}"))
                    .selected_text(strip.direction.label())
                    .show_ui(ui, |ui| {
                        for direction in FrameDirection::ALL {
                            ui.selectable_value(&mut strip.direction, direction, direction.label());
                        }
                    });

                ui.separator();
                if strip.gap_offsets.is_empty() {
                    ui.label("No gaps to adjust (frame count = 1).");
                } else {
                    ui.label(
                        "Drag gap boundary lines directly in the preview to adjust positions.",
                    );
                    if ui.small_button("Reset Gap Adjustments").clicked() {
                        for offset in &mut strip.gap_offsets {
                            *offset = 0.0;
                        }
                    }
                }
            });
        }

        if let Some(mode) = requested_mode {
            self.interaction_mode = mode;
            self.active_corner_drag = None;
            self.active_gap_drag = None;
        }
    }

    fn draw_preview_panel(&mut self, ui: &mut egui::Ui) {
        let Some(preview) = &self.preview else {
            if let Some(progress) = &self.preview_task.progress {
                ui.label(format!("Loading preview... {}", progress.label));
            } else {
                ui.label("No preview available.");
            }
            return;
        };

        let preview_path = preview.path.clone();
        let preview_texture = preview.texture.clone();
        let preview_size = preview.preview_size;
        let source_size = preview.source_size;
        let settings_snapshot = self.current_image_settings().cloned();

        ui.horizontal_wrapped(|ui| {
            let file = preview_path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("<unknown>");
            ui.label(file);
            ui.separator();
            ui.label(format!("Source: {}x{}", source_size[0], source_size[1]));
            ui.separator();
            ui.label(format!("Preview: {}x{}", preview_size[0], preview_size[1]));
        });

        ui.add(
            egui::Slider::new(&mut self.zoom, 0.1..=8.0)
                .logarithmic(true)
                .text("Zoom"),
        );

        ui.separator();

        let base_size = texture_size_vec2(&preview_texture);
        let scaled_size = base_size * self.zoom.max(0.1);

        let scroll_output = egui::ScrollArea::both()
            .id_salt("preview_scroll")
            .scroll_offset(self.preview_scroll)
            .enable_scrolling(false)
            .show(ui, |ui| {
                let viewport_size = ui.clip_rect().size();
                let pad_x = ((viewport_size.x - scaled_size.x).max(0.0)) * 0.5;
                let pad_y = ((viewport_size.y - scaled_size.y).max(0.0)) * 0.5;

                ui.add_space(pad_y);
                let image_rect = ui
                    .horizontal(|ui| {
                        ui.add_space(pad_x);
                        let image_widget = egui::Image::new(&preview_texture)
                            .fit_to_exact_size(scaled_size)
                            .sense(Sense::click_and_drag());

                        let response = ui.add(image_widget);
                        self.handle_preview_interaction(&response, response.rect, source_size);
                        self.paint_overlay(
                            ui,
                            response.rect,
                            source_size,
                            settings_snapshot.as_ref(),
                        );
                        self.paint_peeker(ui, &preview_texture, response.rect);
                        ui.add_space(pad_x);
                        response.rect
                    })
                    .inner;
                ui.add_space(pad_y);
                image_rect
            });

        self.preview_scroll = self.clamp_preview_scroll(
            scroll_output.state.offset,
            &preview_texture,
            scroll_output.inner_rect,
        );
        self.apply_scroll_and_zoom_input(ui.ctx(), &preview_texture, scroll_output.inner_rect);
        self.apply_middle_pan(ui.ctx(), &preview_texture, scroll_output.inner_rect);

        let max_offsets = self.max_preview_offsets(&preview_texture, scroll_output.inner_rect);
        let mut scroll_x = self.preview_scroll.x.clamp(0.0, max_offsets.x.max(0.0));
        ui.add_enabled_ui(max_offsets.x > 0.0, |ui| {
            let slider = egui::Slider::new(&mut scroll_x, 0.0..=max_offsets.x.max(0.0))
                .show_value(false)
                .text("Horizontal");
            ui.add_sized([ui.available_width(), 18.0], slider);
        });
        if max_offsets.x > 0.0 && (scroll_x - self.preview_scroll.x).abs() > f32::EPSILON {
            self.preview_scroll.x = scroll_x;
            ui.ctx().request_repaint();
        }
    }

    fn draw_export_screen(&mut self, ui: &mut egui::Ui) {
        ui.heading("Export");
        ui.separator();

        let input_files = self.image_files.len();
        let excluded = self.excluded_files.len().min(input_files);
        let included = input_files.saturating_sub(excluded);

        ui.label(format!(
            "Input files: {input_files} (included: {included}, excluded: {excluded})"
        ));

        let output_folder = self
            .output_folder
            .clone()
            .or_else(|| self.input_folder.as_ref().map(|input| input.join("output")));

        if let Some(folder) = output_folder {
            ui.label(format!("Output folder: {}", folder.display()));
            ui.horizontal_wrapped(|ui| {
                if ui.button("Select Output Folder").clicked() {
                    self.pick_output_folder();
                }
                if ui.button("Open Output Folder").clicked()
                    && let Err(err) = open_folder_in_file_manager(&folder)
                {
                    self.status_message = err;
                }
            });
        } else {
            ui.label("Output folder: <not set>");
            if ui.button("Select Output Folder").clicked() {
                self.pick_output_folder();
            }
        }

        ui.separator();
        ui.label("Export image format");
        ComboBox::from_id_salt("export_format")
            .selected_text(self.export_settings.format.label())
            .show_ui(ui, |ui| {
                for format in ExportImageFormat::ALL {
                    ui.selectable_value(&mut self.export_settings.format, format, format.label());
                }
            });
        ui.horizontal(|ui| {
            ui.label("Output bit depth");
            ComboBox::from_id_salt("export_bit_depth_mode")
                .selected_text(self.export_settings.bit_depth.label())
                .show_ui(ui, |ui| {
                    for mode in OutputBitDepth::ALL {
                        ui.selectable_value(
                            &mut self.export_settings.bit_depth,
                            mode,
                            mode.label(),
                        );
                    }
                });
        });

        ui.separator();
        ui.label("Per-frame processing");
        ui.checkbox(
            &mut self.export_settings.auto_contrast_enabled,
            "Enable auto contrast (percentile stretch)",
        );

        ui.add_enabled_ui(self.export_settings.auto_contrast_enabled, |ui| {
            ui.horizontal_wrapped(|ui| {
                ui.label("Low percentile %");
                ui.add(
                    egui::DragValue::new(&mut self.export_settings.low_percentile)
                        .range(0.0..=49.9)
                        .speed(0.1),
                );

                ui.label("High percentile %");
                ui.add(
                    egui::DragValue::new(&mut self.export_settings.high_percentile)
                        .range(50.0..=100.0)
                        .speed(0.1),
                );
            });

            ui.horizontal_wrapped(|ui| {
                ui.label("Auto-contrast sample area %");
                ui.add(
                    egui::Slider::new(
                        &mut self.export_settings.auto_contrast_sample_area_percent,
                        1.0..=100.0,
                    )
                    .show_value(true)
                    .clamping(egui::SliderClamping::Always),
                );
            });
            ui.small("Percentile histogram uses a centered rectangle of this area.");
        });
        if self.export_settings.high_percentile <= self.export_settings.low_percentile {
            self.export_settings.high_percentile =
                (self.export_settings.low_percentile + 1.0).min(100.0);
        }
        self.export_settings.auto_contrast_sample_area_percent = self
            .export_settings
            .auto_contrast_sample_area_percent
            .clamp(1.0, 100.0);

        ui.separator();
        ui.label("Metadata (EXIF-standard field mapping where applicable)");
        let camera_make_options = self.app_config.camera_make_options();
        let camera_model_options = self
            .app_config
            .camera_model_options_for_make(&self.export_settings.metadata.camera_make);
        let film_options = self.app_config.film_options();
        let author_options = self.app_config.author_options();

        ui.horizontal(|ui| {
            ui.label("Camera Make (EXIF: Make)");
            draw_preset_text_input(
                ui,
                "camera_make_input",
                &camera_make_options,
                &mut self.export_settings.metadata.camera_make,
            );
        });
        ui.horizontal(|ui| {
            ui.label("Camera Model (EXIF: Model)");
            draw_preset_text_input(
                ui,
                "camera_model_input",
                &camera_model_options,
                &mut self.export_settings.metadata.camera_model,
            );
        });
        ui.horizontal(|ui| {
            ui.label("Author (EXIF: Artist)");
            draw_preset_text_input(
                ui,
                "author_input",
                &author_options,
                &mut self.export_settings.metadata.author,
            );
        });
        ui.horizontal(|ui| {
            ui.label("Scan Time (EXIF: DateTimeDigitized)");
            ui.checkbox(&mut self.scan_datetime.enabled, "Set");
        });
        if self.scan_datetime.enabled {
            let max_day = days_in_month(
                self.scan_datetime.year,
                self.scan_datetime.month.clamp(1, 12),
            );
            ui.horizontal_wrapped(|ui| {
                ui.label("Date");
                ui.add(
                    egui::DragValue::new(&mut self.scan_datetime.year)
                        .range(1900..=2200)
                        .speed(1.0)
                        .prefix("Y "),
                );
                ui.add(
                    egui::DragValue::new(&mut self.scan_datetime.month)
                        .range(1..=12)
                        .speed(1.0)
                        .prefix("M "),
                );
                ui.add(
                    egui::DragValue::new(&mut self.scan_datetime.day)
                        .range(1..=max_day)
                        .speed(1.0)
                        .prefix("D "),
                );
            });
            ui.horizontal_wrapped(|ui| {
                ui.label("Time");
                ui.add(
                    egui::DragValue::new(&mut self.scan_datetime.hour)
                        .range(0..=23)
                        .speed(1.0)
                        .prefix("H "),
                );
                ui.add(
                    egui::DragValue::new(&mut self.scan_datetime.minute)
                        .range(0..=59)
                        .speed(1.0)
                        .prefix("Min "),
                );
                ui.add(
                    egui::DragValue::new(&mut self.scan_datetime.second)
                        .range(0..=59)
                        .speed(1.0)
                        .prefix("S "),
                );
            });
            self.sync_scan_datetime_metadata_from_picker();
            ui.monospace(format!(
                "EXIF value: {}",
                self.export_settings.metadata.scan_datetime
            ));
        } else {
            self.export_settings.metadata.scan_datetime.clear();
        }
        ui.horizontal(|ui| {
            ui.label("Film Used");
            draw_preset_text_input(
                ui,
                "film_input",
                &film_options,
                &mut self.export_settings.metadata.film_stock,
            );
        });
        ui.horizontal(|ui| {
            ui.label("Description (EXIF: ImageDescription)");
            ui.text_edit_singleline(&mut self.export_settings.metadata.image_description);
        });
        ui.label("Notes (EXIF: UserComment)");
        ui.text_edit_multiline(&mut self.export_settings.metadata.notes);

        ui.separator();
        let export_enabled = included > 0 && self.export_task.receiver.is_none();
        if ui
            .add_enabled(export_enabled, egui::Button::new("Start Export"))
            .clicked()
        {
            self.start_export();
        }
        if self.export_task.receiver.is_some() {
            ui.label("Export is running...");
        }
    }

    fn handle_preview_interaction(
        &mut self,
        response: &egui::Response,
        image_rect: Rect,
        source_size: [usize; 2],
    ) {
        let Some(path) = self.current_image_path().cloned() else {
            return;
        };

        let source_w = source_size[0] as f32;
        let source_h = source_size[1] as f32;
        if source_w <= 1.0 || source_h <= 1.0 {
            return;
        }

        let pointer_pos = response
            .interact_pointer_pos()
            .or_else(|| response.hover_pos())
            .or_else(|| response.ctx.input(|i| i.pointer.latest_pos()));
        let primary_down = response.ctx.input(|i| i.pointer.primary_down());

        match self.interaction_mode {
            InteractionMode::None => {
                if response.drag_started()
                    && primary_down
                    && let Some(pos) = pointer_pos
                    && let Some(settings) = self.settings_by_image.get(&path)
                {
                    if let Some(hit) = find_corner_hit(settings, pos, image_rect, source_size) {
                        self.active_corner_drag = Some(hit);
                        self.active_gap_drag = None;
                        self.status_message = format!(
                            "Moving Strip {} Corner {}",
                            hit.strip_index + 1,
                            hit.corner_index + 1
                        );
                    } else if let Some(hit) =
                        find_gap_boundary_hit(settings, pos, image_rect, source_size)
                    {
                        self.active_gap_drag = Some(hit);
                        self.active_corner_drag = None;
                        self.status_message = format!(
                            "Adjusting Strip {} Gap {}",
                            hit.strip_index + 1,
                            hit.gap_index + 1
                        );
                    }
                }

                if let Some(active_drag) = self.active_corner_drag
                    && ((response.dragged() && primary_down) || response.drag_stopped())
                    && let Some(pos) = pointer_pos
                {
                    let clamped_pos = clamp_screen_pos_to_rect(pos, image_rect);
                    if let Some(source_point) =
                        screen_to_source(clamped_pos, image_rect, source_size)
                        && let Some(settings) = self.settings_by_image.get_mut(&path)
                        && active_drag.strip_index < settings.strip_count
                    {
                        let strip = &mut settings.strips[active_drag.strip_index];
                        let point = clamp_point_to_image(source_point, source_w, source_h);
                        set_strip_corner(strip, active_drag.corner_index, point);
                    }
                }

                if let Some(active_drag) = self.active_gap_drag
                    && ((response.dragged() && primary_down) || response.drag_stopped())
                    && let Some(pos) = pointer_pos
                {
                    let clamped_pos = clamp_screen_pos_to_rect(pos, image_rect);
                    if let Some(source_point) =
                        screen_to_source(clamped_pos, image_rect, source_size)
                        && let Some(settings) = self.settings_by_image.get_mut(&path)
                        && active_drag.strip_index < settings.strip_count
                    {
                        let strip = &mut settings.strips[active_drag.strip_index];
                        strip.ensure_gap_offsets_len();
                        if let Some(strip_q) = strip_quad(strip) {
                            let t =
                                source_split_t(strip_q, strip, source_point).clamp(0.001, 0.999);
                            if let Some(offset) =
                                offset_for_dragged_boundary(strip, active_drag.gap_index, t)
                                && active_drag.gap_index < strip.gap_offsets.len()
                            {
                                strip.gap_offsets[active_drag.gap_index] = offset;
                            }
                        }
                    }
                }

                if response.drag_stopped() {
                    self.active_corner_drag = None;
                    self.active_gap_drag = None;
                }
            }
            InteractionMode::PickStripCorner {
                strip_index,
                corner_index,
            } => {
                let commit = response.clicked() || response.drag_stopped();
                if commit
                    && let Some(pos) = pointer_pos
                    && let Some(source_point) = screen_to_source(
                        clamp_screen_pos_to_rect(pos, image_rect),
                        image_rect,
                        source_size,
                    )
                    && let Some(settings) = self.settings_by_image.get_mut(&path)
                    && strip_index < settings.strips.len()
                {
                    let point = clamp_point_to_image(source_point, source_w, source_h);
                    let strip = &mut settings.strips[strip_index];
                    set_strip_corner(strip, corner_index, point);
                    strip.ensure_gap_offsets_len();

                    self.status_message =
                        format!("Set Strip {} corner {}.", strip_index + 1, corner_index + 1);
                    self.interaction_mode = InteractionMode::None;
                    self.active_corner_drag = None;
                    self.active_gap_drag = None;
                }
            }
        }
    }

    fn paint_overlay(
        &self,
        ui: &mut egui::Ui,
        image_rect: Rect,
        source_size: [usize; 2],
        settings: Option<&ImageSettings>,
    ) {
        let painter = ui.painter_at(image_rect);

        if let Some(settings) = settings {
            for strip_index in 0..settings.strip_count {
                let strip = &settings.strips[strip_index];
                let color = strip_color(strip_index);

                if let Some(strip_q) = strip_quad(strip)
                    && let (Some(s0), Some(s1), Some(s2), Some(s3)) = (
                        source_point_to_screen(strip_q.p0, image_rect, source_size),
                        source_point_to_screen(strip_q.p1, image_rect, source_size),
                        source_point_to_screen(strip_q.p2, image_rect, source_size),
                        source_point_to_screen(strip_q.p3, image_rect, source_size),
                    )
                {
                    painter.line_segment([s0, s1], Stroke::new(2.0, color));
                    painter.line_segment([s1, s3], Stroke::new(2.0, color));
                    painter.line_segment([s3, s2], Stroke::new(2.0, color));
                    painter.line_segment([s2, s0], Stroke::new(2.0, color));

                    for frame_q in frame_quads_for_strip(strip) {
                        if let (Some(f0), Some(f1), Some(f2), Some(f3)) = (
                            source_point_to_screen(frame_q.p0, image_rect, source_size),
                            source_point_to_screen(frame_q.p1, image_rect, source_size),
                            source_point_to_screen(frame_q.p2, image_rect, source_size),
                            source_point_to_screen(frame_q.p3, image_rect, source_size),
                        ) {
                            let stroke = Stroke::new(1.0, color.gamma_multiply(0.65));
                            painter.line_segment([f0, f1], stroke);
                            painter.line_segment([f1, f3], stroke);
                            painter.line_segment([f3, f2], stroke);
                            painter.line_segment([f2, f0], stroke);
                        }
                    }

                    for (gap_idx, boundary_u) in gap_boundaries_along(strip).into_iter().enumerate()
                    {
                        let (line_start, line_end) =
                            gap_boundary_line_points(strip_q, strip, boundary_u);
                        if let (Some(g0), Some(g1)) = (
                            source_point_to_screen(line_start, image_rect, source_size),
                            source_point_to_screen(line_end, image_rect, source_size),
                        ) {
                            let active = self.active_gap_drag.is_some_and(|drag| {
                                drag.strip_index == strip_index && drag.gap_index == gap_idx
                            });
                            let line_color = if active {
                                Color32::WHITE
                            } else {
                                color.gamma_multiply(0.9)
                            };
                            let line_width = if active { 2.5 } else { 1.5 };
                            painter.line_segment([g0, g1], Stroke::new(line_width, line_color));

                            let handle_center = Pos2::new((g0.x + g1.x) * 0.5, (g0.y + g1.y) * 0.5);
                            painter.circle_filled(handle_center, 3.0, line_color);
                        }
                    }
                }

                if let Some(p) = strip.corner_origin
                    && let Some(screen_p) = source_point_to_screen(p, image_rect, source_size)
                {
                    let active = self.active_corner_drag.is_some_and(|drag| {
                        drag.strip_index == strip_index && drag.corner_index == 0
                    });
                    let marker_color = if active { Color32::WHITE } else { color };
                    let marker_radius = if active { 6.0 } else { 4.0 };
                    painter.circle_filled(screen_p, marker_radius, marker_color);
                    painter.text(
                        screen_p + Vec2::new(6.0, -6.0),
                        egui::Align2::LEFT_BOTTOM,
                        "1",
                        egui::TextStyle::Body.resolve(ui.style()),
                        marker_color,
                    );
                }

                if let Some(p) = strip.corner_along
                    && let Some(screen_p) = source_point_to_screen(p, image_rect, source_size)
                {
                    let active = self.active_corner_drag.is_some_and(|drag| {
                        drag.strip_index == strip_index && drag.corner_index == 1
                    });
                    let marker_color = if active { Color32::WHITE } else { color };
                    let marker_radius = if active { 6.0 } else { 4.0 };
                    painter.circle_filled(screen_p, marker_radius, marker_color);
                    painter.text(
                        screen_p + Vec2::new(6.0, -6.0),
                        egui::Align2::LEFT_BOTTOM,
                        "2",
                        egui::TextStyle::Body.resolve(ui.style()),
                        marker_color,
                    );
                }

                if let Some(p) = strip.corner_across
                    && let Some(screen_p) = source_point_to_screen(p, image_rect, source_size)
                {
                    let active = self.active_corner_drag.is_some_and(|drag| {
                        drag.strip_index == strip_index && drag.corner_index == 2
                    });
                    let marker_color = if active { Color32::WHITE } else { color };
                    let marker_radius = if active { 6.0 } else { 4.0 };
                    painter.circle_filled(screen_p, marker_radius, marker_color);
                    painter.text(
                        screen_p + Vec2::new(6.0, -6.0),
                        egui::Align2::LEFT_BOTTOM,
                        "3",
                        egui::TextStyle::Body.resolve(ui.style()),
                        marker_color,
                    );
                }
            }
        }
    }

    fn start_export(&mut self) {
        if self.image_files.is_empty() {
            self.status_message = "No files loaded.".to_owned();
            return;
        }

        if self.export_task.receiver.is_some() {
            self.status_message = "Export is already running.".to_owned();
            return;
        }

        // Release current preview texture before batch export to reduce peak memory.
        self.preview = None;
        self.preview_scroll = Vec2::ZERO;

        if self.export_settings.auto_contrast_enabled
            && self.export_settings.high_percentile <= self.export_settings.low_percentile
        {
            self.status_message = "High percentile must be greater than low percentile.".to_owned();
            return;
        }

        let Some(input_folder) = self.input_folder.clone() else {
            self.status_message = "Input folder is not set.".to_owned();
            return;
        };

        let output_folder = self
            .output_folder
            .clone()
            .unwrap_or_else(|| input_folder.join("output"));

        if let Err(err) = fs::create_dir_all(&output_folder) {
            self.status_message = format!(
                "Failed creating output folder {}: {err}",
                output_folder.display()
            );
            return;
        }

        let files = self.image_files.clone();
        let excluded_files = self.excluded_files.clone();
        let settings_by_image = self.settings_by_image.clone();
        let export_settings = self.export_settings.clone();
        let total_files = files.len();
        let total_frames: usize = files
            .iter()
            .filter(|path| !excluded_files.contains(*path) && path.is_file())
            .filter_map(|path| settings_by_image.get(path))
            .map(configured_frame_count)
            .sum();
        let (tx, rx) = mpsc::channel::<ExportWorkerMessage>();
        self.export_task.receiver = Some(rx);
        self.export_task.progress = Some(TaskProgress {
            label: format!("Exporting frame (0/{total_frames})"),
            fraction: 0.0,
        });
        self.status_message = "Export started...".to_owned();

        thread::spawn(move || {
            let mut global_index: usize = 1;
            let mut exported_frames: usize = 0;
            let mut skipped_files: usize = 0;
            let mut failures = Vec::new();
            let mut processed_files = 0usize;

            for path in files {
                if excluded_files.contains(&path) {
                    skipped_files += 1;
                    processed_files += 1;
                    let _ = tx.send(ExportWorkerMessage::Progress {
                        exported_frames,
                        total_frames,
                        processed_files,
                        total_files,
                        skipped_files,
                    });
                    continue;
                }

                if !path.is_file() {
                    skipped_files += 1;
                    failures.push(format!(
                        "{}: file is missing or no longer a regular file (refresh folder list).",
                        path.display()
                    ));
                    processed_files += 1;
                    let _ = tx.send(ExportWorkerMessage::Progress {
                        exported_frames,
                        total_frames,
                        processed_files,
                        total_files,
                        skipped_files,
                    });
                    continue;
                }

                let Some(settings) = settings_by_image.get(&path).cloned() else {
                    failures.push(format!("{}: no settings", path.display()));
                    processed_files += 1;
                    let _ = tx.send(ExportWorkerMessage::Progress {
                        exported_frames,
                        total_frames,
                        processed_files,
                        total_files,
                        skipped_files,
                    });
                    continue;
                };

                let mut on_frame_exported = || {
                    exported_frames += 1;
                    let _ = tx.send(ExportWorkerMessage::Progress {
                        exported_frames,
                        total_frames,
                        processed_files,
                        total_files,
                        skipped_files,
                    });
                };

                if let Err(err) = process_image_file(
                    &path,
                    &settings,
                    &export_settings,
                    &output_folder,
                    &mut global_index,
                    &mut on_frame_exported,
                ) {
                    failures.push(format!("{}: {err}", path.display()));
                }

                processed_files += 1;
                let _ = tx.send(ExportWorkerMessage::Progress {
                    exported_frames,
                    total_frames,
                    processed_files,
                    total_files,
                    skipped_files,
                });
            }

            let _ = tx.send(ExportWorkerMessage::Finished {
                exported_frames,
                skipped_files,
                failures,
                output_folder,
            });
        });
    }

    fn poll_background_tasks(&mut self, ctx: &egui::Context) {
        if let Some(rx) = self.preview_task.receiver.take() {
            let mut keep_receiver = true;
            while let Ok(msg) = rx.try_recv() {
                match msg {
                    PreviewWorkerMessage::Progress {
                        request_id,
                        label,
                        fraction,
                    } => {
                        if request_id != self.preview_request_id {
                            continue;
                        }
                        self.preview_task.progress = Some(TaskProgress {
                            label,
                            fraction: fraction.clamp(0.0, 1.0),
                        });
                        ctx.request_repaint();
                    }
                    PreviewWorkerMessage::Ready {
                        request_id,
                        path,
                        source_size,
                        preview_size,
                        rgba,
                    } => {
                        let cached = CachedPreview {
                            source_size,
                            preview_size,
                            rgba,
                        };
                        self.preview_cache.insert(path.clone(), cached.clone());

                        if request_id != self.preview_request_id {
                            continue;
                        }

                        self.apply_cached_preview(path, cached, ctx, "Preview loaded.");
                        keep_receiver = false;
                    }
                    PreviewWorkerMessage::Failed {
                        request_id,
                        path,
                        error,
                        source_size,
                    } => {
                        if request_id != self.preview_request_id {
                            continue;
                        }

                        self.preview = None;
                        if let Some([w, h]) = source_size {
                            self.ensure_settings(&path, w, h);
                            self.status_message = format!(
                                "Preview disabled for {} ({error}). Dimensions {}x{} loaded; export may still work if memory allows.",
                                path.display(),
                                w,
                                h
                            );
                        } else {
                            self.status_message =
                                format!("Failed loading preview for {}: {error}", path.display());
                        }
                        self.preview_task.progress = None;
                        keep_receiver = false;
                        ctx.request_repaint();
                    }
                }
            }

            if keep_receiver {
                self.preview_task.receiver = Some(rx);
            }
        }

        if let Some(rx) = self.preview_preload_task.receiver.take() {
            let mut keep_receiver = true;
            while let Ok(msg) = rx.try_recv() {
                match msg {
                    PreviewPreloadMessage::Cached {
                        path,
                        source_size,
                        preview_size,
                        rgba,
                        completed,
                        total,
                    } => {
                        self.preview_cache.insert(
                            path.clone(),
                            CachedPreview {
                                source_size,
                                preview_size,
                                rgba,
                            },
                        );
                        let fraction = if total == 0 {
                            1.0
                        } else {
                            completed as f32 / total as f32
                        };
                        self.preview_preload_task.progress = Some(TaskProgress {
                            label: format!("Preloading previews: {completed}/{total}"),
                            fraction: fraction.clamp(0.0, 1.0),
                        });

                        if self.preview.is_none()
                            && self.current_image_path().is_some_and(|p| *p == path)
                            && let Some(cached) = self.preview_cache.get(&path).cloned()
                        {
                            self.apply_cached_preview(path, cached, ctx, "Preview loaded.");
                        } else {
                            self.ensure_settings(&path, source_size[0], source_size[1]);
                        }
                        ctx.request_repaint();
                    }
                    PreviewPreloadMessage::Failed {
                        path,
                        error,
                        completed,
                        total,
                    } => {
                        let fraction = if total == 0 {
                            1.0
                        } else {
                            completed as f32 / total as f32
                        };
                        self.preview_preload_task.progress = Some(TaskProgress {
                            label: format!(
                                "Preloading previews: {completed}/{total} (last failed: {})",
                                path.file_name().and_then(|s| s.to_str()).unwrap_or("image")
                            ),
                            fraction: fraction.clamp(0.0, 1.0),
                        });
                        if self.current_image_path().is_some_and(|p| *p == path) {
                            self.status_message =
                                format!("Failed loading preview for {}: {error}", path.display());
                        }
                        ctx.request_repaint();
                    }
                    PreviewPreloadMessage::Finished => {
                        self.preview_preload_task.progress = None;
                        keep_receiver = false;
                        ctx.request_repaint();
                    }
                }
            }

            if keep_receiver {
                self.preview_preload_task.receiver = Some(rx);
            }
        }

        if let Some(rx) = self.export_task.receiver.take() {
            let mut keep_receiver = true;
            while let Ok(msg) = rx.try_recv() {
                match msg {
                    ExportWorkerMessage::Progress {
                        exported_frames,
                        total_frames,
                        processed_files,
                        total_files,
                        skipped_files,
                    } => {
                        let fraction = if total_frames > 0 {
                            exported_frames as f32 / total_frames as f32
                        } else if total_files > 0 {
                            processed_files as f32 / total_files as f32
                        } else {
                            1.0
                        };
                        let frame_label = if total_frames == 0 {
                            "Exporting frame (0/0)".to_owned()
                        } else {
                            format!(
                                "Exporting frame ({}/{})",
                                exported_frames.min(total_frames),
                                total_frames
                            )
                        };
                        self.export_task.progress = Some(TaskProgress {
                            label: format!(
                                "{frame_label} - files {processed_files}/{total_files}, skipped files {skipped_files}",
                            ),
                            fraction: fraction.clamp(0.0, 1.0),
                        });
                        self.status_message = "Export in progress...".to_owned();
                        ctx.request_repaint();
                    }
                    ExportWorkerMessage::Finished {
                        exported_frames,
                        skipped_files,
                        failures,
                        output_folder,
                    } => {
                        if failures.is_empty() {
                            self.status_message = format!(
                                "Exported {} frames to {}. Skipped {} file(s).",
                                exported_frames,
                                output_folder.display(),
                                skipped_files
                            );
                        } else {
                            let first = failures
                                .first()
                                .cloned()
                                .unwrap_or_else(|| "unknown error".to_owned());
                            self.status_message = format!(
                                "Exported {} frames, skipped {} file(s), with {} file error(s). First: {}",
                                exported_frames,
                                skipped_files,
                                failures.len(),
                                first
                            );
                        }

                        self.export_task.progress = None;
                        keep_receiver = false;
                        ctx.request_repaint();
                    }
                }
            }

            if keep_receiver {
                self.export_task.receiver = Some(rx);
            }
        }
    }

    fn max_preview_offsets(&self, texture: &TextureHandle, viewport_rect: Rect) -> Vec2 {
        let scaled_size = texture_size_vec2(texture) * self.zoom.max(0.1);
        egui::vec2(
            (scaled_size.x - viewport_rect.width()).max(0.0),
            (scaled_size.y - viewport_rect.height()).max(0.0),
        )
    }

    fn clamp_preview_scroll(
        &self,
        scroll: Vec2,
        texture: &TextureHandle,
        viewport_rect: Rect,
    ) -> Vec2 {
        let max_offsets = self.max_preview_offsets(texture, viewport_rect);
        egui::vec2(
            scroll.x.clamp(0.0, max_offsets.x),
            scroll.y.clamp(0.0, max_offsets.y),
        )
    }

    fn apply_scroll_and_zoom_input(
        &mut self,
        ctx: &egui::Context,
        texture: &TextureHandle,
        viewport_rect: Rect,
    ) {
        let (modifiers, scroll_delta, pointer_pos) =
            ctx.input(|i| (i.modifiers, i.raw_scroll_delta, i.pointer.hover_pos()));

        if scroll_delta.length_sq() <= f32::EPSILON {
            return;
        }

        let Some(pointer_pos) = pointer_pos else {
            return;
        };

        if !viewport_rect.contains(pointer_pos) {
            return;
        }

        if modifiers.ctrl {
            let wheel_delta = if scroll_delta.y.abs() > f32::EPSILON {
                scroll_delta.y
            } else {
                scroll_delta.x
            };
            if wheel_delta.abs() < f32::EPSILON {
                return;
            }

            let old_zoom = self.zoom;
            let zoom_factor = (wheel_delta / 220.0).exp();
            let new_zoom = (old_zoom * zoom_factor).clamp(0.1, 8.0);
            if (new_zoom - old_zoom).abs() < f32::EPSILON {
                return;
            }

            let pointer_in_view = pointer_pos - viewport_rect.min;
            let before_x = self.preview_scroll.x + pointer_in_view.x;
            let before_y = self.preview_scroll.y + pointer_in_view.y;
            let scale = new_zoom / old_zoom.max(0.0001);
            let new_offset = egui::vec2(
                before_x * scale - pointer_in_view.x,
                before_y * scale - pointer_in_view.y,
            );

            self.zoom = new_zoom;
            self.preview_scroll = self.clamp_preview_scroll(new_offset, texture, viewport_rect);
            ctx.request_repaint();
            return;
        }

        let mut next_scroll = self.preview_scroll;
        if modifiers.shift {
            let horizontal_delta = if scroll_delta.y.abs() > f32::EPSILON {
                scroll_delta.y
            } else {
                scroll_delta.x
            };
            next_scroll.x -= horizontal_delta;
        } else {
            let vertical_delta = if scroll_delta.y.abs() > f32::EPSILON {
                scroll_delta.y
            } else {
                -scroll_delta.x
            };
            next_scroll.y -= vertical_delta;
        }

        self.preview_scroll = self.clamp_preview_scroll(next_scroll, texture, viewport_rect);
        ctx.request_repaint();
    }

    fn apply_middle_pan(
        &mut self,
        ctx: &egui::Context,
        texture: &TextureHandle,
        viewport_rect: Rect,
    ) {
        let (middle_down, pointer_pos, delta) = ctx.input(|i| {
            (
                i.pointer.button_down(egui::PointerButton::Middle),
                i.pointer.hover_pos(),
                i.pointer.delta(),
            )
        });

        if !middle_down || delta.length_sq() <= f32::EPSILON {
            return;
        }

        let Some(pointer_pos) = pointer_pos else {
            return;
        };
        if !viewport_rect.contains(pointer_pos) {
            return;
        }

        let next = self.preview_scroll - delta;
        self.preview_scroll = self.clamp_preview_scroll(next, texture, viewport_rect);
        ctx.request_repaint();
    }

    fn paint_peeker(&self, ui: &mut egui::Ui, texture: &TextureHandle, image_rect: Rect) {
        let (primary_down, pointer_pos) = ui
            .ctx()
            .input(|i| (i.pointer.primary_down(), i.pointer.hover_pos()));

        if !primary_down {
            return;
        }

        let Some(pointer_pos) = pointer_pos else {
            return;
        };

        if !image_rect.contains(pointer_pos) {
            return;
        }

        let nx = ((pointer_pos.x - image_rect.left()) / image_rect.width()).clamp(0.0, 1.0);
        let ny = ((pointer_pos.y - image_rect.top()) / image_rect.height()).clamp(0.0, 1.0);

        let [tw, th] = texture.size();
        let tw = tw as f32;
        let th = th as f32;
        if tw <= 1.0 || th <= 1.0 {
            return;
        }

        let sample_radius_px = 9.0;
        let half_u = (sample_radius_px / tw).clamp(0.001, 0.5);
        let half_v = (sample_radius_px / th).clamp(0.001, 0.5);

        let uv_min = Pos2::new((nx - half_u).clamp(0.0, 1.0), (ny - half_v).clamp(0.0, 1.0));
        let uv_max = Pos2::new((nx + half_u).clamp(0.0, 1.0), (ny + half_v).clamp(0.0, 1.0));
        let uv_rect = Rect::from_min_max(uv_min, uv_max);

        let lens_size = Vec2::new(170.0, 170.0);
        let lens_bounds = ui.clip_rect();
        let margin = 18.0;

        // Place lens within the visible preview viewport, not the full image extent.
        let place_left = pointer_pos.x + margin + lens_size.x > lens_bounds.right();
        let place_above = pointer_pos.y + margin + lens_size.y > lens_bounds.bottom();

        let mut lens_min = Pos2::new(
            if place_left {
                pointer_pos.x - lens_size.x - margin
            } else {
                pointer_pos.x + margin
            },
            if place_above {
                pointer_pos.y - lens_size.y - margin
            } else {
                pointer_pos.y + margin
            },
        );

        let min_x = lens_bounds.left();
        let max_x = (lens_bounds.right() - lens_size.x).max(min_x);
        let min_y = lens_bounds.top();
        let max_y = (lens_bounds.bottom() - lens_size.y).max(min_y);
        lens_min.x = lens_min.x.clamp(min_x, max_x);
        lens_min.y = lens_min.y.clamp(min_y, max_y);

        let lens_rect = Rect::from_min_size(lens_min, lens_size);
        let inner = lens_rect.shrink(3.0);
        let painter = ui.painter();
        painter.rect_filled(lens_rect, 4.0, Color32::from_black_alpha(220));
        painter.rect_stroke(
            lens_rect,
            4.0,
            Stroke::new(1.0, Color32::LIGHT_GRAY),
            StrokeKind::Outside,
        );
        painter.image(texture.id(), inner, uv_rect, Color32::WHITE);

        let center = inner.center();
        painter.line_segment(
            [
                Pos2::new(inner.left(), center.y),
                Pos2::new(inner.right(), center.y),
            ],
            Stroke::new(1.0, Color32::YELLOW),
        );
        painter.line_segment(
            [
                Pos2::new(center.x, inner.top()),
                Pos2::new(center.x, inner.bottom()),
            ],
            Stroke::new(1.0, Color32::YELLOW),
        );
    }
}

impl eframe::App for ScanDividerApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.poll_background_tasks(ctx);

        egui::TopBottomPanel::top("top_bar").show(ctx, |ui| {
            self.draw_top_bar(ui, ctx);
        });

        egui::TopBottomPanel::bottom("status_bar").show(ctx, |ui| {
            ui.label(&self.status_message);

            if let Some(progress) = &self.preview_task.progress {
                ui.add(
                    egui::ProgressBar::new(progress.fraction.clamp(0.0, 1.0))
                        .show_percentage()
                        .text(format!("Preview: {}", progress.label)),
                );
            }

            if let Some(progress) = &self.export_task.progress {
                ui.add(
                    egui::ProgressBar::new(progress.fraction.clamp(0.0, 1.0))
                        .show_percentage()
                        .text(format!("Export: {}", progress.label)),
                );
            }
        });

        match self.screen {
            Screen::Editor => {
                egui::SidePanel::left("files_panel")
                    .resizable(true)
                    .default_width(250.0)
                    .show(ctx, |ui| {
                        self.draw_file_list_panel(ui, ctx);
                    });

                egui::SidePanel::right("settings_panel")
                    .resizable(true)
                    .default_width(360.0)
                    .show(ctx, |ui| {
                        self.draw_settings_panel(ui);
                    });

                egui::CentralPanel::default().show(ctx, |ui| {
                    self.draw_preview_panel(ui);
                });
            }
            Screen::Export => {
                egui::CentralPanel::default().show(ctx, |ui| {
                    egui::ScrollArea::vertical()
                        .auto_shrink([false, false])
                        .show(ui, |ui| {
                            self.draw_export_screen(ui);
                        });
                });
            }
        }

        self.handle_editor_shortcuts(ctx);
        self.draw_controls_help_window(ctx);

        if self.preview_task.receiver.is_some()
            || self.preview_preload_task.receiver.is_some()
            || self.export_task.receiver.is_some()
        {
            ctx.request_repaint_after(Duration::from_millis(33));
        }
    }
}

fn texture_size_vec2(texture: &TextureHandle) -> Vec2 {
    let [w, h] = texture.size();
    Vec2::new(w as f32, h as f32)
}

fn corner_shortcut_label(strip_index: usize, corner_index: usize) -> &'static str {
    match (strip_index, corner_index) {
        (0, 0) => "1",
        (0, 1) => "2",
        (0, 2) => "3",
        (1, 0) => "4",
        (1, 1) => "5",
        (1, 2) => "6",
        _ => "?",
    }
}

fn draw_preset_text_input(
    ui: &mut egui::Ui,
    id_salt: impl std::hash::Hash,
    options: &[String],
    value: &mut String,
) -> egui::Response {
    ui.horizontal(|ui| {
        let dropdown_width = if options.is_empty() { 0.0 } else { 24.0 };
        let available_for_text = (ui.available_width() - dropdown_width).max(80.0);
        let text_width = (available_for_text * 0.65)
            .max(340.0)
            .min(available_for_text);
        let response = ui.add_sized([text_width, 0.0], egui::TextEdit::singleline(value));

        if !options.is_empty() {
            ui.push_id(id_salt, |ui| {
                ui.menu_button("v", |ui| {
                    if ui.button("Clear").clicked() {
                        value.clear();
                        ui.close_menu();
                    }
                    ui.separator();
                    for option in options {
                        if ui.button(option).clicked() {
                            *value = option.clone();
                            ui.close_menu();
                        }
                    }
                });
            });
        }

        response
    })
    .inner
}

fn draw_corner_editor(
    ui: &mut egui::Ui,
    label: &str,
    corner: &mut Option<PointPx>,
    source_size: [usize; 2],
) {
    let max_x = source_size[0].saturating_sub(1) as f32;
    let max_y = source_size[1].saturating_sub(1) as f32;

    ui.horizontal(|ui| {
        ui.label(label);

        if corner.is_none() {
            ui.label("<unset>");
            if ui.small_button("Set (0,0)").clicked() {
                *corner = Some(PointPx { x: 0.0, y: 0.0 });
            }
            return;
        }

        if let Some(point) = corner.as_mut() {
            ui.label("x");
            if ui
                .add(
                    egui::DragValue::new(&mut point.x)
                        .range(0.0..=max_x)
                        .speed(1.0),
                )
                .changed()
            {
                point.x = point.x.clamp(0.0, max_x);
            }

            ui.label("y");
            if ui
                .add(
                    egui::DragValue::new(&mut point.y)
                        .range(0.0..=max_y)
                        .speed(1.0),
                )
                .changed()
            {
                point.y = point.y.clamp(0.0, max_y);
            }

            if ui.small_button("Clear").clicked() {
                *corner = None;
            }
        }
    });
}

fn find_gap_boundary_hit(
    settings: &ImageSettings,
    pointer_pos: Pos2,
    image_rect: Rect,
    source_size: [usize; 2],
) -> Option<GapDragState> {
    let mut best: Option<(GapDragState, f32)> = None;

    for strip_index in 0..settings.strip_count {
        let strip = &settings.strips[strip_index];
        let Some(strip_q) = strip_quad(strip) else {
            continue;
        };

        for (gap_index, boundary_u) in gap_boundaries_along(strip).into_iter().enumerate() {
            let (line_start, line_end) = gap_boundary_line_points(strip_q, strip, boundary_u);
            let (Some(s0), Some(s1)) = (
                source_point_to_screen(line_start, image_rect, source_size),
                source_point_to_screen(line_end, image_rect, source_size),
            ) else {
                continue;
            };

            let distance = point_to_segment_distance(pointer_pos, s0, s1);
            if distance > GAP_DRAG_HIT_RADIUS_PX {
                continue;
            }

            let candidate = GapDragState {
                strip_index,
                gap_index,
            };

            match best {
                None => best = Some((candidate, distance)),
                Some((_, best_distance)) if distance < best_distance => {
                    best = Some((candidate, distance));
                }
                _ => {}
            }
        }
    }

    best.map(|(hit, _)| hit)
}

fn find_corner_hit(
    settings: &ImageSettings,
    pointer_pos: Pos2,
    image_rect: Rect,
    source_size: [usize; 2],
) -> Option<CornerDragState> {
    let mut best: Option<(CornerDragState, f32)> = None;

    for strip_index in 0..settings.strip_count {
        let strip = &settings.strips[strip_index];
        let corners = [strip.corner_origin, strip.corner_along, strip.corner_across];

        for (corner_index, corner) in corners.into_iter().enumerate() {
            let Some(point) = corner else {
                continue;
            };
            let Some(screen_p) = source_point_to_screen(point, image_rect, source_size) else {
                continue;
            };

            let distance = pointer_pos.distance(screen_p);
            if distance > CORNER_DRAG_HIT_RADIUS_PX {
                continue;
            }

            let candidate = CornerDragState {
                strip_index,
                corner_index,
            };
            match best {
                None => best = Some((candidate, distance)),
                Some((_, best_distance)) if distance < best_distance => {
                    best = Some((candidate, distance));
                }
                _ => {}
            }
        }
    }

    best.map(|(hit, _)| hit)
}

fn set_strip_corner(strip: &mut StripSettings, corner_index: usize, point: PointPx) {
    match corner_index {
        0 => strip.corner_origin = Some(point),
        1 => strip.corner_along = Some(point),
        _ => strip.corner_across = Some(point),
    }
}

fn point_to_segment_distance(point: Pos2, a: Pos2, b: Pos2) -> f32 {
    let ab = b - a;
    let ap = point - a;
    let ab_len_sq = ab.length_sq();

    if ab_len_sq <= f32::EPSILON {
        return point.distance(a);
    }

    let t = (ap.dot(ab) / ab_len_sq).clamp(0.0, 1.0);
    let closest = a + ab * t;
    point.distance(closest)
}

fn clamp_screen_pos_to_rect(pos: Pos2, rect: Rect) -> Pos2 {
    Pos2::new(
        pos.x.clamp(rect.left(), rect.right()),
        pos.y.clamp(rect.top(), rect.bottom()),
    )
}

fn source_split_t(
    strip_q: crate::geometry::FrameQuad,
    strip: &StripSettings,
    source_point: PointPx,
) -> f32 {
    let (split_axis, _) = split_and_orth_axes(strip_q, strip);

    let rel = source_point.sub(strip_q.p0);
    let denom = split_axis.x * split_axis.x + split_axis.y * split_axis.y;
    if denom <= f32::EPSILON {
        return 0.0;
    }
    (rel.x * split_axis.x + rel.y * split_axis.y) / denom
}

fn gap_boundary_line_points(
    strip_q: crate::geometry::FrameQuad,
    strip: &StripSettings,
    boundary_t: f32,
) -> (PointPx, PointPx) {
    let (split_axis, orth_axis) = split_and_orth_axes(strip_q, strip);
    let boundary_origin = strip_q.p0.add(split_axis.mul(boundary_t));
    (boundary_origin, boundary_origin.add(orth_axis))
}

fn offset_for_dragged_boundary(
    strip: &StripSettings,
    gap_index: usize,
    boundary_u: f32,
) -> Option<f32> {
    let frame_count = strip.frame_count as usize;
    if frame_count <= 1 || gap_index >= frame_count.saturating_sub(1) {
        return None;
    }

    let gap_base = strip.gap_percent.max(0.0) / 100.0;
    let denom = frame_count as f32 + (frame_count.saturating_sub(1) as f32) * gap_base;
    if denom <= 0.0 {
        return None;
    }

    let frame_u = 1.0 / denom;
    let gap_u = gap_base * frame_u;
    let min_frame_u = 0.001;
    let base_start = (gap_index as f32 + 1.0) * frame_u + (gap_index as f32) * gap_u;

    let current_boundaries = gap_boundaries_along(strip);
    if gap_index >= current_boundaries.len() {
        return None;
    }

    let lower = if gap_index == 0 {
        min_frame_u
    } else {
        current_boundaries[gap_index - 1] + gap_u + min_frame_u
    };
    let upper = if gap_index + 1 >= current_boundaries.len() {
        1.0 - gap_u - min_frame_u
    } else {
        current_boundaries[gap_index + 1] - gap_u - min_frame_u
    };

    let clamped_u = if lower <= upper {
        boundary_u.clamp(lower, upper)
    } else {
        lower
    }
    .clamp(0.0, 1.0 - gap_u);

    let offset = (clamped_u - base_start) / frame_u;

    Some(offset.clamp(GAP_OFFSET_MIN, GAP_OFFSET_MAX))
}

fn load_preview_payload(path: &Path) -> Result<PreviewPayload, String> {
    let mut reader = image::ImageReader::open(path)
        .and_then(|r| r.with_guessed_format())
        .map_err(|err| format!("Failed opening/identifying image: {err}"))?;
    reader.no_limits();

    let decoded = reader
        .decode()
        .map_err(|err| format!("Failed decoding image: {err}"))?;
    let (source_w, source_h) = decoded.dimensions();
    let preview_image = downscale_for_preview(decoded);
    let (preview_w, preview_h) = preview_image.dimensions();
    let rgba = preview_image.into_rgba8().into_raw();

    Ok((
        [source_w as usize, source_h as usize],
        [preview_w as usize, preview_h as usize],
        rgba,
    ))
}

fn invert_rgba_bytes_in_place(rgba: &mut [u8]) {
    for px in rgba.chunks_exact_mut(4) {
        px[0] = u8::MAX - px[0];
        px[1] = u8::MAX - px[1];
        px[2] = u8::MAX - px[2];
    }
}

fn parse_exif_scan_datetime(value: &str) -> Option<(i32, u32, u32, u32, u32, u32)> {
    let mut parts = value.split_whitespace();
    let date = parts.next()?;
    let time = parts.next()?;
    if parts.next().is_some() {
        return None;
    }

    let mut date_parts = date.split(':');
    let year: i32 = date_parts.next()?.parse().ok()?;
    let month: u32 = date_parts.next()?.parse().ok()?;
    let day: u32 = date_parts.next()?.parse().ok()?;
    if date_parts.next().is_some() {
        return None;
    }

    let mut time_parts = time.split(':');
    let hour: u32 = time_parts.next()?.parse().ok()?;
    let minute: u32 = time_parts.next()?.parse().ok()?;
    let second: u32 = time_parts.next()?.parse().ok()?;
    if time_parts.next().is_some() {
        return None;
    }

    if !(1..=12).contains(&month) || hour > 23 || minute > 59 || second > 59 {
        return None;
    }

    let max_day = days_in_month(year, month);
    if day == 0 || day > max_day {
        return None;
    }

    Some((year, month, day, hour, minute, second))
}

fn format_exif_scan_datetime(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
) -> String {
    format!("{year:04}:{month:02}:{day:02} {hour:02}:{minute:02}:{second:02}")
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 31,
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

fn open_folder_in_file_manager(path: &Path) -> Result<(), String> {
    #[cfg(target_os = "linux")]
    let mut cmd = {
        let mut c = std::process::Command::new("xdg-open");
        c.arg(path);
        c
    };

    #[cfg(target_os = "macos")]
    let mut cmd = {
        let mut c = std::process::Command::new("open");
        c.arg(path);
        c
    };

    #[cfg(target_os = "windows")]
    let mut cmd = {
        let mut c = std::process::Command::new("explorer");
        c.arg(path);
        c
    };

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        let _ = path;
        return Err("Open folder is not supported on this platform.".to_owned());
    }

    cmd.spawn()
        .map_err(|err| format!("Failed to open folder {}: {err}", path.display()))?;
    Ok(())
}
